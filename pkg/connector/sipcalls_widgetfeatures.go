//go:build sipcalls

package connector

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"

	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
	"github.com/williamkray/mrtc-sip-bridge/pkg/obs"
	"github.com/williamkray/mrtc-sip-bridge/pkg/phone"
)

// Widget wordings of the roster-only warnings (the command's own replies keep their text).
const (
	widgetRosterNotLoaded = "Your Jabber roster has not loaded yet: calls ring for everyone until it has."
	widgetRosterEmpty     = "Your roster has no phone contacts, so every incoming call goes to voicemail."
	msgXMPPOffline        = "Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again."
)

// widgetFeatures implements the optional host features of the Dialpad widget (hostkit/contacts.go):
// the roster-only toggle, Message (open the chat), Save contact and the saved-name lookup. The widget
// has already verified that ul belongs to the session's user and is ready (a SIP account attached).
type widgetFeatures struct{ x *callsExt }

var (
	_ hostkit.RosterOnlyPrefs = widgetFeatures{}
	_ hostkit.ChatOpener      = widgetFeatures{}
	_ hostkit.ContactSaver    = widgetFeatures{}
	_ hostkit.ContactLookup   = widgetFeatures{}
)

// client returns ul's XMPP client, or the checkLogin refusal as an unavailable error.
func (f widgetFeatures) client(ul *bridgev2.UserLogin) (*xmppClient, error) {
	if ul != nil {
		if xc, ok := ul.Client.(*xmppClient); ok && xc != nil {
			return xc, nil
		}
	}
	return nil, hostkit.NewActionError(hostkit.CodeUnavailable, "Log in to Jabber first.")
}

// RosterOnly reads the cached preference and the roster counters (memory only).
func (f widgetFeatures) RosterOnly(ul *bridgev2.UserLogin) hostkit.RosterOnlyState {
	st := hostkit.RosterOnlyState{On: f.x.loginPrefs(ul).RosterOnly}
	if !st.On {
		return st
	}
	xc, err := f.client(ul)
	if err != nil {
		return st
	}
	n, loaded := xc.countRoster(func(j string) bool { _, ok := cheogramE164(j); return ok })
	switch {
	case !loaded:
		st.Warning = widgetRosterNotLoaded
	case n == 0:
		st.Warning = widgetRosterEmpty
	}
	return st
}

// SetRosterOnly persists the preference (a save error is returned as is: the widget reports it as internal).
func (f widgetFeatures) SetRosterOnly(ctx context.Context, ul *bridgev2.UserLogin, on bool) (hostkit.RosterOnlyState, error) {
	if err := f.x.setRosterOnly(ctx, ul, on); err != nil {
		return hostkit.RosterOnlyState{}, err
	}
	return f.RosterOnly(ul), nil
}

// ContactName looks e164 up in the cached roster (memory only).
func (f widgetFeatures) ContactName(ul *bridgev2.UserLogin, e164 string) (string, bool) {
	xc, err := f.client(ul)
	if err != nil {
		return "", false
	}
	jid := cheogramJID(e164)
	name := xc.lookupRosterName(jid)
	saved, _ := xc.inRoster(jid)
	return name, saved || name != ""
}

// SaveContact has the semantics of the save-contact command: the name is set, overwriting an existing
// roster name. It sends nothing when the number is already saved under exactly that name. The name is
// never logged.
func (f widgetFeatures) SaveContact(ctx context.Context, ul *bridgev2.UserLogin, e164, name string) (hostkit.SavedContact, error) {
	if !phone.IsE164(e164) {
		return hostkit.SavedContact{}, hostkit.NewActionError(hostkit.CodeInvalidNumber, "That is not a phone number.")
	}
	name = cleanContactName([]string{name})
	if name == "" || utf8.RuneCountInString(name) > maxContactNameRunes {
		return hostkit.SavedContact{}, hostkit.NewActionError(hostkit.CodeInvalidName, "Use a name of 1-64 characters.")
	}
	xc, err := f.client(ul)
	if err != nil {
		return hostkit.SavedContact{}, err
	}
	if !xc.IsLoggedIn() {
		return hostkit.SavedContact{}, hostkit.NewActionError(hostkit.CodeUnavailable, msgXMPPOffline)
	}
	jid := cheogramJID(e164)
	prev := xc.lookupRosterName(jid)
	if in, _ := xc.inRoster(jid); in && prev == name {
		return hostkit.SavedContact{Name: name, PreviousName: prev}, nil
	}
	set := f.x.setRoster
	if set == nil {
		set = func(ctx context.Context, xc *xmppClient, jid, name string) error {
			return xc.SetRosterItem(ctx, jid, name)
		}
	}
	if err = set(ctx, xc, jid, name); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return hostkit.SavedContact{}, err
		}
		return hostkit.SavedContact{}, fmt.Errorf("save contact: %w", err)
	}
	f.x.logFor(ctx).Info().Str("login", string(ul.ID)).Str("jid", jid).Msg("Contact saved from the Dialpad widget")
	return hostkit.SavedContact{Name: name, PreviousName: prev, Changed: true}, nil
}

type openChatResult struct {
	room *core.CallRoom
	err  error
}

// OpenChat opens (creating if needed) the Cheogram chat of e164 through ResolveCallRoom: the same room
// start-chat makes, plus the Dialpad widget, with no inline MAM backfill. Creation is detached from
// the request so that a deadline cannot cancel CreateMatrixRoom halfway: on a deadline OpenChat returns
// Pending and the room appears shortly. The MAM backfill of a newly created room starts afterwards in
// its own goroutine.
func (f widgetFeatures) OpenChat(ctx context.Context, ul *bridgev2.UserLogin, e164 string) (hostkit.ChatRoom, error) {
	if !phone.IsE164(e164) {
		return hostkit.ChatRoom{}, hostkit.NewActionError(hostkit.CodeInvalidNumber, "That is not a phone number.")
	}
	xc, err := f.client(ul)
	if err != nil {
		return hostkit.ChatRoom{}, err
	}
	open := f.x.openChatRoom
	if open == nil {
		open = f.x.callHost().ResolveCallRoom
	}
	backfill := f.x.backfillChat
	if backfill == nil {
		backfill = func(from context.Context, xc *xmppClient, portalID string) {
			xc.backfillChatAsync(from, portalID, "widget_chat")
		}
	}
	carried := obs.CarrySpan(context.Background(), ctx)
	done := make(chan openChatResult, 1) // buffered: the goroutine never blocks when the request is gone
	go func() {
		cctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cctx, span := obs.StartLinkedRoot(cctx, "bridge.calls.open_chat", carried, trace.WithAttributes(
			attribute.String("xmpp.login.jid", string(ul.ID)), obs.AttrCallRemoteE164.String(e164)))
		room, err := open(cctx, core.LoginRef(ul.ID), e164)
		if err != nil {
			obs.Fail(span, err)
			// The request may have returned Pending already: this line is the only trace of the failure.
			f.x.logFor(cctx).Warn().Str("error", obs.Scrub(err.Error())).Str("login", string(ul.ID)).
				Str("number", e164).Msg("Opening chat from the Dialpad widget failed")
		} else {
			span.SetAttributes(attribute.Bool("created", room.Created))
		}
		done <- openChatResult{room, err}
		if err == nil && room.Created {
			backfill(cctx, xc, cheogramJID(e164)) // starts its own goroutine and linked root
		}
		span.End()
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return hostkit.ChatRoom{}, fmt.Errorf("open chat: %w", r.err)
		}
		return hostkit.ChatRoom{RoomID: r.room.RoomID, Created: r.room.Created}, nil
	case <-ctx.Done():
		f.x.logFor(ctx).Info().Str("login", string(ul.ID)).Str("number", e164).Msg("Opening chat continues in the background")
		return hostkit.ChatRoom{Pending: true}, nil
	}
}
