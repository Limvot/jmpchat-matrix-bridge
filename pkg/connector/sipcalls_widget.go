//go:build sipcalls

package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
	"github.com/williamkray/mrtc-sip-bridge/pkg/widget"
)

// widgetInstaller is the part of *widget.Installer this host uses (a fake in tests).
type widgetInstaller interface {
	hostkit.WidgetInstaller
	RequestSweep(from context.Context, trigger string)
}

// widgetSeams are test seams for the bridge lookups (nil = the real bridge call).
type widgetSeams struct {
	portalByMXID func(ctx context.Context, roomID id.RoomID) (*bridgev2.Portal, error)
	defaultLogin func(user *bridgev2.User) *bridgev2.UserLogin
	cachedLogin  func(id networkid.UserLoginID) *bridgev2.UserLogin
	allLogins    func() []*bridgev2.UserLogin
	private      func(ctx context.Context, room id.RoomID, owner id.UserID) (bool, error)
	openDevices  func(ctx context.Context) (*widget.DeviceStore, error)
}

// wRequirePrivate refuses a room with members other than the bot, this bridge's ghosts and owner
// (WIDGET-SPEC §5.2): the widget URL carries the room key.
func (x *callsExt) wRequirePrivate(ctx context.Context, room id.RoomID, owner id.UserID) error {
	var ok bool
	var err error
	if f := x.wseam.private; f != nil {
		ok, err = f(ctx, room, owner)
	} else {
		ok, err = widget.PrivateRoom(ctx, x.br, room, owner)
	}
	if err != nil {
		return fmt.Errorf("check room privacy: %w", err)
	}
	if !ok {
		return widget.Refuse(widget.MsgNotPrivateRoom)
	}
	return nil
}

// widgetRoomOwners is widget.Deps.RoomOwners: the receiver's user for a portal, the user for a
// management room.
func (x *callsExt) widgetRoomOwners(ctx context.Context, room id.RoomID) []id.UserID {
	if portal, err := x.wPortalByMXID(ctx, room); err == nil && portal != nil && portal.Portal != nil && portal.Receiver != "" {
		if ul := x.wCachedLogin(portal.Receiver); ul != nil {
			return []id.UserID{ul.UserMXID}
		}
		return nil
	}
	for _, ul := range x.wAllLogins() {
		if ul != nil && ul.User != nil && ul.User.User != nil && ul.User.ManagementRoom == room {
			return []id.UserID{ul.UserMXID}
		}
	}
	return nil
}

func (x *callsExt) wPortalByMXID(ctx context.Context, roomID id.RoomID) (*bridgev2.Portal, error) {
	if f := x.wseam.portalByMXID; f != nil {
		return f(ctx, roomID)
	}
	return x.br.GetPortalByMXID(ctx, roomID)
}

func (x *callsExt) wDefaultLogin(user *bridgev2.User) *bridgev2.UserLogin {
	if f := x.wseam.defaultLogin; f != nil {
		return f(user)
	}
	return user.GetDefaultLogin()
}

func (x *callsExt) wCachedLogin(loginID networkid.UserLoginID) *bridgev2.UserLogin {
	if f := x.wseam.cachedLogin; f != nil {
		return f(loginID)
	}
	return x.br.GetCachedUserLoginByID(loginID)
}

func (x *callsExt) wAllLogins() []*bridgev2.UserLogin {
	if f := x.wseam.allLogins; f != nil {
		return f()
	}
	return x.br.GetAllCachedUserLogins()
}

func (x *callsExt) widgetInstallerNow() widgetInstaller {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.widgetInst
}

// widgetEnqueue asks the installer to add the widget to a room (never blocks; no-op without a widget).
func (x *callsExt) widgetEnqueue(roomID id.RoomID, title string) {
	if in := x.widgetInstallerNow(); in != nil {
		in.Enqueue(roomID, title)
	}
}

// widgetSweep requests a login-triggered sweep of all eligible rooms (never blocks; no-op without a widget).
func (x *callsExt) widgetSweep(ctx context.Context) {
	if in := x.widgetInstallerNow(); in != nil {
		in.RequestSweep(ctx, widget.TriggerLogin)
	}
}

// lazyWidgetInstaller is the hostkit.CommandDeps.Widget of a host whose widget is enabled in the config:
// the commands are registered before start creates the server.
type lazyWidgetInstaller struct{ x *callsExt }

var errWidgetNotRunning = errors.New("the widget is not running")

func (l lazyWidgetInstaller) Ensure(ctx context.Context, roomID id.RoomID, title string) error {
	in := l.x.widgetInstallerNow()
	if in == nil {
		return errWidgetNotRunning
	}
	return in.Ensure(ctx, roomID, title)
}

func (l lazyWidgetInstaller) Enqueue(roomID id.RoomID, title string) {
	l.x.widgetEnqueue(roomID, title)
}

// widgetCommandInstaller returns the installer dep of the call commands: a nil interface (widget
// disabled) unless network.calls.widget.enabled.
func (x *callsExt) widgetCommandInstaller() hostkit.WidgetInstaller {
	if !x.cfg.Widget.Enabled {
		return nil
	}
	return lazyWidgetInstaller{x}
}

// lazyDialpad is the hostkit.CommandDeps.Dialpad of a host whose PWA is enabled in the config: the
// commands are registered before start creates the server (PWA-SPEC §11.1).
type lazyDialpad struct{ x *callsExt }

var errDialpadNotRunning = errors.New("the Dialpad app is not running")

func (l lazyDialpad) links() (hostkit.DialpadLinks, error) {
	l.x.mu.Lock()
	srv := l.x.widgetSrv
	l.x.mu.Unlock()
	if srv != nil {
		if d := srv.DialpadLinks(); d != nil {
			return d, nil
		}
	}
	return nil, errDialpadNotRunning
}

func (l lazyDialpad) Issue(ctx context.Context, user id.UserID, login networkid.UserLoginID, room id.RoomID, name string) (hostkit.LinkIssue, error) {
	d, err := l.links()
	if err != nil {
		return hostkit.LinkIssue{}, err
	}
	return d.Issue(ctx, user, login, room, name)
}

func (l lazyDialpad) AttachNotice(ctx context.Context, linkID string, room id.RoomID, evt id.EventID) error {
	d, err := l.links()
	if err != nil {
		return err
	}
	return d.AttachNotice(ctx, linkID, room, evt)
}

func (l lazyDialpad) List(ctx context.Context, user id.UserID) ([]hostkit.LinkInfo, error) {
	d, err := l.links()
	if err != nil {
		return nil, err
	}
	return d.List(ctx, user)
}

func (l lazyDialpad) Revoke(ctx context.Context, user id.UserID, linkID string) (int, error) {
	d, err := l.links()
	if err != nil {
		return 0, err
	}
	return d.Revoke(ctx, user, linkID)
}

// dialpadCommandDeps returns the Dialpad dep of the call commands: a nil interface (PWA disabled) unless
// network.calls.widget.enabled and widget.pwa.enabled.
func (x *callsExt) dialpadCommandDeps() hostkit.DialpadLinks {
	if !x.cfg.Widget.Enabled || !x.cfg.Widget.PWA.Enabled {
		return nil
	}
	return lazyDialpad{x}
}

// widgetResolveDevice is widget.Deps.ResolveDevice (PWA-SPEC §11.2): the device's login must be a jabber
// login of the user; ready means a SIP account is attached (checkLoginWithAccount).
func (x *callsExt) widgetResolveDevice(ctx context.Context, user *bridgev2.User, loginID networkid.UserLoginID) (widget.RoomContext, error) {
	if user == nil {
		return widget.RoomContext{}, widget.ErrDeviceLoginGone
	}
	ul := x.wCachedLogin(loginID)
	if ul == nil || ul.UserMXID != user.MXID {
		return widget.RoomContext{}, widget.ErrDeviceLoginGone
	}
	rc := widget.RoomContext{Kind: widget.KindDevice}
	rc.Login, rc.Refusal = x.checkLoginWithAccount(ul)
	return rc, nil
}

// widgetDeps are the host adapters of the widget (WIDGET-SPEC §3.1).
func (x *callsExt) widgetDeps(requireAccount bool) widget.Deps {
	return widget.Deps{
		ResolveDevice:      x.widgetResolveDevice,
		CommandPrefix:      x.commandPrefix(),
		Bridge:             x.br,
		Calls:              x.callAPI,
		ResolveRoom:        x.widgetResolveRoom,
		RoomOwners:         x.widgetRoomOwners,
		EligibleRooms:      func(ctx context.Context) ([]widget.RoomRef, error) { return x.widgetEligibleRooms(ctx, requireAccount) },
		DefaultCountryCode: x.cfg.DefaultCountryCode,
		Log:                x.bridgeLog(),
		Features:           widgetFeatures{x},
	}
}

// startWidget creates, mounts and starts the widget server when network.calls.widget.enabled (config
// already validated by validateCallsConfig). With the widget disabled it may run the removal sweep.
func (x *callsExt) startWidget() error {
	if !x.cfg.Widget.Enabled {
		x.startWidgetRemoval()
		return nil
	}
	deps := x.widgetDeps(true)
	if x.cfg.Widget.PWA.Enabled {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		st, err := x.openDeviceStore(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("network.calls.widget.pwa: %w", err)
		}
		deps.Devices = st
	}
	srv, err := widget.New(x.cfg.Widget, deps)
	if err != nil {
		return fmt.Errorf("network.calls.widget: %w", err)
	}
	if err = widget.Mount(x.br, srv); err != nil {
		return fmt.Errorf("network.calls.widget: %w", err)
	}
	x.mu.Lock()
	x.widgetSrv, x.widgetInst = srv, srv.Installer()
	x.mu.Unlock()
	srv.Start()
	x.log.Info().Str("prefix", srv.Prefix()).Msg("Dialpad widget enabled")
	return nil
}

// openDeviceStore opens the PWA device store (a var-free seam: tests set wseam.openDevices).
func (x *callsExt) openDeviceStore(ctx context.Context) (*widget.DeviceStore, error) {
	if f := x.wseam.openDevices; f != nil {
		return f(ctx)
	}
	return widget.OpenDeviceStore(ctx, x.br, x.cfg.Widget, x.bridgeLog())
}

// startWidgetRemoval runs the remove_when_disabled sweep once in the background.
func (x *callsExt) startWidgetRemoval() {
	if x.cfg.Widget.Enabled || !x.cfg.Widget.RemoveWhenDisabled {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	x.mu.Lock()
	x.widgetStop = cancel
	x.mu.Unlock()
	d := x.widgetDeps(false)
	x.widgetWG.Add(1)
	go func() {
		defer x.widgetWG.Done()
		defer cancel()
		if err := widget.Remove(ctx, d); err != nil {
			x.log.Warn().Err(err).Msg("Widget removal sweep failed")
		}
	}()
}

// stopWidget stops the widget server and any removal sweep.
func (x *callsExt) stopWidget() {
	x.mu.Lock()
	srv, cancel := x.widgetSrv, x.widgetStop
	x.widgetSrv, x.widgetInst, x.widgetStop = nil, nil, nil
	x.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if srv != nil {
		srv.Stop()
	}
	x.widgetWG.Wait()
}

// widgetResolveRoom is widget.Deps.ResolveRoom (WIDGET-SPEC §5.4): a Cheogram 1:1 portal of one of the
// user's logins, or the user's management room. A valid room whose login is not ready returns a context
// with Login nil and the same refusal text as the commands.
func (x *callsExt) widgetResolveRoom(ctx context.Context, user *bridgev2.User, roomID id.RoomID) (widget.RoomContext, error) {
	if user == nil || roomID == "" {
		return widget.RoomContext{}, widget.Refuse(widget.MsgNotBridgeRoom)
	}
	if user.ManagementRoom == roomID {
		if err := x.wRequirePrivate(ctx, roomID, user.MXID); err != nil {
			return widget.RoomContext{}, err
		}
		rc := widget.RoomContext{Kind: widget.KindManagement}
		rc.Login, rc.Refusal = x.checkLoginWithAccount(x.wDefaultLogin(user))
		return rc, nil
	}
	portal, err := x.wPortalByMXID(ctx, roomID)
	if err != nil {
		return widget.RoomContext{}, fmt.Errorf("get portal: %w", err)
	}
	if portal == nil || portal.Portal == nil || portal.MXID != roomID {
		return widget.RoomContext{}, widget.Refuse(widget.MsgNotBridgeRoom)
	}
	owner := x.wCachedLogin(portal.Receiver)
	if owner == nil || owner.UserMXID != user.MXID {
		return widget.RoomContext{}, widget.Refuse(widget.MsgNotBridgeRoom)
	}
	e164, ok := isCallPortal(portal, owner.ID)
	if !ok {
		return widget.RoomContext{}, widget.Refuse(widget.MsgNotBridgeRoom)
	}
	if err = x.wRequirePrivate(ctx, roomID, user.MXID); err != nil {
		return widget.RoomContext{}, err
	}
	rc := widget.RoomContext{Kind: widget.KindPortal, Number: e164}
	rc.Login, rc.Refusal = x.checkLoginWithAccount(owner)
	return rc, nil
}

// widgetEligibleRooms lists the rooms that should carry the widget: Cheogram 1:1 portals of jabber logins
// and the management room of their users. requireAccount limits it to logins with an attached SIP account
// (install sweeps); false lists every jabber login (removal sweep).
func (x *callsExt) widgetEligibleRooms(ctx context.Context, requireAccount bool) ([]widget.RoomRef, error) {
	ok := func(ul *bridgev2.UserLogin) bool {
		if ul == nil {
			return false
		}
		if _, isX := ul.Client.(*xmppClient); !isX {
			return false
		}
		return !requireAccount || x.hasAccount(ul.ID)
	}
	var refs []widget.RoomRef
	list := x.listPortals
	if list == nil {
		list = x.br.GetAllPortalsWithMXID
	}
	portals, err := list(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range portals {
		if p == nil || p.Portal == nil || p.MXID == "" {
			continue
		}
		e164, isCall := isCallPortal(p, p.Receiver)
		if !isCall || !ok(x.wCachedLogin(p.Receiver)) {
			continue
		}
		refs = append(refs, widget.RoomRef{RoomID: p.MXID, Title: "Call " + e164})
	}
	seen := make(map[id.RoomID]bool)
	for _, ul := range x.wAllLogins() {
		if !ok(ul) || ul.User == nil || ul.User.User == nil {
			continue
		}
		room := ul.User.ManagementRoom
		if room == "" || seen[room] {
			continue
		}
		seen[room] = true
		refs = append(refs, widget.RoomRef{RoomID: room})
	}
	return refs, nil
}
