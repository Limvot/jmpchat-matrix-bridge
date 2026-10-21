package connector

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"mellium.im/xmpp/stanza"
)

func TestPermanentVCardError(t *testing.T) {
	mk := func(typ stanza.ErrorType, c stanza.Condition) error {
		return fmt.Errorf("vcard fetch: %w", &iqStanzaError{from: "x", err: stanza.Error{Type: typ, Condition: c}})
	}
	for _, c := range []stanza.Condition{stanza.FeatureNotImplemented, stanza.ServiceUnavailable, stanza.ItemNotFound,
		stanza.NotAllowed, stanza.Forbidden, stanza.RecipientUnavailable} {
		if !permanentVCardError(mk(stanza.Cancel, c)) {
			t.Errorf("cancel/%s should be permanent", c)
		}
	}
	for _, err := range []error{nil, context.DeadlineExceeded, errors.New("boom"),
		mk(stanza.Wait, stanza.ServiceUnavailable), mk(stanza.Cancel, stanza.InternalServerError),
		mk(stanza.Cancel, stanza.RemoteServerTimeout)} {
		if permanentVCardError(err) {
			t.Errorf("%v should be transient", err)
		}
	}
}

func TestQueueAvatarRefreshSkipsCheogram(t *testing.T) {
	x := &xmppClient{log: zerolog.Nop()}
	fetched := make(chan string, 4)
	x.avatarFetchHook = func(_ context.Context, j string) ([]byte, string, string, error) {
		fetched <- j
		return nil, "", "", errors.New("transient")
	}
	for _, j := range []string{"+15551234567@cheogram.com", "123456;phone-context=ca-us.phone-context.soprani.ca@cheogram.com", "+15551234567,+15557654321@cheogram.com"} {
		x.queueAvatarRefresh(j)
	}
	x.queueAvatarRefresh("bob@example.com")
	select {
	case j := <-fetched:
		if j != "bob@example.com" {
			t.Fatalf("fetched %q", j)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("regular contact not fetched")
	}
	time.Sleep(50 * time.Millisecond)
	if len(fetched) != 0 {
		t.Fatalf("cheogram JIDs were fetched: %d extra", len(fetched))
	}
}

func TestDoAvatarRefreshStampsOnPermanentErrorOnly(t *testing.T) {
	x := &xmppClient{log: zerolog.Nop()}
	var stamped []string
	x.avatarStampHook = func(j string) { stamped = append(stamped, j) }
	perm := &iqStanzaError{err: stanza.Error{Type: stanza.Cancel, Condition: stanza.FeatureNotImplemented}}
	x.avatarFetchHook = func(context.Context, string) ([]byte, string, string, error) {
		return nil, "", "", fmt.Errorf("vcard fetch: %w", perm)
	}
	x.doAvatarRefresh("bob@example.com")
	if len(stamped) != 1 || stamped[0] != "bob@example.com" {
		t.Fatalf("stamped = %v", stamped)
	}
	x.avatarFetchHook = func(context.Context, string) ([]byte, string, string, error) {
		return nil, "", "", context.DeadlineExceeded
	}
	x.doAvatarRefresh("carol@example.com")
	if len(stamped) != 1 {
		t.Fatalf("transient error stamped: %v", stamped)
	}
}
