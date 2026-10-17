//go:build sipcalls

package connector

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/mrtc-sip-bridge/pkg/calls"
	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
	"github.com/williamkray/mrtc-sip-bridge/pkg/widget"
)

// Snapshot completes hostkit.CallAPI for the shared fake (no active call).
func (f *fakeCallAPI) Snapshot(core.LoginRef) (calls.Snapshot, bool) { return calls.Snapshot{}, false }

type fakeWidgetInst struct {
	mu       sync.Mutex
	enqueued [][2]string
	sweeps   []string
}

func (f *fakeWidgetInst) Ensure(context.Context, id.RoomID, string) error { return nil }
func (f *fakeWidgetInst) Enqueue(r id.RoomID, title string) {
	f.mu.Lock()
	f.enqueued = append(f.enqueued, [2]string{string(r), title})
	f.mu.Unlock()
}
func (f *fakeWidgetInst) RequestSweep(_ context.Context, trigger string) {
	f.mu.Lock()
	f.sweeps = append(f.sweeps, trigger)
	f.mu.Unlock()
}

type widgetHarness struct {
	*accountsHarness
	ul   *bridgev2.UserLogin
	user *bridgev2.User
	inst *fakeWidgetInst
	port map[id.RoomID]*bridgev2.Portal
	// shared rooms have an extra member (not private); privErr rooms fail the privacy check.
	shared  map[id.RoomID]bool
	privErr map[id.RoomID]bool
}

func newWidgetHarness(t *testing.T, withAccount bool) *widgetHarness {
	t.Helper()
	ah := newAccountsHarness(t)
	h := &widgetHarness{accountsHarness: ah, inst: &fakeWidgetInst{}, port: map[id.RoomID]*bridgev2.Portal{},
		shared: map[id.RoomID]bool{}, privErr: map[id.RoomID]bool{}}
	h.ul = testLogin(withAccount)
	h.ul.Client = testClient(h.ul)
	h.user = h.ul.User
	lc := &loginCalls{id: h.ul.ID, client: h.ul.Client.(*xmppClient)}
	if withAccount {
		lc.acct = h.x.newLoginAccount(h.ul, testSIPCreds)
	}
	h.x.accounts[h.ul.ID] = lc
	h.x.widgetInst = h.inst
	h.x.wseam = widgetSeams{
		portalByMXID: func(_ context.Context, r id.RoomID) (*bridgev2.Portal, error) { return h.port[r], nil },
		defaultLogin: func(*bridgev2.User) *bridgev2.UserLogin { return h.ul },
		cachedLogin: func(l networkid.UserLoginID) *bridgev2.UserLogin {
			if l == h.ul.ID {
				return h.ul
			}
			return nil
		},
		allLogins: func() []*bridgev2.UserLogin { return []*bridgev2.UserLogin{h.ul} },
		private: func(_ context.Context, r id.RoomID, _ id.UserID) (bool, error) {
			if h.privErr[r] {
				return false, errors.New("no state store")
			}
			return !h.shared[r], nil
		},
	}
	return h
}

func (h *widgetHarness) add(p *bridgev2.Portal) *bridgev2.Portal {
	h.port[p.MXID] = p
	return p
}

func TestWidgetResolveRoom(t *testing.T) {
	h := newWidgetHarness(t, true)
	const dm = "+15551234567@cheogram.com"
	h.add(testPortal(dm, h.ul.ID, database.RoomTypeDM, "!dm:x"))
	h.add(testPortal("+15551234567,+15557654321@cheogram.com", h.ul.ID, database.RoomTypeGroupDM, "!mms:x"))
	h.add(testPortal("room@conference.example.com", h.ul.ID, database.RoomTypeGroupDM, "!muc:x"))
	h.add(testPortal("bob@example.com", h.ul.ID, database.RoomTypeDM, "!bob:x"))
	h.add(testPortal(dm, "other@example.com", database.RoomTypeDM, "!other:x"))
	ctx := context.Background()

	rc, err := h.x.widgetResolveRoom(ctx, h.user, "!dm:x")
	if err != nil || rc.Kind != widget.KindPortal || rc.Number != "+15551234567" || rc.Login != h.ul || rc.Refusal != "" {
		t.Fatalf("portal: %+v, %v", rc, err)
	}
	rc, err = h.x.widgetResolveRoom(ctx, h.user, "!mgmt:example.com")
	if err != nil || rc.Kind != widget.KindManagement || rc.Number != "" || rc.Login != h.ul {
		t.Fatalf("management: %+v, %v", rc, err)
	}
	for _, room := range []id.RoomID{"!mms:x", "!muc:x", "!bob:x", "!other:x", "!unknown:x"} {
		_, err = h.x.widgetResolveRoom(ctx, h.user, room)
		var ref *widget.RefusalError
		if !errors.As(err, &ref) || ref.Message != widget.MsgNotBridgeRoom {
			t.Errorf("%s: err = %v, want not-bridge-room refusal", room, err)
		}
	}
	// Someone else's user.
	stranger := &bridgev2.User{User: &database.User{MXID: "@x:example.com", ManagementRoom: "!sm:x"}}
	if _, err = h.x.widgetResolveRoom(ctx, stranger, "!dm:x"); err == nil {
		t.Error("another user's portal must be refused")
	}
}

func TestWidgetResolveRoomNotPrivate(t *testing.T) {
	h := newWidgetHarness(t, true)
	h.add(testPortal("+15551234567@cheogram.com", h.ul.ID, database.RoomTypeDM, "!dm:x"))
	ctx := context.Background()
	for _, room := range []id.RoomID{"!dm:x", "!mgmt:example.com"} {
		h.shared[room] = true
		_, err := h.x.widgetResolveRoom(ctx, h.user, room)
		var ref *widget.RefusalError
		if !errors.As(err, &ref) || ref.Message != widget.MsgNotPrivateRoom {
			t.Errorf("%s: err = %v, want not-private refusal", room, err)
		}
		delete(h.shared, room)
		h.privErr[room] = true
		if _, err = h.x.widgetResolveRoom(ctx, h.user, room); err == nil || errors.As(err, &ref) {
			t.Errorf("%s: privacy check error must be internal, got %v", room, err)
		}
	}
}

func TestWidgetRoomOwners(t *testing.T) {
	h := newWidgetHarness(t, true)
	h.add(testPortal("+15551234567@cheogram.com", h.ul.ID, database.RoomTypeDM, "!dm:x"))
	ctx := context.Background()
	for _, room := range []id.RoomID{"!dm:x", "!mgmt:example.com"} {
		if got := h.x.widgetRoomOwners(ctx, room); len(got) != 1 || got[0] != h.ul.UserMXID {
			t.Errorf("%s owners = %v, want [%s]", room, got, h.ul.UserMXID)
		}
	}
	if got := h.x.widgetRoomOwners(ctx, "!unknown:x"); got != nil {
		t.Errorf("unknown room owners = %v", got)
	}
	if d := h.x.widgetDeps(true); d.RoomOwners == nil {
		t.Error("widgetDeps must set RoomOwners")
	}
}

func TestWidgetResolveRoomNoAccount(t *testing.T) {
	h := newWidgetHarness(t, false)
	h.add(testPortal("+15551234567@cheogram.com", h.ul.ID, database.RoomTypeDM, "!dm:x"))
	for _, room := range []id.RoomID{"!dm:x", "!mgmt:example.com"} {
		rc, err := h.x.widgetResolveRoom(context.Background(), h.user, room)
		if err != nil || rc.Login != nil || rc.Refusal == "" {
			t.Errorf("%s: %+v, %v; want ready=false refusal without error", room, rc, err)
		}
	}
	rc, _ := h.x.widgetResolveRoom(context.Background(), h.user, "!dm:x")
	if rc.Kind != widget.KindPortal || rc.Number != "+15551234567" {
		t.Errorf("portal context lost: %+v", rc)
	}
	want := "No SIP account is set up for this login. Use `!jc sip-login`."
	if rc.Refusal != want {
		t.Errorf("refusal = %q, want %q", rc.Refusal, want)
	}
}

func TestWidgetResolveRoomNotXMPPLogin(t *testing.T) {
	h := newWidgetHarness(t, true)
	h.ul.Client = nil
	rc, err := h.x.widgetResolveRoom(context.Background(), h.user, "!mgmt:example.com")
	if err != nil || rc.Login != nil || rc.Refusal != "Log in to Jabber first." {
		t.Errorf("%+v, %v", rc, err)
	}
}

func TestWidgetEligibleRooms(t *testing.T) {
	h := newWidgetHarness(t, true)
	h.add(testPortal("+15551234567@cheogram.com", h.ul.ID, database.RoomTypeDM, "!dm:x"))
	h.add(testPortal("+15557654321@cheogram.com", "other@example.com", database.RoomTypeDM, "!other:x"))
	h.add(testPortal("+15551234567,+15557654321@cheogram.com", h.ul.ID, database.RoomTypeGroupDM, "!mms:x"))
	h.add(testPortal("room@conference.example.com", h.ul.ID, database.RoomTypeGroupDM, "!muc:x"))
	h.add(testPortal("+15559999999@cheogram.com", h.ul.ID, database.RoomTypeDM, ""))
	h.x.listPortals = func(context.Context) ([]*bridgev2.Portal, error) {
		var out []*bridgev2.Portal
		for _, p := range h.port {
			out = append(out, p)
		}
		out = append(out, testPortal("+15559999999@cheogram.com", h.ul.ID, database.RoomTypeDM, ""))
		return out, nil
	}
	got := map[id.RoomID]string{}
	refs, err := h.x.widgetEligibleRooms(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range refs {
		got[r.RoomID] = r.Title
	}
	if len(got) != 2 || got["!dm:x"] != "Call +15551234567" || got["!mgmt:example.com"] != "" {
		t.Errorf("eligible = %v", got)
	}
	if _, ok := got["!mgmt:example.com"]; !ok {
		t.Error("management room missing")
	}

	// Without a SIP account: install sweeps list nothing, the removal sweep still lists the rooms.
	h.x.accounts[h.ul.ID].acct = nil
	refs, _ = h.x.widgetEligibleRooms(context.Background(), true)
	if len(refs) != 0 {
		t.Errorf("no account: %v", refs)
	}
	refs, _ = h.x.widgetEligibleRooms(context.Background(), false)
	if len(refs) != 2 {
		t.Errorf("removal list: %v", refs)
	}
}

func TestWidgetHooksAreNilSafe(t *testing.T) {
	x, _ := testCallsExt(t, true)
	x.widgetEnqueue("!r:x", "t")
	x.widgetSweep(context.Background())
	x.stopWidget()
	if x.widgetCommandInstaller() != nil {
		t.Error("widget disabled: installer dep must be a nil interface")
	}
}

func TestWidgetCommandInstallerLazy(t *testing.T) {
	x, _ := testCallsExt(t, true)
	x.cfg.Widget.Enabled = true
	in := x.widgetCommandInstaller()
	if in == nil {
		t.Fatal("enabled widget needs an installer dep")
	}
	if err := in.Ensure(context.Background(), "!r:x", ""); !errors.Is(err, errWidgetNotRunning) {
		t.Errorf("before start: %v", err)
	}
	f := &fakeWidgetInst{}
	x.widgetInst = f
	in.Enqueue("!r:x", "Call +1")
	if len(f.enqueued) != 1 {
		t.Errorf("enqueued = %v", f.enqueued)
	}
	var _ hostkit.WidgetInstaller = in
}

func TestWidgetSweepAfterPLSweep(t *testing.T) {
	h := newWidgetHarness(t, true)
	h.x.sweepCallPowerLevels(context.Background(), h.ul.ID)
	waitFor(t, "widget sweep requested", func() bool {
		h.inst.mu.Lock()
		defer h.inst.mu.Unlock()
		return len(h.inst.sweeps) == 1 && h.inst.sweeps[0] == widget.TriggerLogin
	})
}

func TestStartWidgetBadBridge(t *testing.T) {
	x, _ := testCallsExt(t, true)
	x.cfg.Widget = hostkit.WidgetConfig{Enabled: true}
	if err := x.startWidget(); err == nil {
		t.Error("a widget that cannot be built must fail start")
	}
	if x.widgetInstallerNow() != nil {
		t.Error("no installer after failure")
	}
}

func TestWidgetResolveDevice(t *testing.T) {
	ctx := context.Background()
	h := newWidgetHarness(t, true)
	rc, err := h.x.widgetResolveDevice(ctx, h.user, h.ul.ID)
	if err != nil || rc.Kind != widget.KindDevice || rc.Login != h.ul || rc.Refusal != "" {
		t.Fatalf("ready: %+v, %v", rc, err)
	}
	if _, err = h.x.widgetResolveDevice(ctx, h.user, "nobody@example.com"); !errors.Is(err, widget.ErrDeviceLoginGone) {
		t.Errorf("missing login: %v", err)
	}
	other := &bridgev2.User{User: &database.User{MXID: "@other:example.com"}}
	if _, err = h.x.widgetResolveDevice(ctx, other, h.ul.ID); !errors.Is(err, widget.ErrDeviceLoginGone) {
		t.Errorf("foreign login: %v", err)
	}
	if _, err = h.x.widgetResolveDevice(ctx, nil, h.ul.ID); !errors.Is(err, widget.ErrDeviceLoginGone) {
		t.Errorf("nil user: %v", err)
	}
	h.ul.Client = nil
	rc, err = h.x.widgetResolveDevice(ctx, h.user, h.ul.ID)
	if err != nil || rc.Kind != widget.KindDevice || rc.Login != nil || rc.Refusal != "Log in to Jabber first." {
		t.Errorf("not xmpp: %+v, %v", rc, err)
	}
}

func TestWidgetResolveDeviceNoAccount(t *testing.T) {
	h := newWidgetHarness(t, false)
	rc, err := h.x.widgetResolveDevice(context.Background(), h.user, h.ul.ID)
	want := "No SIP account is set up for this login. Use `!jc sip-login`."
	if err != nil || rc.Kind != widget.KindDevice || rc.Login != nil || rc.Refusal != want {
		t.Errorf("%+v, %v", rc, err)
	}
}

func TestDialpadCommandDepsLazy(t *testing.T) {
	x, _ := testCallsExt(t, true)
	if x.dialpadCommandDeps() != nil {
		t.Fatal("PWA off must give a nil interface")
	}
	x.cfg.Widget.Enabled = true
	if x.dialpadCommandDeps() != nil {
		t.Fatal("PWA off (widget on) must give a nil interface")
	}
	x.cfg.Widget.PWA.Enabled = true
	d := x.dialpadCommandDeps()
	if d == nil {
		t.Fatal("PWA on needs a dialpad dep")
	}
	ctx := context.Background()
	if _, err := d.Issue(ctx, "@u:x", "l", "!r:x", ""); !errors.Is(err, errDialpadNotRunning) {
		t.Errorf("Issue before start: %v", err)
	}
	if err := d.AttachNotice(ctx, "i", "!r:x", "$e"); !errors.Is(err, errDialpadNotRunning) {
		t.Errorf("AttachNotice before start: %v", err)
	}
	if _, err := d.List(ctx, "@u:x"); !errors.Is(err, errDialpadNotRunning) {
		t.Errorf("List before start: %v", err)
	}
	if _, err := d.Revoke(ctx, "@u:x", "all"); !errors.Is(err, errDialpadNotRunning) {
		t.Errorf("Revoke before start: %v", err)
	}
	if got := x.callCommandDeps().Dialpad; got == nil {
		t.Error("callCommandDeps must carry the adapter")
	}
}

func TestStartWidgetPWAStoreErrorFailsStart(t *testing.T) {
	x, _ := testCallsExt(t, true)
	x.cfg.Widget = hostkit.WidgetConfig{Enabled: true}
	x.cfg.Widget.PWA.Enabled = true
	x.wseam.openDevices = func(context.Context) (*widget.DeviceStore, error) { return nil, errors.New("db down") }
	err := x.startWidget()
	if err == nil || !strings.Contains(err.Error(), "db down") {
		t.Errorf("err = %v, want store failure", err)
	}
	if x.widgetInstallerNow() != nil {
		t.Error("no installer after failure")
	}
}
