package connector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const (
	cardDAVMaxConsecutiveFailures = 5
	cardDAVPreviewLines           = 20
)

// Executor outcomes.
const (
	outcomeDone        = "ok"
	outcomeMaxAdds     = "max_adds"
	outcomeFailures    = "consecutive_failures"
	outcomeDisconnect  = "disconnected"
	outcomeCancelled   = "cancelled"
	outcomeFetchFailed = "fetch_failed"
)

// execEnv is what the executor needs from the XMPP client (a seam for tests).
type execEnv struct {
	setItem  func(ctx context.Context, jid, name string) error
	inRoster func(jid string) bool
	loggedIn func() bool
	wait     func(ctx context.Context, d time.Duration) error // rate limiter sleep
	debug    func(jid string)                                 // per-add log (JID only)
	perSec   int
	maxAdds  int
	maxFails int
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// applyResult is what applyPlan did.
type applyResult struct {
	Added     int
	Failed    int
	Already   int // in the roster by the time of the re-check
	Remaining int // planned adds not attempted (max adds, early stop)
	Outcome   string
	FailRun   int // consecutive failures when it stopped
}

// applyPlan adds the planned entries serially (rate limited). Per-add failures are counted; it stops
// after maxFails consecutive failures, when the connection is gone, at maxAdds and on cancellation.
// A "not connected" failure stops at once without counting towards the consecutive-failure rule.
func applyPlan(ctx context.Context, env execEnv, adds []rosterAdd) applyResult {
	res := applyResult{Outcome: outcomeDone}
	interval := time.Second
	if env.perSec > 0 {
		interval = time.Second / time.Duration(env.perSec)
	}
	attempted, consecutive := 0, 0
	for i, a := range adds {
		if ctx.Err() != nil {
			res.Outcome, res.Remaining = outcomeCancelled, len(adds)-i
			return res
		}
		if !env.loggedIn() {
			res.Outcome, res.Remaining = outcomeDisconnect, len(adds)-i
			return res
		}
		if env.inRoster(a.JID) { // re-check: a concurrent add or rename wins
			res.Already++
			continue
		}
		if env.maxAdds > 0 && attempted >= env.maxAdds {
			res.Outcome, res.Remaining = outcomeMaxAdds, len(adds)-i
			return res
		}
		if attempted > 0 {
			if err := env.wait(ctx, interval); err != nil {
				res.Outcome, res.Remaining = outcomeCancelled, len(adds)-i
				return res
			}
		}
		attempted++
		if err := env.setItem(ctx, a.JID, a.Name); err != nil {
			if strings.Contains(err.Error(), "not connected") {
				res.Outcome, res.Remaining = outcomeDisconnect, len(adds)-i
				return res
			}
			res.Failed++
			consecutive++
			if env.maxFails > 0 && consecutive >= env.maxFails {
				res.Outcome, res.FailRun, res.Remaining = outcomeFailures, consecutive, len(adds)-i-1
				return res
			}
			continue
		}
		consecutive = 0
		res.Added++
		if env.debug != nil {
			env.debug(a.JID)
		}
	}
	return res
}

// cardDAVLastRun is the in-memory summary of a login's last sync (lost on restart).
type cardDAVLastRun struct {
	At          time.Time
	Apply       bool
	Read        int
	Added       int
	Planned     int
	Already     int
	Skipped     int
	Failed      int
	BooksFailed int
	Outcome     string
}

type cardDAVRunState struct {
	running bool
	last    *cardDAVLastRun
}

var cardDAVRuns = struct {
	sync.Mutex
	m map[networkid.UserLoginID]*cardDAVRunState
}{m: map[networkid.UserLoginID]*cardDAVRunState{}}

// beginCardDAVRun marks a run as started; false if one is already running for the login.
func beginCardDAVRun(id networkid.UserLoginID) bool {
	cardDAVRuns.Lock()
	defer cardDAVRuns.Unlock()
	st := cardDAVRuns.m[id]
	if st == nil {
		st = &cardDAVRunState{}
		cardDAVRuns.m[id] = st
	}
	if st.running {
		return false
	}
	st.running = true
	return true
}

func endCardDAVRun(id networkid.UserLoginID, last *cardDAVLastRun) {
	cardDAVRuns.Lock()
	defer cardDAVRuns.Unlock()
	if st := cardDAVRuns.m[id]; st != nil {
		st.running = false
		if last != nil {
			st.last = last
		}
	}
}

func cardDAVRunStatus(id networkid.UserLoginID) (running bool, last *cardDAVLastRun) {
	cardDAVRuns.Lock()
	defer cardDAVRuns.Unlock()
	if st := cardDAVRuns.m[id]; st != nil {
		c := *st
		if c.last != nil {
			l := *c.last
			return c.running, &l
		}
		return c.running, nil
	}
	return false, nil
}

// cardDAVSyncJob is one detached preview or apply run. It captures everything it needs; it never uses
// the command event of the handler that started it.
type cardDAVSyncJob struct {
	loginID  networkid.UserLoginID
	cfg      CardDAVConfig
	creds    *cardDAVCreds
	apply    bool
	prefix   string
	snapshot func() (map[string]struct{}, bool)
	env      execEnv
	fetch    func(ctx context.Context, cfg CardDAVConfig, creds *cardDAVCreds) (*fetchSummary, error)
	reply    func(text string)
	log      *zerolog.Logger
}

// run executes the job and always ends the busy marker. span is the job's own root span.
func (j *cardDAVSyncJob) run(ctx context.Context, span trace.Span) {
	last := &cardDAVLastRun{At: time.Now(), Apply: j.apply, Outcome: outcomeFetchFailed}
	defer func() {
		endCardDAVRun(j.loginID, last)
		span.SetAttributes(
			attribute.Bool("carddav.apply", j.apply),
			attribute.Int("carddav.read", last.Read),
			attribute.Int("carddav.planned", last.Planned),
			attribute.Int("carddav.added", last.Added),
			attribute.Int("carddav.already", last.Already),
			attribute.Int("carddav.skipped", last.Skipped),
			attribute.Int("carddav.failed", last.Failed),
			attribute.Int("carddav.books_failed", last.BooksFailed),
			attribute.String("carddav.outcome", last.Outcome),
		)
		if last.Outcome != outcomeDone && last.Outcome != outcomeMaxAdds {
			span.SetStatus(codes.Error, last.Outcome)
		}
	}()
	host := hostOf(j.creds.URL)
	span.SetAttributes(attribute.String("carddav.host", host))
	s, err := j.fetch(ctx, j.cfg, j.creds)
	if err != nil {
		last.Outcome = cardDAVErrorKind(err)
		j.log.Info().Str("host", host).Str("outcome", last.Outcome).Msg("CardDAV sync failed")
		j.reply(fmt.Sprintf("CardDAV sync failed: %s", err.Error()))
		return
	}
	last.BooksFailed = s.BooksFailed
	jids, loaded := j.snapshot()
	if !loaded {
		last.Outcome = "roster_not_loaded"
		j.reply("The roster has not loaded yet. Try again in a moment.")
		return
	}
	plan := planCardDAVSync(s.Contacts, jids, s.Skips)
	last.Read, last.Planned, last.Already, last.Skipped = plan.Read, len(plan.Adds), plan.AlreadyInRoster, totalSkipped(plan.Skipped)
	notes := j.fetchNotes(s)
	if !j.apply {
		last.Outcome = outcomeDone
		j.log.Info().Str("host", host).Int("read", plan.Read).Int("planned", len(plan.Adds)).
			Int("already", plan.AlreadyInRoster).Int("skipped", last.Skipped).Msg("CardDAV preview finished")
		j.reply(j.previewText(plan) + notes)
		return
	}
	res := applyPlan(ctx, j.env, plan.Adds)
	last.Added, last.Failed, last.Already, last.Outcome = res.Added, res.Failed, last.Already+res.Already, res.Outcome
	j.log.Info().Str("host", host).Int("read", plan.Read).Int("added", res.Added).Int("failed", res.Failed).
		Int("already", last.Already).Int("skipped", last.Skipped).Str("outcome", res.Outcome).Msg("CardDAV sync finished")
	j.reply(j.summaryText(plan, res) + notes)
}

func hostOf(rawURL string) string {
	if u, err := normalizeCardDAVURL(rawURL, CardDAVConfig{AllowInsecureHTTP: true}); err == nil {
		return u.Hostname()
	}
	return ""
}

// fetchNotes mentions books that failed and truncated answers.
func (j *cardDAVSyncJob) fetchNotes(s *fetchSummary) string {
	var b strings.Builder
	if s.BooksFailed > 0 {
		fmt.Fprintf(&b, "\n%d address book(s) could not be read and were skipped (the others were synced).", s.BooksFailed)
	}
	if s.Truncated {
		b.WriteString("\nThe server said its answer was truncated; some contacts may be missing.")
	}
	return b.String()
}

func (j *cardDAVSyncJob) previewText(p cardDAVPlan) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CardDAV preview: %d contacts read, %d numbers would be added, %d already in your roster, %d skipped%s.",
		p.Read, len(p.Adds), p.AlreadyInRoster, totalSkipped(p.Skipped), reasonSuffix(p.Skipped))
	if len(p.Adds) == 0 {
		b.WriteString("\nNothing to add.")
		return b.String()
	}
	n := min(len(p.Adds), cardDAVPreviewLines)
	for _, a := range p.Adds[:n] {
		fmt.Fprintf(&b, "\n- %s: %s (%s)", a.Name, a.E164, a.Label)
	}
	if len(p.Adds) > n {
		fmt.Fprintf(&b, "\n...and %d more.", len(p.Adds)-n)
	}
	if len(p.Adds) > cardDAVMaxAddsPerRun {
		fmt.Fprintf(&b, "\nOne run adds at most %d; run it again to continue.", cardDAVMaxAddsPerRun)
	}
	fmt.Fprintf(&b, "\nRun `%s carddav-sync apply` to add them.", j.prefix)
	return b.String()
}

func (j *cardDAVSyncJob) summaryText(p cardDAVPlan, r applyResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CardDAV sync finished: %d contacts read, %d numbers added, %d already in your roster, %d skipped%s.",
		p.Read, r.Added, p.AlreadyInRoster+r.Already, totalSkipped(p.Skipped), reasonSuffix(p.Skipped))
	if r.Failed > 0 {
		fmt.Fprintf(&b, " %d failed.", r.Failed)
	}
	switch r.Outcome {
	case outcomeFailures:
		fmt.Fprintf(&b, "\nStopped early: the server refused %d roster changes in a row.", r.FailRun)
	case outcomeDisconnect:
		b.WriteString("\nStopped early: the Jabber connection was lost. Run it again once reconnected.")
	case outcomeCancelled:
		b.WriteString("\nStopped early: the bridge is shutting down. Run it again to continue.")
	case outcomeMaxAdds:
		fmt.Fprintf(&b, "\nAdded %d; run it again to continue.", r.Added)
	}
	return b.String()
}

func reasonSuffix(skipped map[string]int) string {
	if s := skippedSummary(skipped); s != "" {
		return " (" + s + ")"
	}
	return ""
}
