package connector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/ping"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Connection health: bad-credentials bridge state (the reconnect loop stops), XEP-0199 keepalive and
// the wake-up channel that lets the reconnect loop react to a session end.

// Keepalive timing. Variables so tests can shorten them.
var (
	keepaliveTick    = 30 * time.Second // how often the loop looks at the idle time
	keepaliveIdle    = 90 * time.Second // ping only after this long without inbound traffic
	keepaliveTimeout = 30 * time.Second // per ping
	keepaliveMaxMiss = 2                // consecutive unanswered pings before the session is closed
)

// authNotAuthorizedLimit is how many consecutive not-authorized results make the failure final (one
// retry absorbs servers that briefly answer not-authorized while an auth backend restarts).
const authNotAuthorizedLimit = 2

// classifyAuth maps a dial error to a SASL condition. permanent is true for conditions that retrying
// cannot fix on its own (not-authorized, account-disabled, credentials-expired, invalid-authzid);
// temporary-auth-failure, aborted and the rest are not permanent. ok is false when err is no SASL failure.
func classifyAuth(err error) (cond string, permanent bool, ok bool) {
	cond, ok = saslCondition(err)
	if !ok {
		return "", false, false
	}
	switch cond {
	case "not-authorized", "account-disabled", "credentials-expired", "invalid-authzid":
		return cond, true, true
	}
	return cond, false, true
}

// authFailureMessage is the fixed English text per condition (the server's SASL text is never used).
func authFailureMessage(cond string) string {
	switch cond {
	case "account-disabled":
		return "The Jabber account is disabled on the server."
	case "credentials-expired":
		return "The Jabber password has expired."
	case "invalid-authzid":
		return "The server rejected the Jabber account name."
	default:
		return "The Jabber server rejected the username or password."
	}
}

// isTLSVerifyError reports a certificate verification failure.
func isTLSVerifyError(err error) bool {
	var cve *tls.CertificateVerificationError
	var he x509.HostnameError
	var ua x509.UnknownAuthorityError
	var ci x509.CertificateInvalidError
	return errors.As(err, &cve) || errors.As(err, &he) || errors.As(err, &ua) || errors.As(err, &ci)
}

// sendState reports a bridge state (tests replace the framework with stateHook).
func (x *xmppClient) sendState(s status.BridgeState) {
	if x.stateHook != nil {
		x.stateHook(s)
		return
	}
	if x.login != nil {
		x.login.BridgeState.Send(s)
	}
}

// wakeChan returns the reconnect loop wake channel (buffered 1, created on first use).
func (x *xmppClient) wakeChan() chan struct{} {
	x.wakeOnce.Do(func() { x.wakeCh = make(chan struct{}, 1) })
	return x.wakeCh
}

// wakeReconnect makes a sleeping ReconnectLoop re-check now. Never blocks.
func (x *xmppClient) wakeReconnect() {
	select {
	case x.wakeChan() <- struct{}{}:
	default:
	}
}

// sendBadCredentials reports the final authentication failure state.
func (x *xmppClient) sendBadCredentials(cond string) {
	x.sendState(status.BridgeState{
		StateEvent: status.StateBadCredentials,
		Error:      status.BridgeStateErrorCode("jabber-" + cond),
		Message:    authFailureMessage(cond),
	})
}

// handleDialFailure classifies a failed dial in Connect (x.mu held by the caller): a final SASL failure
// marks the client authFailed (the reconnect loop stops), everything else stays a transient disconnect.
func (x *xmppClient) handleDialFailure(ctx context.Context, err error) {
	log := x.logFor(ctx)
	cond, permanent, isSASL := classifyAuth(err)
	if isSASL {
		final := false
		switch {
		case cond == "not-authorized":
			// bridgev2's own Connect and the reconnect loop's first pass can fail back to back;
			// two answers inside reconnectBackoffStart count as one (transient) failure.
			now := time.Now()
			if x.authFailures == 0 || now.Sub(x.lastAuthFailure) >= reconnectBackoffStart {
				x.authFailures++
				x.lastAuthFailure = now
				final = x.authFailures >= authNotAuthorizedLimit
			}
		case permanent:
			final = true
		}
		if final {
			x.authFailed.Store(true)
			x.authCond = cond
			log.Warn().Str("sasl_condition", cond).Msg("XMPP login rejected by the server; not reconnecting until the next login")
			x.sendBadCredentials(cond)
			go x.postAuthFailedNotice(cond)
			return
		}
		log.Debug().Str("sasl_condition", cond).Int("failures", x.authFailures).Msg("XMPP authentication failed, will retry")
		x.sendState(status.BridgeState{StateEvent: status.StateTransientDisconnect, Error: status.BridgeStateErrorCode("jabber-" + cond), Message: "authentication failed, retrying"})
		return
	}
	if isTLSVerifyError(err) {
		x.sendState(status.BridgeState{StateEvent: status.StateUnknownError, Error: "jabber-tls-error", Message: "The server's TLS certificate could not be verified."})
		return
	}
	x.sendState(status.BridgeState{StateEvent: status.StateTransientDisconnect, Message: "connection failed"})
}

// postAuthFailedNotice posts the management room notice (own context; runs in a goroutine).
func (x *xmppClient) postAuthFailedNotice(cond string) {
	prefix := "!jabber"
	if x.conn != nil && x.conn.br != nil && x.conn.br.Config != nil && x.conn.br.Config.CommandPrefix != "" {
		prefix = x.conn.br.Config.CommandPrefix
	}
	text := fmt.Sprintf("Your Jabber login %s was rejected by the server (%s). Log in again with `%s login password` (or `%s login password-server`).",
		string(x.loginID), cond, prefix, prefix)
	if x.mgmtNoticeHook != nil {
		x.mgmtNoticeHook(text)
		return
	}
	if x.conn == nil || x.conn.br == nil || x.conn.br.Bot == nil || x.login == nil || x.login.User == nil || x.login.User.ManagementRoom == "" {
		return
	}
	ctx, span := telemetry.Start(context.Background(), "xmpp.auth_failed_notice")
	defer span.End()
	content := format.RenderMarkdown(text, true, false)
	content.MsgType = event.MsgNotice
	if _, err := x.conn.br.Bot.SendMessage(ctx, x.login.User.ManagementRoom, event.EventMessage, &event.Content{Parsed: &content}, nil); err != nil {
		x.logFor(ctx).Warn().Err(err).Msg("Failed to send bad credentials notice")
	}
}

// runKeepalive pings the server after keepaliveIdle without inbound traffic (XEP-0199) and closes the
// session's connection after keepaliveMaxMiss unanswered pings so Serve returns and the reconnect loop
// takes over. It runs on the session context, never on the Serve goroutine, and holds no IQ response
// open. Successful pings are not traced (they would flood traces).
func (x *xmppClient) runKeepalive(ctx context.Context, sess *xmpp.Session, server jid.JID) {
	ticker := time.NewTicker(keepaliveTick)
	defer ticker.Stop()
	misses := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		x.mu.Lock()
		current := x.session == sess
		x.mu.Unlock()
		if !current {
			return
		}
		if time.Since(time.Unix(0, x.lastInbound.Load())) < keepaliveIdle {
			misses = 0
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, keepaliveTimeout)
		var err error
		if x.pingFunc != nil {
			err = x.pingFunc(pctx, sess, server)
		} else {
			err = ping.Send(pctx, sess, server)
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		var stanzaErr stanza.Error
		if err == nil || errors.As(err, &stanzaErr) {
			// Any error reply (service-unavailable, feature-not-implemented, ...) proves the
			// connection is alive; only timeouts and transport errors count as misses.
			misses = 0
			continue
		}
		misses++
		x.log.Debug().Err(err).Int("misses", misses).Msg("XMPP keepalive ping failed")
		if misses < keepaliveMaxMiss {
			continue
		}
		x.log.Warn().Int("misses", misses).Msg("XMPP keepalive: no ping reply, closing session")
		_, span := telemetry.Start(context.Background(), "xmpp.keepalive.timeout")
		if span.IsRecording() {
			span.SetAttributes(attribute.Int("xmpp.keepalive.misses", misses))
		}
		span.End()
		// Closing the connection (not sess.Close, which writes </stream> and can block on a dead
		// socket) makes Serve return; serveMessages then clears the session and wakes the loop.
		x.closeConn(sess)
		return
	}
}

// closeConn closes the session's network connection; closeConnHook (tests only) replaces it.
func (x *xmppClient) closeConn(sess *xmpp.Session) {
	if x.closeConnHook != nil {
		x.closeConnHook(sess)
		return
	}
	_ = sess.Conn().Close()
}

// Reconnect loop timing (variables so tests can shorten them).
var (
	reconnectBackoffStart = 5 * time.Second  // first wait after a failed connect, doubled up to 5 minutes
	reconnectPoll         = 30 * time.Second // re-check interval while connected (a session end wakes the loop sooner)
	minHealthySession     = 60 * time.Second // a session that ends sooner keeps (and doubles) the reconnect backoff
)
