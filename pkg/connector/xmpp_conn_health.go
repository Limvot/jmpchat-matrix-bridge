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
	keepaliveTick      = 30 * time.Second // how often the loop looks at the idle time
	keepaliveIdle      = 90 * time.Second // ping only after this long without inbound traffic
	keepaliveTimeout   = 30 * time.Second // per ping
	keepaliveMaxMiss   = 2                // consecutive unanswered pings before the session is closed
	routeCheckInterval = 3 * time.Minute  // how often the session pings its own full JID
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

// runKeepalive runs two checks from one ticker loop (XEP-0199), on the session context, never on the
// Serve goroutine, holding no IQ response open. Successful pings are not traced or logged.
//
// Server ping: after keepaliveIdle without inbound traffic the server domain is pinged; any reply,
// including a stanza error, proves the connection is alive. keepaliveMaxMiss consecutive misses close it.
//
// Route check: every routeCheckInterval, independent of inbound traffic, the session pings its OWN full
// JID. The server must route that ping back to us, our ping handler answers it and the server routes
// the result back. Only a result counts; any error reply (the server saying our full JID is not
// routable) is a miss. This catches a "deaf" session whose TCP connection and replies to our own IQs
// (e.g. MAM) still work while the server no longer delivers stanzas to us (inbound MAM replies keep
// lastInbound fresh, so the idle ping never fires). keepaliveMaxMiss consecutive route misses close
// the connection. Both never run in one tick: a due route check also proves the connection.
//
// Closing the connection makes Serve return so the reconnect loop takes over.
func (x *xmppClient) runKeepalive(ctx context.Context, sess *xmpp.Session, server jid.JID) {
	ticker := time.NewTicker(keepaliveTick)
	defer ticker.Stop()
	misses, routeMisses := 0, 0
	lastRoute := time.Now()
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
		if time.Since(lastRoute) >= routeCheckInterval {
			lastRoute = time.Now()
			misses = 0
			err := x.sendPing(ctx, sess, sess.LocalAddr(), routePing)
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				routeMisses = 0
				continue
			}
			routeMisses++
			x.log.Debug().Err(err).Int("misses", routeMisses).Msg("XMPP route check ping to own address failed")
			if routeMisses < keepaliveMaxMiss {
				continue
			}
			x.log.Warn().Int("misses", routeMisses).Msg("XMPP route check: own address not reachable, closing session")
			_, span := telemetry.Start(context.Background(), "xmpp.keepalive.route_timeout")
			if span.IsRecording() {
				span.SetAttributes(attribute.Int("xmpp.keepalive.route_misses", routeMisses))
			}
			span.End()
			x.closeConn(sess)
			return
		}
		if time.Since(time.Unix(0, x.lastInbound.Load())) < keepaliveIdle {
			misses = 0
			continue
		}
		err := x.sendPing(ctx, sess, server, ping.Send)
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

// sendPing sends one ping with keepaliveTimeout; the pingFunc test seam replaces send.
func (x *xmppClient) sendPing(ctx context.Context, sess *xmpp.Session, to jid.JID, send func(context.Context, *xmpp.Session, jid.JID) error) error {
	pctx, cancel := context.WithTimeout(ctx, keepaliveTimeout)
	defer cancel()
	if x.pingFunc != nil {
		return x.pingFunc(pctx, sess, to)
	}
	return send(pctx, sess, to)
}

// routePing is ping.Send without its special case: ping.Send maps service-unavailable to success
// (a client that does not implement XEP-0199), but for the route check any error reply, including the
// server's service-unavailable/recipient-unavailable for an unroutable full JID, must be an error.
// mellium matches the result to this IQ by id and stanza name only (the response from is not compared
// with the request to, and only a from equal to our bare JID is blanked), so the result arriving with
// from = our full JID is delivered to the waiting UnmarshalIQ.
func routePing(ctx context.Context, sess *xmpp.Session, to jid.JID) error {
	return sess.UnmarshalIQ(ctx, ping.IQ{IQ: stanza.IQ{Type: stanza.GetIQ, To: to}}.TokenReader(), nil)
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
