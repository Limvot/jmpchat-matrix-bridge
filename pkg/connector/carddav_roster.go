package connector

// rosterSnapshot returns a copy of the lowercased bare JIDs of the roster (named or not) and whether the
// first complete roster fetch of this client has finished (before that the set may be partial).
func (x *xmppClient) rosterSnapshot() (jids map[string]struct{}, loaded bool) {
	x.rosterMu.RLock()
	defer x.rosterMu.RUnlock()
	jids = make(map[string]struct{}, len(x.rosterJIDs))
	for j := range x.rosterJIDs {
		jids[j] = struct{}{}
	}
	return jids, x.rosterLoaded
}
