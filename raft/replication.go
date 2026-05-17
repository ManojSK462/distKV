package raft

// broadcastAppendEntriesLocked replicates the log to every peer concurrently.
// It serves double duty: replicating real entries and acting as a heartbeat.
func (n *Node) broadcastAppendEntriesLocked() {
	term := n.currentTerm
	for _, peerID := range n.peerIDs() {
		go n.replicateTo(peerID, term)
	}
}

// replicateTo sends one AppendEntries RPC to a single peer and processes the
// reply: it advances the peer's progress on success, or rewinds nextIndex on a
// log conflict so the next attempt converges.
func (n *Node) replicateTo(peerID, term int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	nextIndex := n.nextIndex[peerID]
	prevIndex := nextIndex - 1
	prevTerm := n.log[prevIndex].Term
	// Copy the entries so the RPC never races with later log mutation.
	entries := append([]LogEntry(nil), n.log[nextIndex:]...)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	var reply AppendEntriesReply
	if !n.transport.call(peerID, "Raft.AppendEntries", args, &reply) {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	// Discard a reply whose context has expired.
	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		matched := prevIndex + len(entries)
		if matched > n.matchIndex[peerID] {
			n.matchIndex[peerID] = matched
		}
		n.nextIndex[peerID] = matched + 1
		n.advanceCommitLocked()
		return
	}

	// The follower rejected the entries: rewind and let the next round retry.
	n.nextIndex[peerID] = n.rewindNextIndexLocked(reply)
}

// rewindNextIndexLocked computes the next index to try for a peer that
// reported a log conflict. Using the conflict hint, the leader skips an entire
// mismatched term in one step rather than backing up index by index.
func (n *Node) rewindNextIndexLocked(reply AppendEntriesReply) int {
	if reply.ConflictTerm != 0 {
		// If the leader also has the conflicting term, resume just past
		// its last entry for that term.
		for i := n.lastLogIndexLocked(); i >= 1; i-- {
			if n.log[i].Term == reply.ConflictTerm {
				return i + 1
			}
		}
	}
	// The leader lacks the term entirely, or the follower's log was simply
	// too short: fall back to the first conflicting index it reported.
	if reply.ConflictIndex < 1 {
		return 1
	}
	return reply.ConflictIndex
}

// advanceCommitLocked raises commitIndex to the highest entry replicated on a
// majority. Crucially, an entry is committed by counting only when it belongs
// to the leader's current term; older entries become committed implicitly once
// a current-term entry above them is (Raft §5.4.2). Log terms are
// non-decreasing, so the scan can stop at the first older entry.
func (n *Node) advanceCommitLocked() {
	for index := n.lastLogIndexLocked(); index > n.commitIndex; index-- {
		if n.log[index].Term != n.currentTerm {
			break
		}
		replicas := 1 // the leader holds the entry itself
		for _, peerID := range n.peerIDs() {
			if n.matchIndex[peerID] >= index {
				replicas++
			}
		}
		if replicas >= n.quorum() {
			n.commitIndex = index
			n.applyCond.Broadcast()
			return
		}
	}
}

// handleAppendEntries implements the receiver side of the AppendEntries RPC:
// the consistency check, conflict reporting, log merge, and commit propagation.
func (n *Node) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	}

	reply.Term = n.currentTerm
	reply.Success = false
	reply.ConflictIndex = 0
	reply.ConflictTerm = 0

	if args.Term < n.currentTerm {
		return // a stale leader; reject and let it discover the newer term
	}

	// The sender is a legitimate leader for this term. Adopt follower state
	// and defer the next election.
	n.state = Follower
	n.leaderID = args.LeaderID
	n.resetElectionDeadlineLocked()

	// Consistency check: the follower must already hold an entry that
	// matches PrevLogIndex/PrevLogTerm.
	if args.PrevLogIndex > n.lastLogIndexLocked() {
		// The log is too short. Point the leader at its tail.
		reply.ConflictIndex = n.lastLogIndexLocked() + 1
		return
	}
	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// The terms diverge. Report the offending term and its first
		// index so the leader can skip the whole run at once.
		reply.ConflictTerm = n.log[args.PrevLogIndex].Term
		first := args.PrevLogIndex
		for first > 1 && n.log[first-1].Term == reply.ConflictTerm {
			first--
		}
		reply.ConflictIndex = first
		return
	}

	// Merge the entries: keep any matching prefix, truncate the log at the
	// first conflict, and append the remainder. A matching entry is never
	// truncated, so a stale heartbeat cannot erase committed history.
	mutated := false
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index <= n.lastLogIndexLocked() {
			if n.log[index].Term == entry.Term {
				continue
			}
			n.log = n.log[:index]
		}
		n.log = append(n.log, entry)
		mutated = true
	}
	if mutated {
		n.persistLocked()
	}

	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastLogIndexLocked())
		n.applyCond.Broadcast()
	}
	reply.Success = true
}
