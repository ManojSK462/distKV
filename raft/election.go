package raft

import (
	"sync"
	"time"
)

func (n *Node) electionTicker() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-time.After(electionCheckInterval):
		}

		n.mu.Lock()
		expired := n.state != Leader && time.Now().After(n.electionDeadline)
		n.mu.Unlock()

		if expired {
			n.startElection()
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.stopped || n.state == Leader {
		n.mu.Unlock()
		return
	}

	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = -1
	n.persistLocked()
	n.resetElectionDeadlineLocked()

	term := n.currentTerm
	lastIndex := n.lastLogIndexLocked()
	lastTerm := n.log[lastIndex].Term
	n.mu.Unlock()

	var (
		votesMu sync.Mutex
		votes   = 1
	)

	for _, peerID := range n.peerIDs() {
		go func(peerID int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply
			if !n.transport.call(peerID, "Raft.RequestVote", args, &reply) {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.stepDownLocked(reply.Term)
				return
			}

			if n.state != Candidate || n.currentTerm != term {
				return
			}
			if !reply.VoteGranted {
				return
			}

			votesMu.Lock()
			votes++
			won := votes >= n.quorum()
			votesMu.Unlock()

			if won {
				n.becomeLeaderLocked()
			}
		}(peerID)
	}
}

func (n *Node) becomeLeaderLocked() {
	if n.state != Candidate {
		return
	}
	n.state = Leader
	n.leaderID = n.id

	lastIndex := n.lastLogIndexLocked()
	for _, peerID := range n.peerIDs() {
		n.nextIndex[peerID] = lastIndex + 1
		n.matchIndex[peerID] = 0
	}

	n.readyIndex = n.appendEntryLocked(Command{Op: OpNoop})
	go n.heartbeatLoop(n.currentTerm)
	n.broadcastAppendEntriesLocked()
}

func (n *Node) heartbeatLoop(term int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
		}

		n.mu.Lock()
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		n.broadcastAppendEntriesLocked()
		n.mu.Unlock()
	}
}

func (n *Node) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	}

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	if args.Term < n.currentTerm {
		return
	}

	alreadyVoted := n.votedFor != -1 && n.votedFor != args.CandidateID
	if alreadyVoted || !n.candidateUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		return
	}

	n.votedFor = args.CandidateID
	n.persistLocked()
	n.resetElectionDeadlineLocked()
	reply.VoteGranted = true
}

func (n *Node) candidateUpToDateLocked(candidateIndex, candidateTerm int) bool {
	localIndex := n.lastLogIndexLocked()
	localTerm := n.log[localIndex].Term
	if candidateTerm != localTerm {
		return candidateTerm > localTerm
	}
	return candidateIndex >= localIndex
}
