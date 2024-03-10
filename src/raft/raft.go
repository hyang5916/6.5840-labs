package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	// "fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int // Last term server has seen
	votedFor          int // Candidate ID that received vote in current term.
	logs              []LogEntry
	state             int  // 0 if leader, 1 if candidate, 2 if follower
	heartbeatReceived bool // The time the last heartbeat was received.

	commitIndex int // Index of highest log entry committed.
	lastApplied int // Index of highest log entry known to be replicated on server.

	// Reinitialized after each election (these are just for leaders).
	nextIndex  []int // For each server, index of the next log entry to send to that server.
	matchIndex []int // For each server, index of highest log entry known to be replicated on server.
	// pendingLogEntires []LogEntry // New commands that still need to be sent to followers.

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == 0
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
	NextIndex   int
}

type AppendEntriesArgs struct {
	Term     int // Leader's term
	LeaderID int

	PrevLogIndex int        // Index of log entry immediately preceding the new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term                    int
	Success                 bool
	TermOfConflictingEntry  int
	IndexOfConflictingEntry int // Index of conflicting entry, if there is one.
	NextIndex               int // Index of next entry to send to this follower.
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	// fmt.Printf("%d requesting vote from %d\n", args.CandidateID, rf.me)
	rf.state = 1
	lastLogIndex := len(rf.logs) - 1
	var lastLogTerm int
	if len(rf.logs) == 0 {
		lastLogTerm = -1
	} else {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	// lastLogEntry := rf.logs[len(rf.logs) - 1]
	if rf.currentTerm > args.Term {
		// Reject vote
		// fmt.Printf("rejected vote reason 1\n")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		// Checks that this peer hasn't voted for a different peer and that the candidate's log is at least as up to date as this peer's log.
		reply.VoteGranted = true
		reply.NextIndex = len(rf.logs)
		// fmt.Printf("%d granting vote for %d\n", rf.me, args.CandidateID)
		rf.votedFor = args.CandidateID
		rf.heartbeatReceived = true
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.state = 2 // Revert to follower
			rf.votedFor = -1
		}
	} else {
		// Reject vote
		// fmt.Printf("rejected vote reason 2, voted for %d\n", rf.votedFor)
		reply.VoteGranted = false
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = 2
		}
	}
	rf.mu.Unlock()
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = len(rf.logs)
		// fmt.Printf("FAILED on %d due to term! Current term: %d, Term in args: %d\n", rf.me, rf.currentTerm, args.Term)
	} else if len(args.Entries) > 0 && len(rf.logs) > 0 && len(rf.logs)-1 < args.PrevLogIndex {
		reply.Success = false
		reply.NextIndex = len(rf.logs)
		reply.TermOfConflictingEntry = rf.logs[len(rf.logs)-1].Term
		reply.IndexOfConflictingEntry = len(rf.logs) - 1
		// fmt.Printf("reason 1 - log rejected from %d for now, term of conflict: %d, index of conflict: %d, log: %v\n", rf.me, reply.TermOfConflictingEntry, reply.IndexOfConflictingEntry, rf.logs)
	} else if len(args.Entries) > 0 && len(rf.logs) > 0 && args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = false
		reply.NextIndex = len(rf.logs)
		reply.TermOfConflictingEntry = rf.logs[args.PrevLogIndex].Term
		index := 0
		for i := 0; i < len(rf.logs); i++ {
			// Find index of first entry with the conflicting term.
			if rf.logs[i].Term == reply.TermOfConflictingEntry {
				index = i
				break
			}
		}
		reply.IndexOfConflictingEntry = index - 1 // -1 because we want the next entry sent to overlap with last entry
		if reply.IndexOfConflictingEntry == -1 {
			reply.IndexOfConflictingEntry = 0 // Don't want to go negative...
		}

		// Delete the conflicting entries and all that follow it.
		rf.logs = rf.logs[0:index]
		// fmt.Printf("reason 2 - log rejected from %d for now, term of conflict: %d, index of conflict: %d, log: %v\n", rf.me, reply.TermOfConflictingEntry, reply.IndexOfConflictingEntry, rf.logs)

	} else {
		rf.state = 2                // Go back to being a follower
		rf.heartbeatReceived = true // Reset last heartbeat timestamp
		reply.Success = true

		// Update logs if not just a heartbeat
		if len(args.Entries) > 0 {
			if len(rf.logs) > 0 {
				rf.logs = rf.logs[0 : args.PrevLogIndex+1] // Delete every entry after the leader's prev log entry.
			}
			rf.logs = append(rf.logs, args.Entries...) // Add all the new entries.

			// fmt.Printf("Successfully updated log on %d to %v\n", rf.me, rf.logs)
		}
		if len(rf.logs) > 0 && args.LeaderCommit > rf.commitIndex && rf.currentTerm == args.Term && (args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.logs) && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm) {
			if args.LeaderCommit < len(rf.logs)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.logs) - 1
			}
			// fmt.Printf("commit index updated on %d, with prevLogIndex = %d, newCommitIndex: %d, len(logs): %d\n", rf.me, args.PrevLogIndex, rf.commitIndex, len(rf.logs))
		}

		if rf.currentTerm < args.Term { // Only do if strictly less than
			rf.currentTerm = args.Term // Set term to be up to date.
			rf.votedFor = -1           // Not voting yet in current term.
		}

		reply.NextIndex = len(rf.logs)
	}
	// fmt.Printf("Heartbeat sent for leader %d of term %d to peer %d of term %d\n", args.LeaderID, args.Term, rf.me, rf.currentTerm)
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// fmt.Printf("%d TRYING TO SEND request vote to %d\n", args.CandidateID, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// fmt.Printf("%d FINISHED sending request vote to %d\n", args.CandidateID, server)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Checks if currently the leader and if so if there's any pending log entries to send to peers.
func (rf *Raft) sendPendingLogEntries() {
	for !rf.killed() {
		term, isleader := rf.GetState()
		if isleader {
			rf.mu.Lock()
			// prevLogIndex := len(rf.logs) - 1
			// prevLogTerm := rf.logs[len(rf.logs)-1].term
			leaderCommit := rf.commitIndex
			// entries := rf.pendingLogEntires
			rf.mu.Unlock()

			for i := range rf.peers {
				// Send newly added log entries to all peers in parallel.
				rf.mu.Lock()
				// fmt.Printf("next index at %d: %v\n", i, rf.nextIndex)
				// fmt.Printf("CURRENT LOGS FOR %d: %v; nextIndex at i=%d: %v\n", rf.me, rf.logs, i, rf.nextIndex[i])
				if i != rf.me && len(rf.logs) != rf.nextIndex[i] {
					var entries []LogEntry
					var prevLogIndex int
					var prevLogTerm int
					if rf.nextIndex[i] < len(rf.logs) {
						entries = rf.logs[rf.nextIndex[i]:len(rf.logs)]
						prevLogIndex = rf.nextIndex[i] - 1
						if rf.nextIndex[i] == 0 { // Handle case where nextIndex[i] is still 0
							prevLogTerm = -1
						} else {
							prevLogTerm = rf.logs[prevLogIndex].Term
						}
					} else { // If follower has more logs than the leader that we need to remove.
						entries = rf.logs
						prevLogIndex = rf.nextIndex[i] - 1
						if rf.nextIndex[i] == 0 { // Handle case where nextIndex[i] is still 0
							prevLogTerm = -1
						} else {
							prevLogTerm = rf.logs[len(rf.logs)-1].Term
						}
					}

					if rf.nextIndex[i] == 0 { // Handle case where nextIndex[i] is still 0
						prevLogTerm = -1
					}

					rf.mu.Unlock()
					go func(i int, term int, leaderCommit int, entries []LogEntry, prevLogIndex int, prevLogTerm int) {
						args := AppendEntriesArgs{}
						reply := AppendEntriesReply{}

						args.Term = term
						args.LeaderID = rf.me
						args.PrevLogIndex = prevLogIndex
						args.PrevLogTerm = prevLogTerm
						args.LeaderCommit = leaderCommit
						args.Entries = entries

						for {
							// fmt.Printf("sending entries %v to %d with prevLogIndex %d\n", entries, i, prevLogIndex)
							ok := rf.sendAppendEntries(i, &args, &reply)
							// fmt.Printf("sent entries from %d to %d! Status: %t\n", rf.me, i, ok)
							if ok {
								// DO STUFF
								if reply.Success == false && reply.Term > 0 { // Failed, because the receiver had a higher term number.
									// Revert back to being a follower
									rf.mu.Lock()
									// fmt.Printf("Failed for leader %d, reply term %d, current term %d, reverting to follower\n", rf.me, reply.Term, term)
									rf.state = 2
									rf.currentTerm = reply.Term
									rf.votedFor = -1
									rf.mu.Unlock()
									return
								} else if reply.Success == false { // Failed, because receiver's log conflicts.
									// Keep trying to resend but with earlier entries
									for {
										rf.mu.Lock()
										rf.nextIndex[i] = reply.IndexOfConflictingEntry
										// fmt.Printf("NEXT INDEX AT %d UPDATED TO %d\n", i, reply.IndexOfConflictingEntry)
										newEntries := rf.logs[reply.IndexOfConflictingEntry:]
										args := AppendEntriesArgs{}
										reply := AppendEntriesReply{}

										args.Term = term
										args.LeaderID = rf.me
										args.PrevLogIndex = rf.nextIndex[i] - 1
										if rf.nextIndex[i] == 0 { // Handle case where nextIndex[i] is still 0
											args.PrevLogTerm = -1
										} else {
											args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
										}
										// args.PrevLogTerm = rf.logs[rf.nextIndex[i]].Term
										args.LeaderCommit = leaderCommit
										args.Entries = newEntries

										rf.mu.Unlock()
										ok := rf.sendAppendEntries(i, &args, &reply)
										if ok && reply.Success {
											rf.mu.Lock()
											// fmt.Printf("FINALLY SUCCESS ON %d!\n", rf.me)
											rf.nextIndex[i] = reply.NextIndex
											rf.matchIndex[i] = reply.NextIndex - 1
											rf.mu.Unlock()
											return
										}
										time.Sleep(time.Duration(10) * time.Millisecond) // Try again in 5 ms
									}
								} else if reply.Success {
									rf.mu.Lock()
									// fmt.Printf("SUCCESS ON %d!\n", rf.me)
									rf.nextIndex[i] = reply.NextIndex
									rf.matchIndex[i] = reply.NextIndex - 1
									rf.mu.Unlock()
									return
								}
							}
							// If failed to contact follower, try again
							// time.Sleep(time.Duration(10) * time.Millisecond) // Try again in 10 ms
							return
						}
					}(i, term, leaderCommit, entries, prevLogIndex, prevLogTerm)
				} else {
					rf.mu.Unlock()
				}
			}
			// Wait 100 ms for peers to add new log entries.
			deadline := time.Now().Add(time.Duration(30) * time.Millisecond)
		outerCheck:
			for time.Now().Before(deadline) {
				rf.mu.Lock()
				// Check if majority have committed for an index N
				for N := len(rf.logs) - 1; N >= 0 && N >= rf.commitIndex; N-- {
					// If a majority of matchIndex[i] > N
					numGreaterThanN := 0
					// fmt.Printf("Match indices: %v\n", rf.matchIndex)
					for i := range rf.matchIndex {
						if rf.matchIndex[i] >= N {
							numGreaterThanN += 1
						}
					}
					// fmt.Printf("num greater than N %d, N: %d\n", numGreaterThanN, N)
					// Check majority and term condition
					if numGreaterThanN >= len(rf.peers)/2 && rf.logs[N].Term == rf.currentTerm {
						rf.commitIndex = N
						// term := rf.currentTerm
						rf.mu.Unlock()
						// Send new commit index via heartbeats to all followers.
						// rf.sendAllHeartbeats(term)
						// fmt.Printf("sent out update commit index heartbeats with commit index = %d\n", N)
						break outerCheck
					}
				}
				rf.mu.Unlock()
				time.Sleep(time.Duration(5) * time.Millisecond)
			}

		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()
	rf.mu.Lock()
	// var index int
	// if rf.commitIndex == -1 {
	// 	index = 1
	// } else {
	// 	index = rf.commitIndex + 1
	// }

	// Your code here (3B).
	if isLeader {
		rf.mu.Unlock()
		rf.sendAllHeartbeats(term) // Send heartbeats to confirm leadership because this leader might have just revived and not be the actual leader.
		rf.mu.Lock()
		if rf.state == 0 {
			// fmt.Printf("START COMMAND %v RECEIVED ON %d\n", command, rf.me)
			newLogEntry := LogEntry{}
			newLogEntry.Command = command
			newLogEntry.Term = rf.currentTerm
			rf.logs = append(rf.logs, newLogEntry)
		}
	}

	index := len(rf.logs)
	rf.mu.Unlock()
	// fmt.Printf("return value from start, index: %d, term: %d, isLeader %t\n", index, term, isLeader)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applyLogEntries() {
	for rf.killed() == false {
		// Apply any log entries that are committed but yet to be applied.
		rf.mu.Lock()
		// fmt.Printf("commit index for %d: %d, lastApplied: %d, current term: %d, current logs: %v\n", rf.me, rf.commitIndex, rf.lastApplied, rf.currentTerm, rf.logs)
		if rf.commitIndex > rf.lastApplied {
			// fmt.Printf("current logs on %d: %v\n", rf.me, rf.logs)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyMsg := ApplyMsg{}
				applyMsg.Command = rf.logs[i].Command
				applyMsg.CommandIndex = i + 1
				applyMsg.CommandValid = true
				rf.applyCh <- applyMsg
				// fmt.Printf("commit index for %d: %d, lastApplied: %d, current term: %d, current logs: %v\n", rf.me, rf.commitIndex, rf.lastApplied, rf.currentTerm, rf.logs)
				// fmt.Printf("%d finished writing %v to apply channel \n", rf.me, applyMsg)
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()

		// Check for messages to apply every 100ms
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) sendAllHeartbeats(term int) {
	for i := range rf.peers {
		go func(i int) {
			if i != rf.me {
				args := AppendEntriesArgs{}
				args.LeaderID = rf.me
				args.Term = term
				args.LeaderCommit = rf.commitIndex

				var prevLogIndex int
				var prevLogTerm int
				if rf.nextIndex[i] < len(rf.logs) {
					prevLogIndex = rf.nextIndex[i] - 1
					if rf.nextIndex[i] == 0 { // Handle case where nextIndex[i] is still 0
						prevLogTerm = -1
					} else {
						prevLogTerm = rf.logs[prevLogIndex].Term
					}
				} else { // If follower has more logs than the leader that we need to remove.
					prevLogIndex = rf.nextIndex[i] - 1
					if rf.nextIndex[i] == 0 { // Handle case where nextIndex[i] is still 0
						prevLogTerm = -1
					} else {
						prevLogTerm = rf.logs[len(rf.logs)-1].Term
					}
				}

				if rf.nextIndex[i] == 0 { // Handle case where nextIndex[i] is still 0
					prevLogTerm = -1
				}

				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = prevLogTerm

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					// fmt.Printf("Heartbeat sent for leader %d of term %d to peer %d of term %d\n", rf.me, term, i, reply.Term)
					if reply.Success == false { // Heartbeat failed, because the receiver had a higher term number.
						// Revert back to being a follower
						rf.mu.Lock()
						// fmt.Printf("heartbeat failed for leader %d, reply term %d, current term %d, reverting to follower\n", rf.me, reply.Term, term)
						rf.state = 2
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.mu.Unlock()
					}
					rf.mu.Lock()
					rf.nextIndex[i] = reply.NextIndex
					rf.mu.Unlock()
				}
			}
		}(i)
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
}

func (rf *Raft) leaderLoop() {
	for rf.killed() == false {
		term, isleader := rf.GetState()
		if isleader {
			// fmt.Printf("sending leader %d heartbeats\n", rf.me)
			rf.sendAllHeartbeats(term)
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.state != 0 && rf.heartbeatReceived == false { // If not the leader and no heartbeat received yet, then start a leader election
			// fmt.Printf("%d FAILED TO RECEIVE HEARTBEAT\n", rf.me)
			rf.state = 1        // become a candidate
			rf.currentTerm += 1 // increment current term
			rf.votedFor = rf.me
			numVotes := 1 // vote for itself
			var innerMutex sync.Mutex
			term := rf.currentTerm
			lastLogIndex := len(rf.logs) - 1
			var lastLogTerm int
			if len(rf.logs) == 0 {
				lastLogTerm = -1
			} else {
				lastLogTerm = rf.logs[len(rf.logs)-1].Term
			}
			// fmt.Printf("%d's current term: %d\n", rf.me, term)
			id := rf.me
			rf.mu.Unlock()
			// Request a vote from each of the peers
			for i := range rf.peers {
				go func(i int) {
					if i != rf.me { // only ask for votes from peers that are not current candidate to avoid double counting.
						args := RequestVoteArgs{}
						args.CandidateID = id
						args.Term = term
						args.LastLogIndex = lastLogIndex
						args.LastLogTerm = lastLogTerm
						reply := RequestVoteReply{}
						// fmt.Printf("%d sending request vote to %d...\n", rf.me, i)
						ok := rf.sendRequestVote(i, &args, &reply)
						// fmt.Printf("%d done requesting vote to %d\n", rf.me, i)
						if ok && reply.VoteGranted {
							innerMutex.Lock()
							numVotes += 1
							innerMutex.Unlock()
						} else if ok && reply.VoteGranted == false && reply.Term > term {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							// fmt.Printf("reverting to follower...\n")
							rf.state = 2 // Revert to follower
							rf.votedFor = -1
							rf.mu.Unlock()
						}
					}
				}(i)
			}
			// Wait 250 ms for votes to come in.
			deadline := time.Now().Add(time.Duration(250) * time.Millisecond)
			for time.Now().Before(deadline) {
				innerMutex.Lock()
				if numVotes > len(rf.peers)/2 {
					break
				}
				innerMutex.Unlock()
				time.Sleep(time.Duration(5) * time.Millisecond)
			}
			// fmt.Printf("%d num votes %d num peers %d\n", rf.me, numVotes, len(rf.peers))

			// Check if majority and still a candidate. If so, become the leader.
			rf.mu.Lock()
			if rf.state == 1 && numVotes > len(rf.peers)/2 {
				// fmt.Printf("%d BECAME LEADER\n", rf.me)
				rf.state = 0
				// rf.votedFor = -1
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.logs)
					// fmt.Printf("VOTING DONE! NEXT INDEX AT %d UPDATED TO %d\n", i, len(rf.logs))
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
				rf.sendAllHeartbeats(term) // Send out heartbeat

				// Set default values for nextIndex and matchIndex
				// rf.nextIndex = nil
				// rf.matchIndex = nil
			} else {
				// Did not receive majority vote
				rf.state = 1     // Go back to being a candidate
				rf.votedFor = -1 // Haven't voted for anyone yet
				rf.mu.Unlock()
			}

		} else {
			rf.heartbeatReceived = false
			// rf.votedFor = -1
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 500 + (rand.Int63() % 750)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = 2 // Start as follower
	rf.heartbeatReceived = false
	rf.votedFor = -1 // Haven't voted for any peer yet.

	// logs
	rf.commitIndex = -1
	rf.lastApplied = -1

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, len(rf.logs))
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// Start other necessary goroutines.
	go rf.leaderLoop()
	go rf.sendPendingLogEntries()
	go rf.applyLogEntries()

	return rf
}
