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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

var DEBUG = true

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	LEADER_UNKNOWN = -1
	CANDIDATE      = -2
)

type JobEntry struct {
	Job  interface{}
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	votedFor     int
	gotContacted bool
	voteCount    int
	leader       int

	// logs
	log         []JobEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// This function is unsafe on purpose
func (rf *Raft) isLeader() bool {
	return rf.leader == rf.me
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.isLeader()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []JobEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	ConflictTerm int
	TermStart    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// // Figure 2 conditions 1. and 2.
	// if args.Term < rf.currentTerm || rf.lastApplied < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	if args.Term < rf.currentTerm {
	// 		rf.gotContacted = true
	// 	}
	// 	rf.debug("Follower %v refusing append entries from %v. Term is %v and received term %v\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
	// 	reply.Success = false
	// 	reply.Term = rf.currentTerm
	// 	reply.LastIndex = rf.lastApplied
	// 	return
	// }

	// Checkup
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.leader = args.LeaderId
	}

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.debug("Rejecting append entries. Reason: term is behind current term.")
		return
	} else if rf.lastApplied < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		termIndex := 0

		// for i, v := range rf.log {
		for i := len(rf.log) - 1; i > 0; i-- {
			v := rf.log[i]
			if v.Term == args.PrevLogTerm {
				termIndex = i
				break
			} else if v.Term > args.PrevLogTerm {
				// no need to continue
				break
			}
		}

		rf.debug("Rejecting append entries. Reason: Incompatible log. Term starts at %v", termIndex)
		reply.TermStart = termIndex
		reply.ConflictTerm = rf.log[termIndex].Term
		return
	}

	// has content to add
	if len(args.Entries) > 0 {
		rf.debug("Server %v adding new command...Log size was %v\n", rf.me, len(rf.log))

		// sanity check
		j := 0
		for i := args.PrevLogIndex + 1; i <= rf.commitIndex && j < len(args.Entries); i++ {
			if rf.log[i].Term != args.Entries[j].Term {

				panic(fmt.Sprintf("Server %v removing committed entry replacing by one from another term, i=%v, j=%v. Received prevLogIndex=%v\n Incoming values = %v\n Log value is %v\nDiffering values are %v and %v\n", rf.me, i, j, args.PrevLogIndex, args.Entries, rf.log[:i+1], rf.log[i].Job, args.Entries[j].Job))
			}
			j++
		}

		// remove all following entries and just put in the new ones
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		rf.lastApplied = len(rf.log) - 1

		// if args.PrevLogIndex < rf.commitIndex {
		// 	rf.debug("Server %v is removing commited entries! PrevLogIndex=%v commitIndex=%v\n", rf.me, args.PrevLogIndex, rf.commitIndex)
		// }
	}

	// get commit information
	if args.LeaderCommit > rf.commitIndex {
		rf.debug("Server %v leaderCommit=%v, commitIndex=%v\n", rf.me, args.LeaderCommit, rf.commitIndex)
		prev := rf.commitIndex + 1
		rf.commitIndex = args.LeaderCommit
		if rf.lastApplied < args.LeaderCommit {
			rf.commitIndex = rf.lastApplied

		}
		for ; prev <= rf.commitIndex; prev++ {
			rf.debug("Server %v sending information about %v, log size is %v\n", rf.me, prev, len(rf.log))
			msg := ApplyMsg{CommandValid: true, Command: rf.log[prev].Job, CommandIndex: prev}
			rf.applyCh <- msg
		}
		rf.debug("Server %v commit index is %v\n", rf.me, rf.commitIndex)
	}

	rf.votedFor = -1
	rf.currentTerm = args.Term
	rf.leader = args.LeaderId
	rf.gotContacted = true
	reply.Success = true
	reply.Term = args.Term

	// just to relax
	lastTerm := 0
	for i, v := range rf.log {
		if v.Term < lastTerm {
			panic(fmt.Sprintf("Server %v has incorrect term order at index %v. Previous term entry term was %v, new one is %v\n", rf.me, i, lastTerm, v.Term))
		}
		lastTerm = v.Term
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if args.Term < rf.currentTerm {
	// 	reply.VoteGranted = false
	// 	reply.Term = rf.currentTerm
	// } else if (rf.votedFor == -1 || rf.votedFor == CANDIDATE) && (rf.log[rf.lastApplied].Term < args.LastLogTerm || rf.log[rf.lastApplied].Term == args.LastLogTerm && args.LastLogIndex >= rf.lastApplied) {
	// 	reply.VoteGranted = true
	// 	rf.currentTerm = args.Term
	// 	rf.gotContacted = true
	// }

	// if args.Term > rf.currentTerm {
	// 	// need to update always
	// 	rf.currentTerm = args.Term
	// 	rf.leader = LEADER_UNKNOWN
	// }

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.leader = LEADER_UNKNOWN
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.debug("Rejecting vote to %v. Reason: candidate is late in terms.\n", args.CandidateId)
	} else if (rf.votedFor == -1 || rf.votedFor == CANDIDATE) && (rf.log[rf.lastApplied].Term < args.LastLogTerm || rf.log[rf.lastApplied].Term == args.LastLogTerm && args.LastLogIndex >= rf.lastApplied) {
		reply.VoteGranted = true
		// rf.currentTerm = args.Term
		rf.gotContacted = true
		rf.debug("Granting vote to %v.\n", args.CandidateId)
	} else {
		reply.VoteGranted = false
		if rf.votedFor == -1 || rf.votedFor == CANDIDATE {
			rf.debug("Rejecting vote to %v. Reason: Log is not as up to date as self.\n", args.CandidateId)
		} else {
			rf.debug("Rejecting vote to %v. Reason: Already voted for someone else.\n", args.CandidateId)
		}
	}

}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.isLeader()
	if !isLeader {
		rf.debug("Server %v rejecting new command.\n", rf.me)
		return -1, -1, false
	}

	rf.debug("Server %v Adding new command at term %v, match index is %v, content=%v\n", rf.me, rf.currentTerm, rf.matchIndex[rf.me], command)

	rf.lastApplied++
	index := rf.lastApplied
	term := rf.currentTerm

	rf.log = append(rf.log, JobEntry{Job: command, Term: term})
	rf.matchIndex[rf.me]++
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) leaderNotify(i, term, leaderId, prevLogIndex, prevLogTerm int, entries []JobEntry, leaderCommit int) {
	args := AppendEntriesArgs{Term: term, LeaderId: leaderId, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: leaderCommit}
	reply := AppendEntriesReply{}
	// rf.debug("Leader %v contacting follower %v in term %v, sending size is %v, prevLogIndex is %v\n", leaderId, i, term, len(entries), prevLogIndex)
	ok := rf.sendAppendEntries(i, &args, &reply)
	if !ok {
		// rf.debug("Leader %v did not hear from %v\n", rf.me, i)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug("Leader %v received message from follower %v in term %v with result %v\n", leaderId, i, term, reply.Success)

	if !reply.Success {
		// if it is no longer the leader
		if rf.currentTerm < reply.Term {
			rf.debug("%v was behind in terms. Updating from term %v to term %v\n", rf.me, term, reply.Term)
			rf.votedFor = -1           // random status
			rf.leader = LEADER_UNKNOWN // no longer the leader
			rf.currentTerm = reply.Term
			// rf.debug("Server %v Finished updating itself to have a new term %v\n", rf.me, rf.currentTerm)
		} else if reply.Term != 0 { // this avoids the case where the connection failed
			// just need to lower the next index to send
			// rf.nextIndex[i] = reply.LastIndex + 1
			lastConflictIndex := 1
			for i := reply.TermStart; i > 0 && rf.log[i].Term >= reply.ConflictTerm; i-- {
				lastConflictIndex = i
			}

			rf.debug("Server %v lowering next index to %v, new value %v\n", rf.me, i, lastConflictIndex)
			rf.nextIndex[i] = lastConflictIndex
		}
	} else {
		// need to update follower information
		rf.debug("Server %v to index %v matchIndex was %v, logsize is %v\n", rf.me, i, rf.matchIndex[i], len(rf.log))
		rf.matchIndex[i] = prevLogIndex + len(entries)
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		rf.debug("Server %v new matchIndex to %v is %v\n", rf.me, i, rf.matchIndex[i])

		rf.checkCommit(term)
	}
}

func (rf *Raft) candidateNotify(i, term, candidateId, lastLogIndex, lastLogTerm int) {
	args := RequestVoteArgs{Term: term, CandidateId: candidateId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	reply := RequestVoteReply{}

	// rf.debug("Candidate %v requesting vote to %v in term %v\n", candidateId, i, term)
	ok := rf.sendRequestVote(i, &args, &reply)
	if !ok {
		// rf.debug("Server %v did not receive vote in time from %v\n", rf.me, i)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug("Candidate %v received vote result from follower %v in term %v with result %v\n", candidateId, i, term, reply.VoteGranted)

	if !reply.VoteGranted && rf.currentTerm < reply.Term {
		// Candidate is late. become follower again
		rf.debug("Candidate %v is late and will now become a follower again\n", rf.me)
		rf.votedFor = -1 // default case because we don't know who's the leader yet
		rf.leader = LEADER_UNKNOWN
		rf.currentTerm = reply.Term
		rf.gotContacted = true // reset timer

	} else if reply.VoteGranted && rf.votedFor != -1 {
		rf.voteCount += 1
		if rf.voteCount > len(rf.peers)/2 {
			// majority, can become leader
			rf.votedFor = -1
			rf.voteCount = -1
			rf.leader = rf.me

			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
				rf.nextIndex[i] = rf.lastApplied + 1
				rf.debug("Server %v next index to %v is %v\n", rf.me, i, rf.nextIndex[i])
			}

			rf.matchIndex[rf.me] = rf.lastApplied // self is updated

			rf.debug("THE NEW LEADER IS %v !!!!!!!!!!!!!!!!!!!!\n", rf.me)
			rf.broadcast()
		}
	}
}

func (rf *Raft) broadcast() {
	// send heartbeat to all followers
	var nilSlice []JobEntry // nil slice for heartbeat

	for i := range rf.peers {
		if i != rf.me {
			var sendingSlice []JobEntry
			prevLogIndex := rf.nextIndex[i] - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			rf.debug("Server %v, %v vs %v\n", i, rf.nextIndex[i], rf.lastApplied)
			if rf.nextIndex[i] > rf.lastApplied {
				// empty heartbeat
				sendingSlice = nilSlice
			} else {
				// sendingSlice = rf.log[prevLogIndex+1:]
				sendingSlice = make([]JobEntry, len(rf.log[prevLogIndex+1:]))
				copy(sendingSlice, rf.log[prevLogIndex+1:])
			}
			go rf.leaderNotify(i, rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, sendingSlice, rf.commitIndex)
		}
	}
}

func (rf *Raft) checkCommit(term int) {
	// check if any message can be considered committed

	count := make([]int, len(rf.log))

	for _, v := range rf.matchIndex {
		//rf.debug("MatchIndex index %v has value %v\n", i, v)
		count[v]++
	}

	// a value ahead means that it is counted before so we can do a reverse sum here
	//rf.debug("Index %v count is %v, content=%v, term=%v\n", len(rf.log)-1, count[len(rf.log)-1], rf.log[len(rf.log)-1].Job, rf.log[len(rf.log)-1].Term)
	for i := len(rf.log) - 2; i >= 0; i-- {
		count[i] += count[i+1]
		//rf.debug("Index %v count is %v, content=%v, term=%v\n", i, count[i], rf.log[i].Job, rf.log[i].Term)
	}

	// for i, v := range count {
	// 	if i > rf.commitIndex && v > len(rf.peers)/2 && rf.log[i].Term == term {
	// 		rf.commitIndex = i // new commit index, increases monotically
	// 		//rf.debug("Server %v sending information about %v, content=%v\n", rf.me, i, rf.log[i].Job)
	// 		msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Job, CommandIndex: i}
	// 		rf.applyCh <- msg
	// 		//rf.debug("New commit index %v\n", i)
	// 	}
	// }

	newCommit := -1
	for i := len(count) - 1; i > rf.commitIndex; i-- {
		v := count[i]
		if i > rf.commitIndex && v > len(rf.peers)/2 && rf.log[i].Term == term {
			newCommit = i
			break
		}
	}

	if newCommit != -1 {
		for i := rf.commitIndex + 1; i <= newCommit; i++ {
			rf.debug("Server %v sending information about %v, content=%v\n", rf.me, i, rf.log[i].Job)
			msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Job, CommandIndex: i}
			rf.applyCh <- msg
		}
		rf.commitIndex = newCommit
		rf.debug("New commit index %v\n", newCommit)
	}

}

func (rf *Raft) debugCommit() {
	count := make([]int, len(rf.log)+1)

	for i, v := range rf.matchIndex {
		rf.debug("MatchIndex index %v has value %v\n", i, v)
		count[v]++
	}

	// a value ahead means that it is counted before so we can do a reverse sum here
	rf.debug("Index %v count is %v, content=%v, term=%v\n", len(rf.log)-1, count[len(rf.log)-1], rf.log[len(rf.log)-1].Job, rf.log[len(rf.log)-1].Term)
	for i := len(rf.log) - 2; i >= 0; i-- {
		count[i] += count[i+1]
		rf.debug("Index %v count is %v, content=%v, term=%v\n", i, count[i], rf.log[i].Job, rf.log[i].Term)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	time.Sleep(time.Duration(1000000 * rand.Intn(400)))

	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		var sleepTime time.Duration
		isLeader := rf.isLeader()
		if isLeader {
			// rf.checkCommit()
			// rf.debugCommit()
			rf.broadcast()                     // heartbeat all followers
			sleepTime = time.Millisecond * 100 // fixed time, 10 times per second max
		} else {
			if !rf.gotContacted {
				// start election
				rf.currentTerm += 1
				rf.votedFor = CANDIDATE
				rf.voteCount = 1 // vote for itself

				rf.debug("Follower %v failed to be contacted, initiating election in term %v\n", rf.me, rf.currentTerm)
				// request vote from all others
				for i := range rf.peers {
					if i != rf.me {
						go rf.candidateNotify(i, rf.currentTerm, rf.me, rf.lastApplied, rf.log[rf.lastApplied].Term)
					}
				}
				sleepTime = time.Duration(1000000 * (100 + rand.Intn(200))) // 100-300ms
			} else {
				rf.debug("Follower %v got contacted and will sleep again...\n", rf.me)
				sleepTime = time.Duration(1000000 * (600 + rand.Intn(350))) // 600-950ms
			}
			rf.gotContacted = false
		}

		rf.mu.Unlock()

		time.Sleep(sleepTime)
	}
}

func (rf *Raft) debug(format string, content ...interface{}) {
	if DEBUG {
		state := "F"
		if rf.isLeader() {
			state = "L"
		} else if rf.votedFor == CANDIDATE {
			state = "C"
		}
		prefix := fmt.Sprintf("[%d:%s:%02d] ", rf.me, state, rf.currentTerm)
		log.Printf(prefix+format, content...)

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = -1
	rf.gotContacted = false
	rf.leader = LEADER_UNKNOWN

	rf.log = make([]JobEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// FIXME
	// for i := 0; i < len(peers); i++ {
	// 	rf.nextIndex[i] = 0
	// 	rf.matchIndex[i] = 0
	// }

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
