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
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

var DEBUG = false

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

// votedFor
const (
	LEADER_UNKNOWN = -1
)

// state
const (
	LEADER    = iota
	CANDIDATE = iota
	FOLLOWER  = iota
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
	voteCount    []bool
	state        int32

	// logs
	log         []JobEntry
	commitIndex int
	nextIndex   []int
	matchIndex  []int
	lastApplied int

	// snapshot
	lastIncludedTerm  int
	lastIncludedIndex int
	snapshot          []byte

	// avoid excessive messages
	canSend bool
}

func (rf *Raft) IsLeader() bool {
	z := atomic.LoadInt32(&rf.state)
	return z == LEADER
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
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
	isleader = rf.IsLeader()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(snapshot bool) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()

	if snapshot {
		rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	} else {
		rf.persister.SaveRaftState(data)
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []JobEntry
	var votedFor int
	var currentTerm int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&log) != nil || d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		panic("Error while reading persist")
	} else {
		rf.log = log
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}

	rf.debug("Finished read persist. Log size is now %v, last entry term is %v, lastIncludedIndex=%v, lastIncludedTerm=%v\n", rf.lastEntryIndex(), rf.index(rf.lastEntryIndex()).Term, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// change occurred in the meantime
	if lastIncludedIndex < rf.commitIndex {
		rf.debug("Refusing snapshot. Reason: change occurred in the meantime: %v %v\n", lastIncludedIndex, rf.commitIndex)
		return false
	}

	// whatever
	rf.log = make([]JobEntry, 0)

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapshot = snapshot
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// persist snapshot
	rf.persist(true)

	rf.debug("Installed snapshot. commitIndex=%v, lastIncludedIndex=%v, logsize=%v\n", rf.commitIndex, rf.lastIncludedIndex, rf.lastEntryIndex())

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// late snapshot
	if index <= rf.lastIncludedIndex {
		rf.debug("Index too small to create snapshot")
	}

	// trim slice and reduce size
	trimIndex := index - rf.lastIncludedIndex
	if rf.lastIncludedIndex > 0 {
		trimIndex -= 1
	}
	rf.log = append([]JobEntry(nil), rf.log[trimIndex+1:]...)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.index(rf.lastEntryIndex()).Term
	rf.snapshot = snapshot

	rf.debug("Updated to snapshot. lastIncludedIndex=%v, logsize=%v\n", rf.lastIncludedIndex, len(rf.log))

	// persist snapshot
	rf.persist(true)
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

	// Checkup
	if args.Term > rf.currentTerm {
		rf.debug("Updating to new term %v\n", args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = LEADER_UNKNOWN
		atomic.StoreInt32(&rf.state, FOLLOWER)
	}

	reply.Success = false
	reply.Term = rf.currentTerm

	// late leader
	if args.Term < rf.currentTerm {
		rf.debug("Rejecting append entries. Reason: term is behind current term.")
		return
	}

	conflictTerm := -1

	if rf.lastEntryIndex() < args.PrevLogIndex || args.PrevLogIndex < rf.lastIncludedIndex {
		// log not long enough
		conflictTerm = rf.index(rf.lastEntryIndex()).Term
	} else if rf.index(args.PrevLogIndex).Term != args.PrevLogTerm {
		// unmatched PrevLogTerm
		conflictTerm = rf.index(args.PrevLogIndex).Term
	}

	// find conflicting term starting point and return
	if conflictTerm != -1 {
		termIndex := 1 // default
		for i := rf.lastIncludedIndex + 1; i <= rf.lastEntryIndex(); i++ {
			if rf.index(i).Term == conflictTerm {
				termIndex = i
			}
		}

		reply.ConflictTerm = conflictTerm
		reply.TermStart = termIndex
		rf.debug("Rejecting append entries. Reason: Incompatible log. Term %v starts at %v. Received PrevLogIndex=%v, last entry index is %v\n", conflictTerm, termIndex, args.PrevLogIndex, rf.lastEntryIndex())
		return
	}

	// has content to add
	if len(args.Entries) > 0 {
		rf.debug("Adding new commands, logsize=%v startIndex=%v, values=%v\n", rf.lastEntryIndex(), args.PrevLogIndex+1, args.Entries)

		// sanity check
		for i, j := args.PrevLogIndex+1, 0; i <= rf.commitIndex && j < len(args.Entries); i, j = i+1, j+1 {
			if rf.index(i).Term != args.Entries[j].Term {

				panic(fmt.Sprintf("Server %v removing committed entry replacing by one from another term, i=%v, j=%v. Received prevLogIndex=%v\n Incoming values = %v\n Log value is %v\nDiffering values are %v and %v\nReceived Arguments are %v\n", rf.me, i, j, args.PrevLogIndex, args.Entries, rf.log[:i-rf.lastIncludedIndex+1], rf.index(i).Job, args.Entries[j].Job, args))
			}
		}

		// remove all following entries and just put in the new ones
		limit := args.PrevLogIndex - rf.lastIncludedIndex
		if rf.lastIncludedIndex > 0 {
			limit -= 1
		}

		prevSize := rf.lastEntryIndex()
		finished := false
		// rf.log = append(rf.log[:limit+1], args.Entries...)
		var i, j int
		for i, j = limit+1, 0; i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
			if rf.log[i].Term != args.Entries[j].Term {
				rf.log = append(rf.log[:i], args.Entries[j:]...)
				finished = true
				break
			}
		}

		if !finished && j < len(args.Entries) {
			rf.debug("Adding missing entries starting at index %v, prevLog = %v, entries = %v, startIndex=%v\n", j, len(rf.log), len(args.Entries), limit)
			rf.log = append(rf.log, args.Entries[j:]...)
		} else if !finished && i < len(rf.log) {
			rf.debug("Avoided network delay!\n")
		}

		if rf.lastEntryIndex() < rf.commitIndex {
			panic(fmt.Sprintf("Server %v after log append, last entry is smaller than commit index (%v, %v). PrevLogIndex=%v, PrevLogTerm=%v, limit=%v, prevLastEntryIndex=%v, entries=%v\n", rf.me, rf.lastEntryIndex(), rf.commitIndex, args.PrevLogIndex, args.PrevLogTerm, limit, prevSize, args.Entries))
		}
	}

	// get commit information
	if args.LeaderCommit > rf.commitIndex {
		rf.debug("Received new commit index=%v. Prev=%v\n", args.LeaderCommit, rf.commitIndex)
		newIndex := args.LeaderCommit

		// in case this server is behind the committed value
		if rf.lastEntryIndex() < args.LeaderCommit {
			newIndex = rf.lastEntryIndex()
		}

		rf.updateCommit(newIndex)
	}

	// rf.votedFor = LEADER_UNKNOWN
	rf.currentTerm = args.Term
	atomic.StoreInt32(&rf.state, FOLLOWER)
	rf.gotContacted = true
	reply.Success = true
	reply.Term = args.Term

	// just to relax
	lastTerm := 0
	for i, v := range rf.log {
		if v.Term < lastTerm {
			panic(fmt.Sprintf("Server %v has incorrect term order at index %v. Previous term entry term was %v, new one is %v\nLog: %v\n", rf.me, i, lastTerm, v.Term, rf.log))
		}
		lastTerm = v.Term
	}

	// save state
	rf.persist(false)
}

func (rf *Raft) index(i int) JobEntry {
	// in some cases, it will need to check the first entry in the snapshot, which isn't reachable
	if i == rf.lastIncludedIndex {
		rf.debug("Generating last snapshot entry...\n")
		return JobEntry{Term: rf.lastIncludedTerm, Job: nil}
	}

	index := i - rf.lastIncludedIndex
	if rf.lastIncludedIndex > 0 {
		index -= 1
	}

	if index < 0 || index >= len(rf.log) {
		rf.debug("Accessing invalid index %v, logsize is %v, lastIncludedIndex is %v, commitIndex is %v, obtained index is %v, last entry is %v\n", index, len(rf.log), rf.lastIncludedIndex, rf.commitIndex, i, rf.lastEntryIndex())
		panic("Index error")
	}

	// rf.debug("True index %v will return index %v. lastIncludedIndex=%v\n", i, index, rf.lastIncludedIndex)
	return rf.log[index]
}

func (rf *Raft) lastEntryIndex() int {
	if rf.lastIncludedIndex > 0 {
		// rf.debug("Last entry with lastIncludedIndex=%v and logsize=%v has index %v\n", rf.lastIncludedIndex, len(rf.log), rf.lastIncludedIndex+len(rf.log))
		return rf.lastIncludedIndex + len(rf.log)
	}
	// rf.debug("Last entry with lastIncludedIndex=%v and logsize=%v has index %v\n", rf.lastIncludedIndex, len(rf.log), rf.lastIncludedIndex+len(rf.log)-1)
	return rf.lastIncludedIndex + len(rf.log) - 1
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

	if rf.currentTerm < args.Term {
		rf.debug("Updating term to new term %v\n", args.Term)
		rf.currentTerm = args.Term
		atomic.StoreInt32(&rf.state, FOLLOWER)
		rf.votedFor = LEADER_UNKNOWN
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// late candidates
	if args.Term < rf.currentTerm {
		rf.debug("Rejecting candidate %v. Reason: late term=%v\n", args.CandidateId, args.Term)
		return
	}

	// avoid double vote
	if rf.votedFor != LEADER_UNKNOWN && rf.votedFor != args.CandidateId {
		rf.debug("Rejecting candidate %v. Reason: already voted\n", args.CandidateId)
		return
	}

	lastLogIndex := rf.lastEntryIndex()

	// reject old logs
	if rf.index(lastLogIndex).Term > args.LastLogTerm {
		rf.debug("Rejecting candidate %v. Reason: old log\n", args.CandidateId)
		return
	}

	// log is smaller
	if rf.index(lastLogIndex).Term == args.LastLogTerm && args.LastLogIndex < lastLogIndex {
		rf.debug("Rejecting candidate %v. Reason: small log\n", args.CandidateId)
		return
	}

	rf.votedFor = args.CandidateId
	rf.gotContacted = true

	rf.debug("Granting vote to %v. me=(%v,%v), candidate=(%v,%v)\n", args.CandidateId, lastLogIndex, rf.index(lastLogIndex).Term, args.LastLogIndex, args.LastLogTerm)
	reply.VoteGranted = true

	// save state
	rf.persist(false)
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

	isLeader := rf.IsLeader()
	if !isLeader {
		rf.debug("Rejecting new command.\n")
		return -1, -1, false
	}

	index := rf.lastEntryIndex() + 1
	term := rf.currentTerm

	rf.log = append(rf.log, JobEntry{Job: command, Term: term})
	rf.matchIndex[rf.me]++ // This is needed for the commit check

	rf.debug("Adding new command at term %v, at index  %v, content=%v\n", rf.currentTerm, index, command)

	// save state
	rf.persist(false)
	rf.canSend = true

	rf.broadcast()

	return index, term, isLeader
}

//
// the tester doesn't halt froutines created by Raft after each test,
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

	ok := rf.sendAppendEntries(i, &args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Checkup
	if rf.currentTerm < reply.Term {
		rf.debug("Updating term to new term %v\n", args.Term)
		rf.currentTerm = reply.Term
		atomic.StoreInt32(&rf.state, FOLLOWER)
		rf.votedFor = LEADER_UNKNOWN
		rf.gotContacted = true
		return
	}

	// stepped down in between
	if !rf.IsLeader() {
		rf.debug("Received reply after stepping down. Skipping...")
		return
	}

	// avoid unreliable communication late messages
	if args.Term != rf.currentTerm {
		rf.debug("Received late append reply message. Skipping...")
		return
	}

	if reply.Success {
		rf.matchIndex[i] = prevLogIndex + len(entries)
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		rf.debug("Successful append entries to %v, next=%v\n", i, rf.nextIndex[i])

		rf.checkCommit()
	} else {
		// replace wrong term
		newNextIndex := reply.TermStart

		// if the leader doesn't have such term start from the beginning
		if rf.lastEntryIndex() < reply.TermStart || reply.TermStart < rf.lastEntryIndex() || rf.index(reply.TermStart).Term != reply.ConflictTerm {
			rf.debug("Couldn't find term obtained from reply from %v.", i)
			newNextIndex = 1
		}

		if newNextIndex == 0 {
			rf.debug("[WARNING] Received newNextIndex 0. Defaulting to 1 for correctness")
			newNextIndex = 1
		}

		rf.debug("Lowering next index to %v, new value %v.\n", i, newNextIndex)
		rf.nextIndex[i] = newNextIndex
	}
}

func (rf *Raft) candidateNotify(i, term, candidateId, lastLogIndex, lastLogTerm int) {
	args := RequestVoteArgs{Term: term, CandidateId: candidateId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	reply := RequestVoteReply{}

	ok := rf.sendRequestVote(i, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rf.debug("Received vote from %v. Content: %v\n", i, reply.VoteGranted)

	// update if late
	if rf.currentTerm < reply.Term {
		rf.debug("Updating term to new term %v\n", args.Term)
		rf.currentTerm = reply.Term
		atomic.StoreInt32(&rf.state, FOLLOWER)
		rf.votedFor = LEADER_UNKNOWN
		rf.gotContacted = true
		return
	}

	// election has already finished
	if rf.IsLeader() {
		rf.debug("Received vote after stepping up. Skipping...")
		return
	}

	// avoid unreliable communication late messages
	if args.Term != rf.currentTerm {
		rf.debug("Received late vote message. Skipping...")
		return
	}

	// avoid stepping up after voting
	if rf.votedFor != rf.me {
		rf.debug("Already voted for somebody else in this term. Skipping...")
		return
	}

	if reply.VoteGranted {
		rf.debug("Received vote from %v.\n", i)
		rf.voteCount[i] = true

		// count num of votes until now
		count := 0
		for _, v := range rf.voteCount {
			if v {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			atomic.StoreInt32(&rf.state, LEADER)

			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
				rf.nextIndex[i] = rf.lastEntryIndex() + 1
			}
			rf.matchIndex[rf.me] = rf.lastEntryIndex()

			rf.debug("Stepped up as leader. LastLogIndex=%v, LastLogTerm=%v\n", rf.lastEntryIndex(), rf.index(rf.lastEntryIndex()).Term)

			rf.broadcast()
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.debug("Rejecting snapshot. Reason: Term is behind.\n")
		return
	}

	// Checkup
	if args.Term > rf.currentTerm {
		rf.debug("Updating to new term %v\n", args.Term)
		atomic.StoreInt32(&rf.state, FOLLOWER)
		rf.votedFor = LEADER_UNKNOWN
		rf.currentTerm = args.Term
		rf.gotContacted = true
	}

	// Sanity check (too many election problems already!)
	if rf.IsLeader() {
		rf.debug("[ERROR] InstallSnapshot sent to leader?! Origin: %v\n", args.LeaderId)
		return
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.debug("Outdated InstallSnapshot. args=%v, rf=%v\n", args.LastIncludedIndex, rf.lastIncludedIndex)
		return
	}

	rf.gotContacted = true
	msg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: args.LastIncludedTerm}
	rf.debug("Applying snapshot received from %v\n", args.LeaderId)
	go func() { rf.applyCh <- msg }() // TODO: should this be blocking or should it also be parallel?
}

func (rf *Raft) sendSnapshot(i, term, leaderId, lastIncludedIndex, lastIncludedTerm int, snapshot []byte) {
	args := InstallSnapshotArgs{Term: term, LeaderId: leaderId, LastIncludedIndex: lastIncludedIndex, LastIncludedTerm: lastIncludedTerm, Data: snapshot}
	reply := InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(i, &args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// update if late
	if rf.currentTerm < reply.Term {
		rf.debug("Updating term to new term %v\n", args.Term)
		rf.currentTerm = reply.Term
		atomic.StoreInt32(&rf.state, FOLLOWER)
		rf.votedFor = LEADER_UNKNOWN
		rf.gotContacted = true
		return
	}

	// unreliable message
	if !rf.IsLeader() || args.Term != rf.currentTerm {
		rf.debug("Received late snapshot reply. Skipping...")
		return
	}

	// update next index to send
	rf.nextIndex[i] = args.LastIncludedIndex + 1
}

// accumulates messages to send faster than heartbeat for performance in lab 3
func (rf *Raft) send() {

	for !rf.killed() {

		rf.mu.Lock()
		if rf.IsLeader() && rf.canSend {
			rf.canSend = false
			rf.broadcast()
		}
		rf.mu.Unlock()

		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) broadcast() {
	// send the log or heartbeat to each follower
	if !rf.IsLeader() {
		panic(fmt.Sprintf("Server %v sending messages when it shouldn't. canSend is %v\n", rf.me, rf.canSend))
	}
	for i := range rf.peers {
		if i != rf.me {

			// if server has lagged behind send a snapshot
			if rf.nextIndex[i]-1 < rf.lastIncludedIndex {
				rf.debug("Sending snapshot to follower %v. NextIndex=%v, lastIndex=%v\n", i, rf.nextIndex[i], rf.lastIncludedIndex)

				go rf.sendSnapshot(i, rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.snapshot)
			} else {
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := rf.index(prevLogIndex).Term

				index := prevLogIndex - rf.lastIncludedIndex
				if rf.lastIncludedIndex > 0 {
					index -= 1
				}
				sendingSlice := append([]JobEntry(nil), rf.log[index+1:]...)

				rf.debug("Sending append entries to %v. Size: %v\n", i, len(sendingSlice))
				go rf.leaderNotify(i, rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, sendingSlice, rf.commitIndex)
			}
		}
	}
}

// update new commit index safely
func (rf *Raft) updateCommit(newCommitIndex int) {

	if newCommitIndex < rf.commitIndex {
		panic(fmt.Sprintf("Server %v: new commit index %v is lower than previous one %v\n", rf.me, newCommitIndex, rf.commitIndex))
	}

	rf.commitIndex = newCommitIndex
	rf.debug("New commit index: %v\n", rf.commitIndex)

	if rf.commitIndex > rf.lastEntryIndex() {
		panic(fmt.Sprintf("Server %v: new commit index is bigger than log size (%v, %v)\n", rf.me, rf.commitIndex, rf.lastEntryIndex()))
	}
}

// will try to apply the log messages at every interval but dropping the lock when full
func (rf *Raft) apply() {
	for !rf.killed() {
		time.Sleep(time.Duration(time.Millisecond * 10)) //50ms
		rf.mu.Lock()
		keepApplying := true
		for keepApplying && rf.lastApplied < rf.commitIndex {
			msg := ApplyMsg{CommandValid: true, Command: rf.index(rf.lastApplied + 1).Job, CommandIndex: rf.lastApplied + 1}

			select {
			case rf.applyCh <- msg:
				// message has been sent
				rf.debug("Sent information about %v, content=%v, commitIndex=%v\n", rf.lastApplied+1, rf.index(rf.lastApplied+1).Job, rf.commitIndex)
				rf.lastApplied++
			default:
				// drop message
				keepApplying = false
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) checkCommit() {
	// check if any message can be considered committed
	if !rf.IsLeader() {
		return
	}

	count := make([]int, len(rf.log))

	// increase the values of each match
	for _, v := range rf.matchIndex {
		if v <= rf.lastIncludedIndex {
			continue
		}

		index := v - rf.lastIncludedIndex
		if rf.lastIncludedIndex > 0 {
			index -= 1
		}

		if index < 0 || index >= len(count) {
			rf.debug("lastIncludedIndex=%v logsize=%v v=%v\n", rf.lastIncludedIndex, len(rf.log), v)
		}
		count[index]++
	}

	// a value ahead means that it is counted before so we can do a reverse sum here
	for i := len(rf.log) - 2; i >= 0; i-- {
		count[i] += count[i+1]
	}

	newCommit := -1
	minIndex := rf.commitIndex - rf.lastIncludedIndex
	if rf.lastIncludedIndex > 0 {
		minIndex -= 1
	}
	for j := len(count) - 1; j > minIndex; j-- {
		i := rf.lastIncludedIndex + j
		if rf.lastIncludedIndex > 0 {
			i += 1
		}
		v := count[j]
		// rf.debug("Checking i=%v, lastIncludedIndex=%v\n", i, rf.lastIncludedIndex)
		if i > rf.commitIndex && v > len(rf.peers)/2 && rf.index(i).Term == rf.currentTerm {
			newCommit = i
			break
		}
	}

	if newCommit != -1 {
		rf.updateCommit(newCommit)
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	time.Sleep(time.Duration(1000000 * rand.Intn(150)))

	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		var sleepTime time.Duration
		isLeader := rf.IsLeader()
		if isLeader {
			// rf.checkCommit()
			// rf.debugCommit()
			rf.canSend = false
			rf.broadcast()                     // heartbeat all followers
			sleepTime = time.Millisecond * 100 // fixed time, 10 times per second max
		} else {
			if !rf.gotContacted {
				// start election
				rf.currentTerm += 1
				rf.votedFor = rf.me
				atomic.StoreInt32(&rf.state, CANDIDATE)
				// rf.state = CANDIDATE

				// reset vote count
				for i := range rf.voteCount {
					rf.voteCount[i] = false
				}
				rf.voteCount[rf.me] = true // vote for itself

				rf.debug("Failed to be contacted, initiating election in term %v\n", rf.currentTerm)

				// request vote from all others
				for i := range rf.peers {
					if i != rf.me {
						go rf.candidateNotify(i, rf.currentTerm, rf.me, rf.lastEntryIndex(), rf.index(rf.lastEntryIndex()).Term)
					}
				}
				sleepTime = time.Duration(1000000 * (100 + rand.Intn(200))) // 100-300ms
			} else {
				rf.debug("Got contacted and will sleep again...\n")
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
		var state string
		if rf.state == FOLLOWER {
			state = "F"
		} else if rf.state == LEADER {
			state = "L"
		} else {
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
	rf.votedFor = LEADER_UNKNOWN

	rf.voteCount = make([]bool, len(peers))
	for i := range rf.voteCount {
		rf.voteCount[i] = false
	}

	rf.gotContacted = false
	atomic.StoreInt32(&rf.state, FOLLOWER)

	rf.log = make([]JobEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.lastIncludedTerm = 0
	rf.lastIncludedIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.canSend = false

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()
	go rf.send()

	return rf
}
