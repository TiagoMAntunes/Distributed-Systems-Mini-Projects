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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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
	SELF_LEADER = -1
	CANDIDATE   = -2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	votedFor     int // points to the currentTerm leader of this node. -1 is None
	gotContacted bool
	voteCount    int
}

// This function is unsafe on purpose
func (rf *Raft) isLeader() bool {
	if rf.votedFor == SELF_LEADER {
		return true
	} else {
		return false
	}
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

	//TODO logs
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		fmt.Printf("[%v] Follower %v refusing append entries from %v. Term is %v and received term %v\n", makeTimestamp(), rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		rf.gotContacted = true
		reply.Success = true

		// TODO logs

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

	// TODO logs info
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

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if (args.Term > rf.currentTerm) && (true /*FIXME log*/) {
		reply.VoteGranted = true
		rf.currentTerm = args.Term
	}

	fmt.Printf("[%v] Server %v received request vote from %v, result is %v, server term is %v\n", makeTimestamp(), rf.me, args.CandidateId, reply.VoteGranted, rf.currentTerm)
	rf.gotContacted = true
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) leaderNotify(i, term, leaderId int) {
	args := AppendEntriesArgs{Term: term, LeaderId: leaderId}
	reply := AppendEntriesReply{}
	fmt.Printf("[%v] Leader %v contacting follower %v in term %v\n", makeTimestamp(), leaderId, i, term)
	rf.sendAppendEntries(i, &args, &reply)
	fmt.Printf("[%v] Leader %v received message from follower %v in term %v with result %v\n", makeTimestamp(), leaderId, i, term, reply.Success)

	if !reply.Success {
		// probably an election happened

		rf.mu.Lock()
		defer rf.mu.Unlock()
		// if hasn't received a vote call yet
		if rf.currentTerm < reply.Term {
			fmt.Printf("[%v] %v was behind in terms. Updating from term %v to term %v\n", makeTimestamp(), rf.me, term, reply.Term)
			rf.votedFor = rf.me // random status
		}
	}
}

func (rf *Raft) candidateNotify(i, term, candidateId int) {
	args := RequestVoteArgs{Term: term, CandidateId: candidateId}
	reply := RequestVoteReply{}

	fmt.Printf("[%v] Candidate %v requesting vote to %v in term %v\n", makeTimestamp(), candidateId, i, term)
	rf.sendRequestVote(i, &args, &reply)
	fmt.Printf("[%v] Candidate %v received vote result from follower %v in term %v\n", makeTimestamp(), candidateId, i, term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !reply.VoteGranted && rf.currentTerm < reply.Term {
		// Candidate is late. become follower again
		fmt.Printf("[%v] Candidate %v is late and will now become a follower again\n", makeTimestamp(), rf.me)
		rf.votedFor = rf.me // default case because we don't know who's the leader yet
	} else if reply.VoteGranted && rf.votedFor != SELF_LEADER {
		rf.voteCount += 1
		if rf.voteCount > len(rf.peers)/2 {
			// majority, can become leader
			rf.votedFor = SELF_LEADER
			fmt.Printf("[%v] THE NEW LEADER IS %v !!!!!!!!!!!!!!!!!!!!\n", makeTimestamp(), rf.me)
			rf.broadcast()
		}
	}
}

func (rf *Raft) broadcast() {
	// send heartbeat to all followers
	for i := range rf.peers {
		if i != rf.me {
			go rf.leaderNotify(i, rf.currentTerm, rf.me)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	time.Sleep(time.Duration(1000000 * rand.Intn(400)))

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		var sleepTime time.Duration
		isLeader := rf.isLeader()
		if isLeader {
			rf.broadcast()                     // heartbeat all followers
			sleepTime = time.Millisecond * 100 // fixed time, 10 times per second max
		} else {
			if !rf.gotContacted {
				// start election
				rf.currentTerm += 1
				rf.votedFor = CANDIDATE

				fmt.Printf("[%v] Follower %v failed to be contacted, initiating election in term %v\n", makeTimestamp(), rf.me, rf.currentTerm)
				// request vote from all others
				for i := range rf.peers {
					if i != rf.me {
						go rf.candidateNotify(i, rf.currentTerm, rf.me)
					}
				}
				sleepTime = time.Duration(1000000 * (100 + rand.Intn(200))) // 100-300ms
			} else {
				fmt.Printf("[%v] Follower %v got contacted and will sleep again...\n", makeTimestamp(), rf.me)
				sleepTime = time.Duration(1000000 * (600 + rand.Intn(350))) // 600-950ms
			}
			rf.gotContacted = false
		}

		rf.mu.Unlock()

		time.Sleep(sleepTime)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = me // initially is a follower but we don't know the leader yet
	rf.gotContacted = false
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
