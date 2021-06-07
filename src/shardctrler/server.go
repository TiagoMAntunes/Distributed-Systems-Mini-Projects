package shardctrler

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int

	data        map[string]string
	results     map[int64]chan Op // because the channels did not exist, messages were never being applied. analyse this<
	clientIndex map[int64]int64   // stores sequential values for each client

	lastIndex int

	configs []Config // indexed by config num
}

const Debug = true

func (sc *ShardCtrler) debug(format string, a ...interface{}) (n int, err error) {
	if Debug {
		state := "L"
		if !sc.rf.IsLeader() {
			state = "F"
		}
		prefix := fmt.Sprintf("[%v:%v] ", sc.me, state)
		log.Printf(prefix+format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
	Type      string

	//join args
	Servers map[int][]string

	//leave args
	GIDs []int

	// move args
	Shard int
	GID   int

	// query args
	Num int

	// query reply
	Config Config
}

const ErrRepeatedRequest = "ErrRepeatedRequest" // this one is a mere help for validateRequest

func (sc *ShardCtrler) validateRequest(op Op) (Err, Op) {
	isLeader := sc.rf.IsLeader()
	if isLeader {
		sc.mu.Lock()
		sc.checkNewClient(op.ClientId)

		// check if message was already handled
		v, ok := sc.clientIndex[op.ClientId]
		sc.mu.Unlock()

		if ok && op.ClientId <= v {
			sc.debug("Repeated request, op=%v\n", op)
			return ErrRepeatedRequest, Op{}

		} else {
			status, res := sc.doOp(op)
			if status != OK || res.RequestId < op.RequestId {
				return ErrNotCommitted, Op{} // late reply?
			} else {
				return OK, res
			}
		}
	} else {
		return ErrWrongLeader, Op{}
	}

}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.debug("Join started, args:=%v", args)

	req := Op{Type: "Join", ClientId: args.ClientId, RequestId: args.RequestId, Servers: args.Servers}

	status, _ := sc.validateRequest(req)
	if status == ErrRepeatedRequest || status == OK {
		// TODO handle repeated
		reply.Err = OK
	} else {
		reply.Err = status
	}
	sc.debug("Join ended, args:=%v, reply:=%v", args, reply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.debug("Leave started, args:=%v", args)

	req := Op{Type: "Leave", ClientId: args.ClientId, RequestId: args.RequestId, GIDs: args.GIDs}

	status, _ := sc.validateRequest(req)

	if status == ErrRepeatedRequest || status == OK {
		reply.Err = OK
	} else {
		reply.Err = status
	}

	sc.debug("Leave ended, args:=%v, reply:=%v", args, reply)

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.debug("Move started, args:=%v", args)

	req := Op{Type: "Move", ClientId: args.ClientId, RequestId: args.RequestId, Shard: args.Shard, GID: args.GID}

	status, _ := sc.validateRequest(req)

	if status == ErrRepeatedRequest || status == OK {
		reply.Err = OK
	} else {
		reply.Err = status
	}

	sc.debug("Move ended, args:=%v, reply:=%v", args, reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.debug("Query started, args:=%v", args)

	req := Op{Type: "Query", ClientId: args.ClientId, RequestId: args.ClientId, Num: args.Num}

	status, result := sc.validateRequest(req)

	if status == ErrRepeatedRequest {
		reply.Err = OK
		// TODO
	} else if status == OK {
		reply.Err = OK
		reply.Config = result.Config
	} else {
		reply.Err = status
	}

	sc.debug("Query ended, args:=%v, reply:=%v", args, reply)

}

// Initiates a command and waits for it to finish
func (sc *ShardCtrler) doOp(op Op) (Err, Op) {
	sc.rf.Start(op) // value goes as null for check after

	sc.mu.Lock()
	readCh := sc.results[op.ClientId]
	sc.mu.Unlock()

	// get the response

	select {
	case response := <-readCh:
		return OK, response
	case <-time.After(2 * time.Second):
		sc.debug("Command %v did not commit in time.\n", op)
		return ErrNotCommitted, Op{}
	}

}

func (sc *ShardCtrler) applyToStateMachine(op Op) Op {

	// update data for writes, avoid writing old data
	if sc.clientIndex[op.ClientId] < op.RequestId {
		switch op.Type {
		//TODO
		default:
			panic(fmt.Sprintf("Unrecognized command %v\n", op))
		}

		sc.clientIndex[op.ClientId] = op.RequestId // update last update info
		sc.debug("New id to client %v is %v\n", op.ClientId, op.RequestId)
	}

	// TODO reads
	// if op.Type == "Get" {
	// 	// fetch the key in the case of get
	// 	if v, ok := sc.data[op.Key]; ok {
	// 		op.Value = v
	// 	}
	// }

	return op
}

// creates the necessary data to handle new clients, usually their channels
func (sc *ShardCtrler) checkNewClient(clientId int64) chan Op {

	var ch chan Op
	var ok1 bool
	ch, ok1 = sc.results[clientId]
	if !ok1 {
		sc.debug("Registered new client %v\n", clientId)
		ch = make(chan Op)
		sc.results[clientId] = ch // holds the command the client was waiting for last time
	}

	_, ok2 := sc.clientIndex[clientId]
	if !ok2 {
		sc.clientIndex[clientId] = -1
	}

	return ch
}

// Receives commands from raft and applies them to the state machine
func (sc *ShardCtrler) apply() {
	for {
		msg := <-sc.applyCh // new message committed in raft
		sc.debug("New message to apply %v\n", msg)
		start := time.Now()
		if msg.CommandValid {
			op := msg.Command.(Op)

			sc.mu.Lock()

			writeCh := sc.checkNewClient(op.ClientId)

			// apply command
			result := sc.applyToStateMachine(op)

			sc.debug("Message #%v applied to state machine.\n", msg.CommandIndex)

			if msg.CommandIndex > sc.lastIndex {
				sc.lastIndex = msg.CommandIndex
			}
			// writeCh := sc.results[result.ClientId]
			sc.mu.Unlock()

			// size has exceeded
			if sc.maxraftstate != -1 && sc.rf.GetRaftStateSize() >= sc.maxraftstate {
				sc.debug("Snapshotting")
				sc.mu.Lock()
				data := sc.makeSnapshot()
				sc.rf.Snapshot(sc.lastIndex, data) // force raft to trim itself and save the data
				sc.mu.Unlock()
			}

			go func() {
				select {
				case writeCh <- result:
					sc.debug("Message #%v received.\n", op.RequestId)
				case <-time.After(time.Millisecond * 20):
					sc.debug("No one to get message #%v, skipping\n", op.RequestId)
				}
			}()

		} else if msg.SnapshotValid {
			// try to install the received snapshot
			sc.mu.Lock()
			if sc.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				sc.debug("Applying new snapshot from index %v\n", msg.SnapshotIndex)
				data := msg.Snapshot
				sc.loadSnapshot(data) //
				sc.lastIndex = msg.SnapshotIndex

			} else {
				sc.debug("Invalid snapshot. Skipping\n")
			}
			sc.mu.Unlock()
		} else {
			panic(fmt.Sprintf("Received non valid message %v\n", msg))
		}
		end := time.Now()
		sc.debug("Sending message took %v\n", time.Duration(end.Sub(start)))
	}
}

// read snapshot into memory
func (sc *ShardCtrler) loadSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	if d.Decode(&sc.data) != nil || d.Decode(&sc.clientIndex) != nil {
		panic("Error while reading snapshot in sc server")
	}
}

// save persistent memory to snapshot
func (sc *ShardCtrler) makeSnapshot() []byte {

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(sc.data)
	e.Encode(sc.clientIndex)
	return w.Bytes()

}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.data = make(map[string]string)      // storage
	sc.results = make(map[int64]chan Op)   // this to ease the reply mechanism
	sc.clientIndex = make(map[int64]int64) // this keeps track of the index of the last operation done by the client
	sc.lastIndex = -1
	sc.maxraftstate = -1 // FIXME no need to handle snapshots

	sc.loadSnapshot(persister.ReadSnapshot())

	// Your code here.
	go sc.apply()

	return sc
}
