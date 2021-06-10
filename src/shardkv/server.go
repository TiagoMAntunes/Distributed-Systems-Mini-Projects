package shardkv

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	data        map[string]string // TODO remove this (trash)
	results     map[int64]chan Op // because the channels did not exist, messages were never being applied. analyse this<
	clientIndex map[int64]int64   // stores sequential values for each client

	lastIndex int
}

const Debug = true

func (kv *ShardKV) debug(format string, a ...interface{}) (n int, err error) {
	if Debug {
		state := "L"
		if !kv.rf.IsLeader() {
			state = "F"
		}
		prefix := fmt.Sprintf("[%v:%v] ", kv.me, state)
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

	// individual args of each command
	Key   string
	Value string
}

func (kv *ShardKV) validateRequest(op Op) (Err, Op) {

	isLeader := kv.rf.IsLeader()
	if isLeader {
		kv.mu.Lock()

		kv.checkNewClient(op.ClientId)

		// check if message was already handled
		v, ok := kv.clientIndex[op.ClientId]

		kv.mu.Unlock()

		if ok && op.RequestId <= v {
			kv.debug("Repeated request, op=%v\n", op)
			kv.mu.Lock()
			defer kv.mu.Unlock()
			return OK, kv.applyToStateMachine(op, true)

		} else {
			status, res := kv.doOp(op)
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.debug("Get started, args:=%v", args)

	req := Op{Type: "Get", ClientId: args.ClientId, RequestId: args.RequestId, Key: args.Key}

	status, result := kv.validateRequest(req)
	if status == OK {
		reply.Err = OK
		reply.Value = result.Value
	} else {
		reply.Err = status
	}

	kv.debug("Get ended, args:=%v, reply:=%v", args, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.debug("PutAppend started, args:=%v", args)

	req := Op{Type: args.Op, ClientId: args.ClientId, RequestId: args.RequestId, Key: args.Key, Value: args.Value}

	status, _ := kv.validateRequest(req)

	reply.Err = status

	kv.debug("PutAppend ended, args:=%v, reply:=%v", args, reply)
}

// Initiates a command and waits for it to finish
func (kv *ShardKV) doOp(op Op) (Err, Op) {
	kv.rf.Start(op) // value goes as null for check after

	kv.mu.Lock()
	readCh := kv.checkNewClient(op.ClientId)
	kv.mu.Unlock()

	// get the response

	select {
	case response := <-readCh:
		return OK, response
	case <-time.After(2 * time.Second):
		kv.debug("Command %v did not commit in time.\n", op)
		return ErrNotCommitted, Op{}
	}

}

func (kv *ShardKV) applyToStateMachine(op Op, repeated bool) Op {

	// update data for writes, avoid writing old data
	if !repeated && kv.clientIndex[op.ClientId] < op.RequestId {
		switch op.Type {
		case "Append":
			kv.data[op.Key] += op.Value
			kv.debug("Key=%v new value=%v\n", op.Key, kv.data[op.Key])
		case "Put":
			kv.data[op.Key] = op.Value
			kv.debug("Key=%v new value=%v\n", op.Key, kv.data[op.Key])
		case "Get":
			// skip
		default:
			panic(fmt.Sprintf("Unrecognized command %v\n", op))
		}

		kv.clientIndex[op.ClientId] = op.RequestId // update last update info
		kv.debug("New id to client %v is %v\n", op.ClientId, op.RequestId)
	}

	if op.Type == "Get" {
		// fetch the key in the case of get
		if v, ok := kv.data[op.Key]; ok {
			op.Value = v
		}
	}

	return op
}

// creates the necessary data to handle new clients, usually their channels
func (kv *ShardKV) checkNewClient(clientId int64) chan Op {

	var ch chan Op
	var ok1 bool
	ch, ok1 = kv.results[clientId]
	if !ok1 {
		kv.debug("Registered new client %v\n", clientId)
		ch = make(chan Op)
		kv.results[clientId] = ch // holds the command the client was waiting for last time
	}

	_, ok2 := kv.clientIndex[clientId]
	if !ok2 {
		kv.clientIndex[clientId] = 0
	}

	return ch
}

// Receives commands from raft and applies them to the state machine
func (kv *ShardKV) apply() {
	for {
		msg := <-kv.applyCh // new message committed in raft
		kv.debug("New message to apply %v\n", msg)
		start := time.Now()
		if msg.CommandValid {
			op := msg.Command.(Op)

			kv.mu.Lock()

			writeCh := kv.checkNewClient(op.ClientId)
			// apply command
			result := kv.applyToStateMachine(op, false)

			kv.debug("Message #%v applied to state machine.\n", msg.CommandIndex)

			if msg.CommandIndex > kv.lastIndex {
				kv.lastIndex = msg.CommandIndex
			}
			// writeCh := kv.results[result.ClientId]
			kv.mu.Unlock()

			// size has exceeded
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
				kv.debug("Snapshotting")
				kv.mu.Lock()
				data := kv.makeSnapshot()
				kv.rf.Snapshot(kv.lastIndex, data) // force raft to trim itself and save the data
				kv.mu.Unlock()
			}

			go func() {
				select {
				case writeCh <- result:
					kv.debug("Message #%v received.\n", op.RequestId)
				case <-time.After(time.Millisecond * 20):
					kv.debug("No one to get message #%v, skipping\n", op.RequestId)
				}
			}()

		} else if msg.SnapshotValid {
			// try to install the received snapshot
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.debug("Applying new snapshot from index %v\n", msg.SnapshotIndex)
				data := msg.Snapshot
				kv.loadSnapshot(data) //
				kv.lastIndex = msg.SnapshotIndex

			} else {
				kv.debug("Invalid snapshot. Skipping\n")
			}
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("Received non valid message %v\n", msg))
		}
		end := time.Now()
		kv.debug("Sending message took %v\n", time.Duration(end.Sub(start)))
	}
}

// read snapshot into memory
func (kv *ShardKV) loadSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	if d.Decode(&kv.data) != nil || d.Decode(&kv.clientIndex) != nil {
		panic("Error while reading snapshot in kv server")
	}
}

// save persistent memory to snapshot
func (kv *ShardKV) makeSnapshot() []byte {

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.clientIndex)
	return w.Bytes()

}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)      // storage
	kv.results = make(map[int64]chan Op)   // this to ease the reply mechanism
	kv.clientIndex = make(map[int64]int64) // this keeps track of the index of the last operation done by the client
	kv.lastIndex = -1

	kv.loadSnapshot(persister.ReadSnapshot())

	go kv.apply()

	return kv
}
