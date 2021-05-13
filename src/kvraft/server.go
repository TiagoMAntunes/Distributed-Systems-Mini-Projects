package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func (kv *KVServer) debug(format string, a ...interface{}) (n int, err error) {
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
	Type  string
	Key   string
	Value string

	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data        map[string]string
	results     map[int64]chan Op // because the channels did not exist, messages were never being applied. analyse this<
	clientIndex map[int64]int64   // stores sequential values for each client
}

// creates the necessary data to handle new clients, usually their channels
func (kv *KVServer) checkNewClient(clientId int64) {
	kv.mu.Lock()
	_, ok := kv.results[clientId]
	if !ok {
		kv.debug("Registered new client %v\n", clientId)
		kv.results[clientId] = make(chan Op) // holds the command the client was waiting for last time
	}
	kv.clientIndex[clientId] = -1
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.debug("Get received, args=%v\n", args)

	isLeader := kv.rf.IsLeader()

	if isLeader {
		kv.checkNewClient(args.ClientId)
		status, result := kv.doOp(Op{Type: "Get", Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId, Value: ""})

		reply.Err = status
		if status == OK {
			if result.Value != "" {
				// key found
				reply.Err = OK
				reply.Value = result.Value
			} else {
				kv.debug("No key found for %v\n", result)
				reply.Err = ErrNoKey
			}
		}
	} else {
		reply.Err = ErrWrongLeader
	}

	kv.debug("Get ended, args=%v, reply=%v\n", args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.debug("Put/Append received, args=%v\n", args)

	isLeader := kv.rf.IsLeader()

	if isLeader {
		kv.checkNewClient(args.ClientId)
		status, result := kv.doOp(Op{Type: args.Op, Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId, Value: args.Value})

		reply.Err = status
		if status == OK && result.RequestId >= args.RequestId {
			reply.Err = OK
		}
	} else {
		reply.Err = ErrWrongLeader
	}

	kv.debug("Put/Append ended, args=%v, reply=%v\n", args, reply)
}

// Initiates a command and waits for it to finish
func (kv *KVServer) doOp(op Op) (Err, Op) {
	kv.rf.Start(op) // value goes as null for check after

	kv.mu.Lock()
	readCh := kv.results[op.ClientId]
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

func (kv *KVServer) applyToStateMachine(op Op) Op {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// update data for writes, avoid writing old data
	if kv.clientIndex[op.ClientId] < op.RequestId {
		switch op.Type {
		case "Append":
			kv.data[op.Key] += op.Value
		case "Put":
			kv.data[op.Key] = op.Value
		case "Get":
			// skip
		default:
			panic(fmt.Sprintf("Unrecognized command %v\n", op))
		}

		kv.clientIndex[op.ClientId] = op.RequestId // update last update info
	}

	// fetch the key in the case of get
	if v, ok := kv.data[op.Key]; op.Type == "Get" && ok {
		op.Value = v
	}

	return op
}

// Receives commands from raft and applies them to the state machine
func (kv *KVServer) apply() {
	for {
		msg := <-kv.applyCh // new message committed in raft
		kv.debug("New message to apply %v\n", msg)
		if msg.CommandValid {
			op := msg.Command.(Op)

			// apply command
			result := kv.applyToStateMachine(op)
			kv.debug("Message #%v applied to state machine.\n", msg.CommandIndex)

			kv.checkNewClient(op.ClientId)
			kv.mu.Lock()
			writeCh := kv.results[result.ClientId]
			kv.mu.Unlock()

			select {
			case writeCh <- result:
				kv.debug("Message received.\n")
			case <-time.After(time.Millisecond * 10):
				kv.debug("No one to get new message, skipping\n")

			}

			// check if can snapshot
		} else if msg.SnapshotValid {
			// TODO
		} else {
			panic(fmt.Sprintf("Received non valid message %v\n", msg))
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)      // storage
	kv.results = make(map[int64]chan Op)   // this to ease the reply mechanism
	kv.clientIndex = make(map[int64]int64) // this keeps track of the index of the last operation done by the client

	go kv.apply()

	log.SetFlags(log.Ldate | log.Lmicroseconds)
	return kv
}
