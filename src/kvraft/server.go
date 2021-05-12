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
	results     map[int]chan Op // will store the information for a given command
	clientIndex map[int64]int64 // stores sequential values for each client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	isLeader := kv.rf.IsLeader()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// get does not need to handle reliable

	op := Op{Type: "Get", Key: args.Key, RequestId: args.RequestId, ClientId: args.ClientId}
	kv.debug("New get: key=%v, value=%v, clientId=%v, requestId=%v\n", op.Key, op.Value, op.ClientId, op.RequestId)
	status, op := kv.doOp(op)
	if !status {
		// message was not successful
		reply.Err = ErrNoKey
		return
	}

	kv.mu.Lock()
	kv.clientIndex[args.ClientId] = args.RequestId
	kv.mu.Unlock()

	reply.Err = OK
	reply.Value = op.Value

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	isLeader := kv.rf.IsLeader()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// unrealiable
	kv.mu.Lock()
	if v, ok := kv.clientIndex[args.ClientId]; ok && args.RequestId <= v {
		kv.debug("Returning previous value")
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, RequestId: args.RequestId, ClientId: args.ClientId}
	kv.debug("New PutAppend: key=%v, value=%v, clientId=%v, requestId=%v\n", op.Key, op.Value, op.ClientId, op.RequestId)
	status, op := kv.doOp(op)
	kv.debug("Finished doOp")
	if !status {
		reply.Err = ErrNotCommitted
		return
	}

	kv.mu.Lock()
	kv.clientIndex[args.ClientId] = args.RequestId
	kv.mu.Unlock()

	reply.Err = OK
}

func (kv *KVServer) doOp(op Op) (bool, Op) {
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return false, Op{}
	}

	kv.debug("New command index %v\n", index)

	kv.mu.Lock()
	// get channel with the committed operation
	opCh, ok := kv.results[index]
	if !ok {
		opCh = make(chan Op, 1)
		kv.results[index] = opCh
	}
	kv.mu.Unlock()

	select {
	case resOp := <-opCh:
		// message was committed on time
		status := resOp.RequestId == op.RequestId && resOp.ClientId == op.ClientId
		kv.debug("Committed, returning")
		return status, resOp
	case <-time.After(time.Millisecond * 700):
		kv.debug("Never arrived")
		return false, Op{}
	}
}

// always running, applying the messages that come from the channel
func (kv *KVServer) apply() {
	for {
		msg := <-kv.applyCh // new message committed in raft
		if !msg.CommandValid {
			// TODO
			continue
		}
		index := msg.CommandIndex
		op := msg.Command.(Op)

		kv.mu.Lock()
		kv.debug("New apply, index=%v, op=%v\n", index, op)

		// apply op to state machine
		switch op.Type {
		case "Get":
			op.Value = kv.data[op.Key] // obtain stored value
		case "Put":
			kv.data[op.Key] = op.Value
		case "Append":
			kv.data[op.Key] += op.Value
		}

		// set results as available
		opCh, ok := kv.results[index]
		if !ok {
			opCh = make(chan Op, 1)
			kv.results[index] = opCh
		}

		kv.mu.Unlock()

		opCh <- op
		kv.debug("Message written")
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
	kv.results = make(map[int]chan Op)     // this to ease the reply mechanism
	kv.clientIndex = make(map[int64]int64) // this keeps track of the index of the last operation done by the client

	go kv.apply()

	log.SetFlags(log.Ldate | log.Lmicroseconds)
	return kv
}
