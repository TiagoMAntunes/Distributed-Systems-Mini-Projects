package kvraft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

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

	lastIndex int
}

// creates the necessary data to handle new clients, usually their channels
func (kv *KVServer) checkNewClient(clientId int64) chan Op {

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
		kv.clientIndex[clientId] = -1
	}

	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.debug("Get received, args=%v\n", args)

	isLeader := kv.rf.IsLeader()

	if isLeader {
		kv.mu.Lock()
		kv.checkNewClient(args.ClientId)

		// check if message was already handled
		v, ok := kv.clientIndex[args.ClientId]
		kv.mu.Unlock()

		if ok && args.RequestId <= v {
			// repeated request
			kv.debug("Repeated request, args=%v\n", args)
			reply.Err = OK
			// fetch the key in the case of get
			reply.Value = ""
			kv.mu.Lock()
			if v, ok := kv.data[args.Key]; ok {
				reply.Value = v
			}
			kv.mu.Unlock()
		} else {
			status, result := kv.doOp(Op{Type: "Get", Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId, Value: ""})

			// linearizability in gets requires it to only read the specific request in time
			if status == OK && result.RequestId >= args.RequestId {
				if result.Value != "" {
					// key found
					reply.Err = OK
					reply.Value = result.Value
				} else {
					kv.debug("No key found for %v\n", result)
					reply.Err = ErrNoKey
				}
			} else if status == OK {
				reply.Err = ErrNotCommitted
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
		kv.mu.Lock()
		kv.checkNewClient(args.ClientId)

		// check if message was already handled
		v, ok := kv.clientIndex[args.ClientId]
		kv.mu.Unlock()

		if ok && args.ClientId <= v {
			// repeated request
			kv.debug("Repeated request, args=%v\n", args)
			reply.Err = OK
		} else {
			//new request
			status, result := kv.doOp(Op{Type: args.Op, Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId, Value: args.Value})

			// in appends and puts we don't need to be so strict because the user can't see the info
			// but a get after will need to see it
			if status == OK && result.RequestId >= args.RequestId {
				reply.Err = OK
			} else if status == OK {
				reply.Err = ErrNotCommitted
			}
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

	// update data for writes, avoid writing old data
	if kv.clientIndex[op.ClientId] < op.RequestId {
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

// Receives commands from raft and applies them to the state machine
func (kv *KVServer) apply() {
	for {
		msg := <-kv.applyCh // new message committed in raft
		kv.debug("New message to apply %v\n", msg)
		start := time.Now()
		if msg.CommandValid {
			op := msg.Command.(Op)

			kv.mu.Lock()

			writeCh := kv.checkNewClient(op.ClientId)

			// apply command
			result := kv.applyToStateMachine(op)

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
func (kv *KVServer) loadSnapshot(data []byte) {
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
func (kv *KVServer) makeSnapshot() []byte {

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.clientIndex)
	return w.Bytes()

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
	kv.lastIndex = -1

	kv.loadSnapshot(persister.ReadSnapshot())

	go kv.apply()

	log.SetFlags(log.Ldate | log.Lmicroseconds)
	return kv
}
