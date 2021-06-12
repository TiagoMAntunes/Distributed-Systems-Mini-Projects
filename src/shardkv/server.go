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
	"6.824/shardctrler"
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
	lastIndex    int

	// Your definitions here.
	data        map[string]string
	results     map[int64]chan Op
	clientIndex map[int64]int64 // stores sequential values for each client

	config shardctrler.Config
	mck    *shardctrler.Clerk
}

const Debug = true

func (kv *ShardKV) debug(format string, a ...interface{}) (n int, err error) {
	if Debug {
		state := "L"
		if !kv.rf.IsLeader() {
			state = "F"
		}
		prefix := fmt.Sprintf("[%v:%v:%v] ", kv.gid, kv.me, state)
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

	// reconfiguration
	Config     shardctrler.Config
	Data       map[string]string // key -> value
	RequestIds map[int64]int64   // Keeps track of the requests of each user to avoid repeated requests
}

func (kv *ShardKV) groupCheck(key string) bool {
	shard := key2shard(key)
	return kv.config.Shards[shard] == kv.gid
}

func (kv *ShardKV) validateRequest(op Op) (Err, Op) {
	isLeader := kv.rf.IsLeader()
	if isLeader {
		kv.mu.Lock()

		kv.checkNewClient(op.ClientId)

		// check if message was already handled
		v, ok := kv.clientIndex[op.ClientId]

		validGroup := kv.groupCheck(op.Key)

		kv.mu.Unlock()

		if !validGroup {
			return ErrWrongGroup, Op{}
		} else if ok && op.RequestId <= v {
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

	// handle reconfig which is a special case
	if op.Type == "Configuration" {
		kv.config = op.Config
		for k, v := range op.Data {
			kv.data[k] = v
		}

		for k, v := range op.RequestIds {
			if kv.clientIndex[k] < v {
				kv.clientIndex[k] = v
			}
		}
	}

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
		kv.debug("New message #%v to apply %v\n", msg.CommandIndex, msg)
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

// Returns the shards and user indices to keep track of them
func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply) {
	// only leader should reply
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	set := make(map[int]bool)
	for _, v := range args.Shards {
		set[v] = true
	}

	reply.Data = make(map[string]string)
	reply.Requests = make(map[int64]int64)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// add the corresponding data
	for k, v := range kv.data {
		if _, ok := set[key2shard(k)]; ok {
			reply.Data[k] = v
		}
	}

	for k, v := range kv.clientIndex {
		reply.Requests[k] = v
	}

	reply.Err = OK
}

func (kv *ShardKV) reconfigure(conf shardctrler.Config) {
	// Message to be committed across self gid
	state := Op{
		Type:       "Configuration",
		Config:     conf,
		Data:       make(map[string]string),
		RequestIds: make(map[int64]int64),
	}

	// Calculate relevant moving shards (incoming ones)
	// shards := make([]int, 0)
	shards := make(map[int][]int)
	kv.mu.Lock()
	prevConf := kv.config
	kv.mu.Unlock()

	for i := range conf.Shards {
		if conf.Shards[i] == kv.gid && conf.Shards[i] != prevConf.Shards[i] {
			// shards = append(shards, i)
			gid := prevConf.Shards[i]
			if _, ok := shards[gid]; !ok {
				shards[gid] = make([]int, 0)
			}

			shards[gid] = append(shards[gid], i)
		}
	}

	// https://gobyexample.com/waitgroups
	var counter sync.WaitGroup
	gidChannels := make(map[int]chan map[string]string) // gid -> chan
	userChannels := make(map[int]chan map[int64]int64)

	// get the shards values from other gids
	for gid, neededShards := range shards {
		// send request to each gid and get the results
		counter.Add(1)
		gidChannels[gid] = make(chan map[string]string, 1)
		userChannels[gid] = make(chan map[int64]int64, 1)

		go func(gid int, s []int, wg *sync.WaitGroup, prevConf shardctrler.Config, gidch chan<- map[string]string, userch chan<- map[int64]int64) {
			defer wg.Done()

			args := GetShardsArgs{Shards: s}
			reply := GetShardsReply{}

			for { // keep trying until success
				if servers, ok := prevConf.Groups[gid]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])

						ok := srv.Call("ShardKV.GetShards", &args, &reply)

						if ok && reply.Err == OK {
							gidch <- reply.Data
							userch <- reply.Requests
							return
						}
						kv.debug("Failed with error: %v\n", reply.Err)
					}
				} else {
					kv.debug("No server that has the shards, skipping...")
					return
				}

				kv.debug("Sleeping...")
				time.Sleep(time.Millisecond * 100)
			}
		}(gid, neededShards, &counter, prevConf, gidChannels[gid], userChannels[gid])
	}

	kv.debug("Waiting for replies")
	counter.Wait()
	kv.debug("Going to merge now")

	// Merge replies

	for gid, ch := range gidChannels {
		select {
		case content := <-ch: // should not be blocking...
			for k, v := range content {
				state.Data[k] = v // we just substitute because all the data is coming and we don't have the data here (and this is the most updated one supposedly...)
			}
			kv.debug("Merged data %v\n", gid)
		default:
			kv.debug("No data for gid %v\n", gid)
		}
	}

	for gid, ch := range userChannels {
		select {
		case content := <-ch:
			for k, v := range content {
				if state.RequestIds[k] < v {
					state.RequestIds[k] = v // keep track of highest only
				}
			}
			kv.debug("Merged users from gid %v\n", gid)
		default:
			kv.debug("No data for gid %v\n", gid)
		}
	}

	// Submit in state machine
	kv.debug("Applying config in state machine")
	kv.doOp(state) // commit it and wait for it to finish or simply move on if in a partition
}

// polls a new config every interval and triggers a config update if it changed
func (kv *ShardKV) pollConfig() {
	const duration = time.Millisecond * 100

	for {
		// only leader needs to take into account this
		if !kv.rf.IsLeader() {
			time.Sleep(time.Millisecond * 500)
			continue
		}

		// poll
		conf := kv.mck.Query(-1)
		kv.mu.Lock()
		num := kv.config.Num
		kv.mu.Unlock()

		if conf.Num != num {
			kv.debug("Found new configuration %v, updating...", conf)
			kv.reconfigure(conf)
			kv.debug("Done reconfiguring")
			// kv.mu.Lock()
			// kv.config = conf
			// kv.mu.Unlock()
		}

		time.Sleep(duration)
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
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)      // storage
	kv.results = make(map[int64]chan Op)   // this to ease the reply mechanism
	kv.clientIndex = make(map[int64]int64) // this keeps track of the index of the last operation done by the client
	kv.lastIndex = -1

	kv.loadSnapshot(persister.ReadSnapshot())

	go kv.apply()
	go kv.pollConfig()

	return kv
}
