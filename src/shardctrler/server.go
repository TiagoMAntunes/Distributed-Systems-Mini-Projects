package shardctrler

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sort"
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

	data        map[string]string // TODO remove this (trash)
	results     map[int64]chan Op // because the channels did not exist, messages were never being applied. analyse this<
	clientIndex map[int64]int64   // stores sequential values for each client

	lastIndex int

	configs []Config // indexed by config num
}

const Debug = false

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

		if ok && op.RequestId <= v {
			sc.debug("Repeated request, op=%v\n", op)
			sc.mu.Lock()
			defer sc.mu.Unlock()
			return OK, sc.applyToStateMachine(op, true)

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

	req := Op{Type: "Query", ClientId: args.ClientId, RequestId: args.RequestId, Num: args.Num}

	status, result := sc.validateRequest(req)

	if status == OK {
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
	readCh := sc.checkNewClient(op.ClientId)
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

// just return a copy of the previous configuration with a new version number
func (sc *ShardCtrler) makeConfig() Config {
	conf := Config{}
	prev := sc.configs[len(sc.configs)-1]

	conf.Num = prev.Num + 1
	conf.Shards = prev.Shards // keep the previous shards so that we can move as few as possible
	conf.Groups = make(map[int][]string)
	for gid, servers := range prev.Groups {
		conf.Groups[gid] = servers
	}
	return conf
}

// This function will rebalance the existing incomplete configuration
// It will take into account both the previous shards state and the new groups
func rebalanceShards(conf *Config) {
	mean := NShards / len(conf.Groups) // Shards should be evenly distributed

	// get GID -> Shards
	gids := map[int][]int{} // needs to be a map since we have no guarantee that shards are contiguous

	// add previous shards' gid
	for _, gid := range conf.Shards {
		if _, ok := gids[gid]; !ok {
			// fmt.Printf("Created!\n")
			gids[gid] = make([]int, 0)
		}
	}

	// add new groups gids in case they are new
	for gid := range conf.Groups {
		if _, ok := gids[gid]; !ok {
			gids[gid] = make([]int, 0)
		}
	}

	for shard, gid := range conf.Shards {
		gids[gid] = append(gids[gid], shard)
	}

	// Now calculate how many values from each gid need to be moved
	type Combo struct {
		gid    int
		offset int
	}
	offsets := []Combo{}
	for gid, shards := range gids {
		offset := len(shards) - mean // get the difference

		// if it's an old shard group, then completely ignore the offset and just move all of them
		if _, ok := conf.Groups[gid]; !ok {
			offset = len(shards)
		}

		offsets = append(offsets, Combo{gid: gid, offset: offset})
	}

	// sort via offset in increasing order
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i].offset < offsets[j].offset
	})
	// fmt.Printf("Sorted: %v\n", offsets)

	// Now greedily move from the last to the first until it can't move anymore
	for i := 0; i < len(offsets) && offsets[i].offset < 0; i++ {
		for j := len(offsets) - 1; j > i; j-- {
			if offsets[j].offset <= 0 {
				continue // already moved all shards from this group
			}

			toadd := -offsets[i].offset // need to get this amount of shards in this gid
			gid := offsets[i].gid
			target_gid := offsets[j].gid
			target_offset := offsets[j].offset

			// calculate amount to move
			canmove := target_offset
			if canmove > toadd {
				canmove = toadd
			}

			// move and trim log
			// fmt.Printf("Handling gid=%v, target_gid=%v, canmove=%v, offset=%v, target_offset=%v\n", gid, target_gid, canmove, toadd, target_offset)
			// fmt.Printf("Previous state: %v\n", gids[gid])
			gids[gid] = append(gids[gid], gids[target_gid][:canmove]...)
			gids[target_gid] = gids[target_gid][canmove:]
			// fmt.Printf("New state: %v\n", gids[gid])
			offsets[j].offset -= canmove //update moved data
			offsets[i].offset += canmove
		}

	}

	// fmt.Printf("Shards %v\n", conf.Shards)
	// fmt.Printf("Gids: %v\n", gids)
	// write shards
	for gid, shards := range gids {
		if _, ok := conf.Groups[gid]; !ok { // avoid writing old groups used in balancing
			// fmt.Printf("Skipping %v\n", gid)
			continue
		}

		for _, shard := range shards {
			conf.Shards[shard] = gid
		}
		// fmt.Printf("Updated: %v\n", conf.Shards)
	}

}

func (sc *ShardCtrler) doJoin(op Op) {
	conf := sc.makeConfig()

	for gid, servers := range op.Servers {
		sc.debug("Gid %v servers %v\n", gid, servers)
		conf.Groups[gid] = servers // set the groups
	}

	delete(conf.Groups, 0) // remove 0 key  always in this case
	sc.debug("Config groups: %v\n", conf.Groups)
	rebalanceShards(&conf)
	sc.configs = append(sc.configs, conf)
	sc.debug("New config: %v\n", conf)
}

func (sc *ShardCtrler) doLeave(op Op) {
	conf := sc.makeConfig()

	sc.debug("Removing %v, before %v\n", op.GIDs, conf.Groups)
	for _, gid := range op.GIDs {
		delete(conf.Groups, gid)
	}
	sc.debug("After removing %v\n", conf.Groups)
	if len(conf.Groups) == 0 {
		// add Group 0 when there is no configuration specified
		conf.Groups[0] = make([]string, 0)
	}

	rebalanceShards(&conf)
	sc.configs = append(sc.configs, conf)
	sc.debug("New config: %v\n", conf)
}

func (sc *ShardCtrler) doMove(op Op) {
	conf := sc.makeConfig()

	conf.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, conf)
	sc.debug("New config: %v\n, conf")
}
func (sc *ShardCtrler) applyToStateMachine(op Op, repeated bool) Op {

	// update data for writes, avoid writing old data
	if !repeated && sc.clientIndex[op.ClientId] < op.RequestId {
		switch op.Type {
		case "Join":
			sc.doJoin(op)
		case "Query":
			//TODO do i need to do anything?
		case "Leave":
			sc.doLeave(op)
		case "Move":
			sc.doMove(op)
		default:
			panic(fmt.Sprintf("Unrecognized command %v\n", op))
		}

		sc.clientIndex[op.ClientId] = op.RequestId // update last update info
		// sc.debug("New id to client %v is %v\n", op.ClientId, op.RequestId)
	}

	// Only read is a Query
	if op.Type == "Query" {
		index := op.Num
		if index == -1 || index >= len(sc.configs) {
			index = len(sc.configs) - 1
		}

		op.Config = sc.configs[index]
		sc.debug("Config %v has content %v\n", index, op.Config)
	}

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
		sc.clientIndex[clientId] = 0
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
			result := sc.applyToStateMachine(op, false)

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
	for i := range sc.configs[0].Shards {
		sc.configs[0].Shards[i] = 0
	}

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
