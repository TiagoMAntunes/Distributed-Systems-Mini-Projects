package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	leader  int32
	me      int64
	index   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0 // doesn't know who's the leader
	ck.me = nrand()
	ck.index = 0
	return ck
}

var DEBUG = false

func (ck *Clerk) debug(format string, content ...interface{}) {
	if DEBUG {
		prefix := fmt.Sprintf("[%v] ", ck.me)
		log.Printf(prefix+format, content...)
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, RequestId: ck.index, ClientId: ck.me}
	reply := GetReply{}
	ck.index++

	leader := atomic.LoadInt32(&ck.leader)

	for {
		ck.debug("Contacting server %v\n", leader)
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		ck.debug("Received response from %v\n", leader)
		if reply.Err == OK {
			break
		}

		if reply.Err == ErrNoKey {
			reply.Value = ""
			break
		}

		if !ok || reply.Err == ErrWrongLeader {
			leader = (leader + 1) % int32(len(ck.servers))
			ck.debug("Picking next leader %v\n", leader)
		}

		ck.debug("Retrying message due to %v, status was %v\n", reply.Err, ok)
		time.Sleep(time.Millisecond * 10)
	}

	atomic.StoreInt32(&ck.leader, leader)
	// ck.debug("Client got key with value %v\n", reply.Value)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op, RequestId: ck.index, ClientId: ck.me}
	reply := PutAppendReply{}
	ck.index++

	leader := atomic.LoadInt32(&ck.leader)

	for {
		ck.debug("Contacting server %v\n", leader)
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		ck.debug("Received response from %v\n", leader)

		if reply.Err == OK {
			break
		}

		if !ok || reply.Err == ErrWrongLeader {
			leader = (leader + 1) % int32(len(ck.servers))
			ck.debug("Picking next leader %v\n", leader)
		}

		ck.debug("Retrying message due to %v, status was %v\n", reply.Err, ok)
		time.Sleep(time.Millisecond * 10)
	}

	atomic.StoreInt32(&ck.leader, leader)
	// ck.debug("Client submitted operation with value %v successfully\n", value)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
