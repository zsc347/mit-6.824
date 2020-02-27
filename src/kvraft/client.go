package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	nserver int
	leader  int
	// You will have to modify this struct.

	clientID int64
	seq      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	// You'll have to add code here.
	ck.servers = servers
	ck.nserver = len(ck.servers)
	ck.leader = 0
	ck.clientID = nrand()
	ck.seq = 1
	return ck
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
	args := GetArgs{
		Key:      key,
		ClinetID: ck.clientID,
		Seq:      ck.seq,
	}

	ck.seq++

	reply := GetReply{}
	for {
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		DPrintf("server %d reply %v", ck.leader, &reply)
		if !ok || reply.WrongLeader || reply.Err != "" {
			ck.leader = (ck.leader + 1) % ck.nserver
			continue
		}
		return reply.Value
	}
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
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClinetID: ck.clientID,
		Seq:      ck.seq,
	}

	ck.seq++

	reply := PutAppendReply{}
	for {
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("server %d, cmd %v reply %v", ck.leader, &args, &reply)
		if !ok || reply.WrongLeader || reply.Err != "" {
			ck.leader++
			continue
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
