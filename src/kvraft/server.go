package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	ClientID int64
	Seq      int64

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string // Get or put
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	getReplyChan chan GetReply
	putReplyChan chan PutAppendReply

	// Your definitions here.
	store        map[string]string
	reponsesChan map[int]chan *ResponseMsg
}

type ResponseMsg struct {
	ClientID    int64
	Seq         int64
	KeyNotExist bool
	Value       string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type: "GET",
		Key:  args.Key,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// TODO

	ch := make(chan *ResponseMsg, 1)
	kv.mu.Lock()
	kv.reponsesChan[index] = ch
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		if msg.ClientID == args.ClinetID && msg.Seq == args.Seq {
			if msg.KeyNotExist {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
			}
			reply.WrongLeader = false
			reply.Value = msg.Value
		} else {
			reply.Err = ErrApplyFailed
			reply.WrongLeader = false
			reply.Value = ""
		}
	case <-time.After(40 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.reponsesChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan *ResponseMsg, 1)
	kv.mu.Lock()
	kv.reponsesChan[index] = ch
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		if msg.ClientID == args.ClinetID && msg.Seq == args.Seq {
			reply.Err = OK
			reply.WrongLeader = false
		} else {
			reply.WrongLeader = false
			reply.Err = ErrApplyFailed
		}
	case <-time.After(40 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	// TODO

	kv.mu.Lock()
	delete(kv.reponsesChan, index)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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

	go func() {
		for {
			msg := <-kv.applyCh
			kv.mu.Lock()
			defer kv.mu.Unlock()

			op := msg.Command.(Op)
			switch op.Type {
			case "Put":
				kv.store[op.Key] = op.Value
				if ch, ok := kv.reponsesChan[msg.CommandIndex]; ok {
					replyMsg := &ResponseMsg{
						ClientID: op.ClientID,
						Seq:      op.Seq,
					}
					ch <- replyMsg
				}
			case "Append":
				kv.store[op.Key] = kv.store[op.Key] + op.Value
				if ch, ok := kv.reponsesChan[msg.CommandIndex]; ok {
					replyMsg := &ResponseMsg{
						ClientID: op.ClientID,
						Seq:      op.Seq,
					}
					ch <- replyMsg
				}
			case "Get":
				if ch, ok := kv.reponsesChan[msg.CommandIndex]; ok {
					v, ok := kv.store[op.Key]
					replyMsg := &ResponseMsg{
						ClientID:    op.ClientID,
						Seq:         op.Seq,
						KeyNotExist: !ok,
						Value:       v,
					}
					ch <- replyMsg
				}
			}
		}
	}()

	// You may need initialization code here.

	return kv
}
