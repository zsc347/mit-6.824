package raftkv

import (
	"encoding/json"
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

// 如何防范客户端的重放攻击 ?
// 情景描述: 客户端发出请求, raft已经正确commit掉请求记录,并且会从apply chan中返回和应用
// 生成回复.
// 但是,客户端没有等待那么久, 或者客户端断开了连接
// 此时,客户端会重新以相同的seq发出请求
// 期望的结果是, 此次请求不会被再次执行, 并返回之前响应的结果, 此时有可能在另一个服务器中返回

// 解决方案
// 真正生效的是已经apply的数据, 所以必须从apply的数据中获取到请求是否已经响应过
// 记录状态, 每一个client的最后一个请求seq
// 则对应相同的apply请求,由于记录了所有客户端的最大seq,所以可以保证操作不会重复执行

// 对于GET请求呢?
// 客户端 a 以 seq=1 朝 server 1 发起GET
// raft开始apply
// raft apply成功
// 客户断超时,没有接收到响应
// 客户端 a 再次 seq=1 朝 server 2 发起GET
// 此时已经有了其他客户端的PUT改动
// raft log中也已经有记录 [client a, seq 1, get x]
// 此时是否可以直接返回最新的数据?
// 此时返回最新的数据是否违背了强一致和线性一致?
// 比如log的记录如下
// [client a, seq 1, get x], return 0  success to append, fail to reply
// [client b, seq 1, put x 1]
// [client a, seq 1, get x], return 1, success to append, success to reply

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store         map[string]string
	clientLastSeq map[int64]int64

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
		Type:     "Get",
		Key:      args.Key,
		ClientID: args.ClinetID,
		Seq:      args.Seq,
	}

	// TODO
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	ch := make(chan *ResponseMsg)
	kv.reponsesChan[index] = ch
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		DPrintf("server %d channel response %v", kv.me, msg)
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
	case <-time.After(5 * time.Second):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.reponsesChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClinetID,
		Seq:      args.Seq,
	}

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	ch := make(chan *ResponseMsg)
	kv.reponsesChan[index] = ch
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		DPrintf("server %d channel response %v", kv.me, msg)
		if msg.ClientID == args.ClinetID && msg.Seq == args.Seq {
			reply.Err = OK
			reply.WrongLeader = false
		} else {
			reply.WrongLeader = false
			reply.Err = ErrApplyFailed
		}
	case <-time.After(5 * time.Second):
		reply.Err = ErrTimeout
		DPrintf("server %d timeout, failed to reply", kv.me)
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
	kv.store = make(map[string]string)
	kv.clientLastSeq = make(map[int64]int64)
	kv.reponsesChan = make(map[int]chan *ResponseMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for {
			msg := <-kv.applyCh

			kv.mu.Lock()

			msgJSON, _ := json.Marshal(msg)
			DPrintf("msg from raft apply chan %s, index %d ", msgJSON, msg.CommandIndex)

			op := msg.Command.(Op)
			switch op.Type {
			case "Put":
				if op.Seq > kv.clientLastSeq[op.ClientID] {
					DPrintf("server %d execute cmd Put key %s - Value %s", kv.me, op.Key, op.Value)
					kv.clientLastSeq[op.ClientID] = op.Seq
					kv.store[op.Key] = op.Value
				}
				if ch, ok := kv.reponsesChan[msg.CommandIndex]; ok {
					replyMsg := &ResponseMsg{
						ClientID: op.ClientID,
						Seq:      op.Seq,
					}
					ch <- replyMsg
				}
			case "Append":
				if op.Seq > kv.clientLastSeq[op.ClientID] {
					kv.clientLastSeq[op.ClientID] = op.Seq
					kv.store[op.Key] = kv.store[op.Key] + op.Value
					DPrintf("server %d execute cmd Append key %s - Value %s", kv.me, op.Key, op.Value)
				}
				if ch, ok := kv.reponsesChan[msg.CommandIndex]; ok {
					replyMsg := &ResponseMsg{
						ClientID: op.ClientID,
						Seq:      op.Seq,
					}
					ch <- replyMsg
				}
			case "Get":
				if op.Seq > kv.clientLastSeq[op.ClientID] {
					// we have already respond to the message and perform the action
					kv.clientLastSeq[op.ClientID] = op.Seq
				}

				DPrintf("server %d execute cmd GET key %s", kv.me, op.Key)
				if ch, ok := kv.reponsesChan[msg.CommandIndex]; ok {
					v, ok := kv.store[op.Key]
					replyMsg := &ResponseMsg{
						ClientID:    op.ClientID,
						Seq:         op.Seq,
						KeyNotExist: !ok,
						Value:       v,
					}
					replyMsgJSON, _ := json.Marshal(replyMsg)
					DPrintf("Get reply to chan %s", replyMsgJSON)
					ch <- replyMsg
				}
			}

			kv.mu.Unlock()
		}
	}()

	// You may need initialization code here.

	return kv
}
