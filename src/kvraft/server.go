package raftkv

import (
	"bytes"
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

// 重放攻击+server-fail vs snapshot+last-seq
// last seq 如何保证重放攻击在kv server非正常挂掉时还能恢复?
// store可以通过重放恢复
// last seq 也需要可以通过log replay恢复
// command 中记录了last seq信息
// snapshot中也应当记录此信息
// last seq 信息应当只通过log play来维护或者通过log play可以完全恢复

type KVServer struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	shutdown chan interface{}

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

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

	isLeader, msg, timeout := kv.start(&op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrApplyFailed
		return
	}
	if timeout {
		reply.WrongLeader = false
		reply.Err = ErrTimeout
		return
	}

	reply.Err = OK
	reply.WrongLeader = false
	reply.KeyNotExist = msg.KeyNotExist
	reply.Value = msg.Value
}

func (kv *KVServer) start(op *Op) (bool, *ResponseMsg, bool) {

	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		return false, nil, false
	}
	kv.mu.Lock()
	ch := make(chan *ResponseMsg, 1)
	kv.reponsesChan[index] = ch
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		if msg.ClientID == op.ClientID && op.Seq == op.Seq {
			return true, msg, false
		}
		return false, nil, false
	case <-time.After(5 * time.Second):
		return false, nil, true
	}
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
	isLeader, _, timeout := kv.start(&op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrApplyFailed
		return
	}
	if timeout {
		reply.WrongLeader = false
		reply.Err = ErrTimeout
		return
	}

	reply.Err = OK
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
	close(kv.shutdown)
}

func (kv *KVServer) notifyIfPresent(index int, rsp *ResponseMsg) {
	if ch, ok := kv.reponsesChan[index]; ok {
		delete(kv.reponsesChan, index)
		ch <- rsp
	}
}

func (kv *KVServer) tryPersist() {
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// kv server state
	e.Encode(kv.store)
	e.Encode(kv.clientLastSeq)
	snapshot := w.Bytes()
	kv.rf.TakeSnapshot(snapshot)
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
	kv.persister = persister
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.store = make(map[string]string)
	kv.clientLastSeq = make(map[int64]int64)
	kv.reponsesChan = make(map[int]chan *ResponseMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shutdown = make(chan interface{})

	go func() {
		for {
			select {
			case msg := <-kv.applyCh:
				if !msg.CommandValid {
					continue
				}

				kv.mu.Lock()
				kv.tryPersist()

				op := msg.Command.(Op)

				var replyMsg *ResponseMsg
				if op.Type == "Get" {
					kv.clientLastSeq[op.ClientID] = op.Seq
					v, ok := kv.store[op.Key]
					replyMsg = &ResponseMsg{
						ClientID:    op.ClientID,
						Seq:         op.Seq,
						KeyNotExist: !ok,
						Value:       v,
					}
				} else {
					if op.Seq > kv.clientLastSeq[op.ClientID] {
						kv.clientLastSeq[op.ClientID] = op.Seq
						if op.Type == "Put" {
							kv.store[op.Key] = op.Value
						} else {
							kv.store[op.Key] += op.Value
						}
					}
					replyMsg = &ResponseMsg{
						ClientID: op.ClientID,
						Seq:      op.Seq,
					}
				}
				kv.notifyIfPresent(msg.CommandIndex, replyMsg)

				kv.mu.Unlock()
			case <-kv.shutdown:
				return
			}

		}
	}()

	// You may need initialization code here.

	return kv
}
