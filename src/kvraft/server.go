package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"os"
	"raft"
	"sort"
	"sync"
)

type PendingOp struct {
	op *Op
	c  chan bool // waiting chan
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	pendingOps map[int][]*PendingOp // index -> list of ops

	data           map[string]string
	client_last_op map[int64]int64

	logger *log.Logger
}

type PendingOpsSorter struct {
	ops []*PendingOp
}

func (s *PendingOpsSorter) Len() int           { return len(s.ops) }
func (s *PendingOpsSorter) Swap(i, j int)      { s.ops[i], s.ops[j] = s.ops[j], s.ops[i] }
func (s *PendingOpsSorter) Less(i, j int) bool { return s.ops[i].op.Id < s.ops[j].op.Id }

func (kv *RaftKV) Apply(msg *raft.ApplyMsg) {
	kv.mu.Lock()

	op := msg.Command.(Op)

	if kv.client_last_op[op.Client] >= op.Id {
		kv.logger.Printf("Duplicated op: %v, ignore\n", op)
	} else {
		switch op.Type {
		case OP_PUT:
			kv.data[op.Key] = op.Value
		case OP_APPEND:
			kv.data[op.Key] = kv.data[op.Key] + op.Value
		default:
		}
		kv.client_last_op[op.Client] = op.Id
	}

	this_pending := kv.pendingOps[msg.Index]
	delete(kv.pendingOps, msg.Index)

	kv.mu.Unlock()

	kv.logger.Printf("Applying command at %v, pending ops: %v\n", msg.Index, len(this_pending))
	sorter := PendingOpsSorter{this_pending}
	sort.Sort(&sorter)

	for _, x := range this_pending {
		if x.op.Client == op.Client && x.op.Id == op.Id {
			kv.logger.Printf("Pending op: %v, success\n", x.op)
			x.c <- true
		} else {
			kv.logger.Printf("Pending op: %v, fail\n", x.op)
			x.c <- false
		}
	}
}

func (kv *RaftKV) Exec(op Op, reply *OpReply) {
	kv.mu.Lock()

	kv.logger.Printf("Exec: %v\n", op)

	op_index, _, is_leader := kv.rf.Start(op)
	if !is_leader {
		reply.Status = STATUS_WRONG_LEADER
		kv.logger.Printf("Not leader, return\n")
		kv.mu.Unlock()
		return
	}

	waiter := make(chan bool, 1)
	kv.pendingOps[op_index] = append(kv.pendingOps[op_index], &PendingOp{op: &op, c: waiter})
	kv.logger.Printf("Waiting for index = %v\n", op_index)

	kv.mu.Unlock()

	ok := <-waiter
	if !ok {
		reply.Status = STATUS_WRONG_LEADER
		kv.logger.Printf("Wait not ok, return\n")
		return
	}

	reply.Status = STATUS_OK
	if op.Type == OP_GET {
		reply.Value = kv.data[op.Key] // "" if not exists
	}
	kv.logger.Printf("Exec return: %v\n", reply)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(OpReply{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.pendingOps = make(map[int][]*PendingOp)
	kv.data = make(map[string]string)
	kv.client_last_op = make(map[int64]int64)
	kv.logger = log.New(os.Stderr, fmt.Sprintf("[KVNode %v] ", me), log.LstdFlags)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for {
			msg := <-kv.applyCh
			go kv.Apply(&msg)
		}
	}()

	return kv
}
