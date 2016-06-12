package raftkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"os"
	"raft"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const EXEC_TIMEOUT = time.Second * 3

type PendingOp struct {
	op *Op
	c  chan bool // waiting chan
}

type RaftKV struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	pendingOps map[int][]*PendingOp // index -> list of ops

	data           map[string]string
	client_last_op map[int64]int64

	logger *log.Logger
	killed int32
}

type PendingOpsSorter struct {
	ops []*PendingOp
}

func (s *PendingOpsSorter) Len() int           { return len(s.ops) }
func (s *PendingOpsSorter) Swap(i, j int)      { s.ops[i], s.ops[j] = s.ops[j], s.ops[i] }
func (s *PendingOpsSorter) Less(i, j int) bool { return s.ops[i].op.Id < s.ops[j].op.Id }

func (kv *RaftKV) Apply(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if msg.UseSnapshot {
		kv.logger.Printf("Applying snapshot... index=%v\n", msg.Index)
		kv.loadSnapshot(msg.Snapshot)
		return
	}

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

	if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.logger.Printf("Raft size too large, going to delete old logs\n")
		snapshot := kv.dumpSnapshot()
		go kv.rf.DeleteOldLogs(msg.Index, snapshot)
	}
}

func (kv *RaftKV) Exec(op Op, reply *OpReply) {
	kv.logger.Printf("Exec: %v\n", op)

	op_index, _, is_leader := kv.rf.Start(op)
	if !is_leader {
		reply.Status = STATUS_WRONG_LEADER
		kv.logger.Printf("Not leader, return\n")
		return
	}

	waiter := make(chan bool, 1)
	kv.mu.Lock()
	kv.pendingOps[op_index] = append(kv.pendingOps[op_index], &PendingOp{op: &op, c: waiter})
	kv.mu.Unlock()
	kv.logger.Printf("Waiting for index = %v\n", op_index)

	var ok bool
	timer := time.NewTimer(EXEC_TIMEOUT)
	select {
	case ok = <-waiter:
	case <-timer.C:
		kv.logger.Printf("Wait timeout...\n")
		ok = false
	}

	if !ok {
		reply.Status = STATUS_WRONG_LEADER
		kv.logger.Printf("Wait not ok, return\n")
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

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
	if !atomic.CompareAndSwapInt32(&kv.killed, 0, 1) {
		return
	}

	// see Raft.Kill()
	kv.mu.Lock()
	kv.rf.Kill()

	// free up memory
	kv.data = nil
	kv.pendingOps = nil
	// Your code here, if desired.
}

func (kv *RaftKV) dumpSnapshot() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(kv.data)
	enc.Encode(kv.client_last_op)
	return buf.Bytes()
}

func (kv *RaftKV) loadSnapshot(snapshot []byte) {
	buf := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(buf)
	dec.Decode(&kv.data)
	dec.Decode(&kv.client_last_op)
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
	kv.persister = persister

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
			kv.Apply(&msg)
		}
	}()

	return kv
}
