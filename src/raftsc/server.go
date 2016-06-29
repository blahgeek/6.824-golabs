/*
* @Author: BlahGeek
* @Date:   2016-06-13
* @Last Modified by:   BlahGeek
* @Last Modified time: 2016-06-29
 */

package raftsc

import (
	"bytes"
	"deepcopy"
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
	op     *Op
	result interface{}
	c      chan bool // waiting chan
}

type RaftServerImpl interface {
	ApplyOp(typ OpType, data interface{}, dup bool) interface{}
	EncodeSnapshot(enc *gob.Encoder)
	DecodeSnapshot(dec *gob.Decoder)
	Free()
}

type RaftServer struct {
	RaftServerImpl

	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg

	maxraftstate       int // snapshot if log grows this big
	last_applied_index int

	pendingOps     map[int][]*PendingOp // index -> list of ops
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

func (rs *RaftServer) Apply(msg *raft.ApplyMsg) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if msg.UseSnapshot {
		rs.logger.Printf("Applying snapshot... index=%v\n", msg.Index)
		rs.loadSnapshot(msg.Snapshot)
		return
	}

	op := msg.Command.(Op)
	op_dup := rs.client_last_op[op.Client] >= op.Id
	op_result := rs.ApplyOp(op.Type, op.Data, op_dup)
	if !op_dup {
		rs.client_last_op[op.Client] = op.Id
	}

	this_pending := rs.pendingOps[msg.Index]
	delete(rs.pendingOps, msg.Index)

	rs.logger.Printf("Applying command at %v, pending ops: %v\n", msg.Index, len(this_pending))
	sorter := PendingOpsSorter{this_pending}
	sort.Sort(&sorter)

	for _, x := range this_pending {
		if x.op.Client == op.Client && x.op.Id == op.Id {
			rs.logger.Printf("Pending op: %v, success\n", x.op)
			x.result = deepcopy.Iface(op_result)
			x.c <- true
		} else {
			rs.logger.Printf("Pending op: %v, fail\n", x.op)
			x.c <- false
		}
	}

	rs.last_applied_index = msg.Index

	if rs.maxraftstate > 0 && rs.persister.RaftStateSize() >= rs.maxraftstate {
		rs.logger.Printf("Raft size too large, going to delete old logs\n")
		go rs.SnapshotAndClean()
	}
}

func (rs *RaftServer) SnapshotAndClean() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.last_applied_index == 0 || rs.maxraftstate <= 0 {
		return
	}

	rs.logger.Printf("SnapshotAndClean...\n")
	snapshot := rs.dumpSnapshot()
	go rs.rf.DeleteOldLogs(rs.last_applied_index, snapshot)
}

func (rs *RaftServer) Exec(op Op, reply *OpReply) {
	rs.logger.Printf("Exec: %v\n", op)

	op_index, _, is_leader := rs.rf.Start(op)
	if !is_leader {
		reply.Status = STATUS_WRONG_LEADER
		rs.logger.Printf("Not leader, return\n")
		return
	}

	pending_op := &PendingOp{op: &op, c: make(chan bool, 1)}
	rs.mu.Lock()
	rs.pendingOps[op_index] = append(rs.pendingOps[op_index], pending_op)
	rs.mu.Unlock()
	rs.logger.Printf("Waiting for index = %v\n", op_index)

	var ok bool
	timer := time.NewTimer(EXEC_TIMEOUT)
	select {
	case ok = <-pending_op.c:
	case <-timer.C:
		rs.logger.Printf("Wait timeout...\n")
		ok = false
	}

	if !ok {
		reply.Status = STATUS_WRONG_LEADER
		rs.logger.Printf("Wait not ok, return\n")
		return
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()

	reply.Status = STATUS_OK
	reply.Data = pending_op.result
	rs.logger.Printf("Exec return: %v\n", reply)
}

func (rs *RaftServer) Kill() {
	if !atomic.CompareAndSwapInt32(&rs.killed, 0, 1) {
		return
	}

	// see Raft.Kill()
	rs.rf.Kill()
	rs.mu.Lock()

	// free up memory
	rs.pendingOps = nil
	rs.Free()
}

func (rs *RaftServer) dumpSnapshot() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	rs.EncodeSnapshot(enc)
	enc.Encode(rs.client_last_op)
	return buf.Bytes()
}

func (rs *RaftServer) loadSnapshot(snapshot []byte) {
	buf := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(buf)
	rs.DecodeSnapshot(dec)
	rs.client_last_op = nil
	dec.Decode(&rs.client_last_op)
}

func (rs *RaftServer) Raft() *raft.Raft {
	return rs.rf
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
func StartServer(name string, server_impl RaftServerImpl, servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftServer {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(OpReply{})

	rs := &RaftServer{
		RaftServerImpl: server_impl,
		me:             me,
		maxraftstate:   maxraftstate,
		persister:      persister,
		applyCh:        make(chan raft.ApplyMsg),
		pendingOps:     make(map[int][]*PendingOp),
		client_last_op: make(map[int64]int64),
		logger:         log.New(os.Stderr, fmt.Sprintf("[%v-RaftServer%v] ", name, me), log.LstdFlags),
	}

	rs.rf = raft.Make(servers, me, persister, rs.applyCh)
	rs.rf.SetLoggerPrefix(fmt.Sprintf("%v-RaftServer%v", name, me))

	go func() {
		for {
			msg := <-rs.applyCh
			rs.Apply(&msg)
		}
	}()

	return rs
}
