package raftkv

import "raftsc"
import "raft"
import "labrpc"
import "encoding/gob"

type RaftKVImpl struct {
	data map[string]string
}

func (kv *RaftKVImpl) ApplyOp(typ raftsc.OpType, data interface{}, dup bool) interface{} {
	op_data := data.(OpData)

	if !dup {
		switch typ {
		case OP_PUT:
			kv.data[op_data.Key] = op_data.Value
		case OP_APPEND:
			kv.data[op_data.Key] = kv.data[op_data.Key] + op_data.Value
		default:
		}
	}

	return kv.data[op_data.Key]
}

func (kv *RaftKVImpl) EncodeSnapshot(enc *gob.Encoder) {
	enc.Encode(kv.data)
}

func (kv *RaftKVImpl) DecodeSnapshot(dec *gob.Decoder) {
	dec.Decode(&kv.data)
}

func (kv *RaftKVImpl) Free() {
	kv.data = nil
}

type RaftKV struct {
	*raftsc.RaftServer
	rf *raft.Raft
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	gob.Register(OpData{})

	kvserver := &RaftKVImpl{data: make(map[string]string)}
	server := raftsc.StartServer("RaftKV", kvserver, servers, me, persister, maxraftstate)
	return &RaftKV{server, server.Raft()}
}
