package raftkv

import "raftsc"
import "labrpc"

type Clerk struct {
	*raftsc.RaftClient
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ret := ck.Exec(OP_GET, OpData{Key: key})
	return ret.(string)
}
func (ck *Clerk) Put(key string, value string) {
	ck.Exec(OP_PUT, OpData{Key: key, Value: value})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Exec(OP_APPEND, OpData{Key: key, Value: value})
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	c := raftsc.MakeClient(servers, "RaftKV")
	return &Clerk{c}
}
