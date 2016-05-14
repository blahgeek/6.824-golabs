package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"
import "log"
import "os"
import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	leader  int

	client_id int64
	op_id     int64

	logger *log.Logger
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
	ck.client_id = nrand()
	ck.logger = log.New(os.Stderr, fmt.Sprintf("[Clerk %v]", ck.client_id), log.LstdFlags)
	ck.logger.Printf("New clerk inited\n")
	return ck
}

func (ck *Clerk) exec(op Op) string {
	var reply OpReply
	var ok bool

	op.Client = ck.client_id
	op.Id = atomic.AddInt64(&ck.op_id, 1)

	for {
		ck.logger.Printf("Try executing %v to leader %v\n", op, ck.leader)
		ok = ck.servers[ck.leader].Call("RaftKV.Exec", op, &reply)
		ck.logger.Printf("Exec result: %v\n", reply)
		if !ok || reply.Status == STATUS_WRONG_LEADER {
			ck.leader = int(nrand() % int64(len(ck.servers)))
			ck.logger.Printf("RPC fail(%v) or wrong leader, random pick: %v\n", !ok, ck.leader)
		} else {
			ck.logger.Printf("OK, reply = %v\n", reply)
			return reply.Value
		}
	}
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
	return ck.exec(Op{Type: OP_GET, Key: key})
}
func (ck *Clerk) Put(key string, value string) {
	ck.exec(Op{Type: OP_PUT, Key: key, Value: value})
}
func (ck *Clerk) Append(key string, value string) {
	ck.exec(Op{Type: OP_APPEND, Key: key, Value: value})
}
