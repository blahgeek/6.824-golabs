/*
* @Author: BlahGeek
* @Date:   2016-06-13
* @Last Modified by:   BlahGeek
* @Last Modified time: 2016-06-30
 */

package raftsc

type OpType int

type Op struct {
	Type OpType
	Data interface{}

	Client int64 // which client sends this OP?
	Id     int64 // unique for this client, monotone increase
}

type StatusType int

const (
	STATUS_OK           StatusType = iota
	STATUS_WRONG_LEADER StatusType = iota
)

type OpReply struct {
	Status StatusType
	Data   interface{}
}
