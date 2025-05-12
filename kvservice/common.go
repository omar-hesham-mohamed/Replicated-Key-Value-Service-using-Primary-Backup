package kvservice

import (
	"crypto/rand"
	"hash/fnv"
	"math/big"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// Add your definitions here.
	ClientId string
	ReqId int
	PrimaryID string

	// Field names should start with capital letters for RPC to work.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// Add your definitions here.
	PrimaryID string

	// wont check dup for get
}

type GetReply struct {
	Err   Err
	Value string
}

// Add your RPC definitions here.
//======================================
type SyncArgs struct {
	KVMap         map[string]string
    LastClientReq map[string]int
	ResponseHistory map[string][]ResponseHistory
}

type SyncReply struct {
	Err   Err
}

type ResponseHistory struct {
    PreviousValue string
    ReqId        int
}
// ======================================

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
