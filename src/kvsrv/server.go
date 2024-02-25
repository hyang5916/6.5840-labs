package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvStore       map[string]string  // Maps a string key to a string value.
	putIDStore    map[int64]struct{} // Map of put IDs encountered thus far.
	appendIDStore map[int64]string   // Map of append IDs encountered thus far and their return values.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	val := kv.kvStore[args.Key]
	reply.Value = val
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.putIDStore[args.ID]
	if ok {
		return
	}
	kv.putIDStore[args.ID] = struct{}{}
	kv.kvStore[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.appendIDStore[args.ID]
	if ok {
		reply.Value = val
		return
	}
	oldValue := kv.kvStore[args.Key]
	kv.appendIDStore[args.ID] = oldValue
	kv.kvStore[args.Key] = kv.kvStore[args.Key] + args.Value
	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.putIDStore = make(map[int64]struct{})
	kv.appendIDStore = make(map[int64]string)

	return kv
}
