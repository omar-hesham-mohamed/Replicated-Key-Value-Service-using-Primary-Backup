package kvservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"sysmonitor"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type KVServer struct {
	l           net.Listener
	dead        bool // for testing
	unreliable  bool // for testing
	id          string
	monitorClnt *sysmonitor.Client
	view        sysmonitor.View
	done        sync.WaitGroup
	finish      chan interface{}

	// Add your declarations here.
	kvMap map[string]string
	role int // 0 = primary, 1 = backup, 2 = neither
	lastClientReq map[string]int
	serverLock sync.Mutex

}

func (server *KVServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	server.serverLock.Lock()

	if (server.role == 0 || server.role == 1) && server.view.Primary == args.PrimaryID {

		clientId := args.ClientId
		reqId := args.ReqId
		lastReq, oldClient := server.lastClientReq[clientId]

		key := args.Key
		value := args.Value

		if oldClient{
			if lastReq >= reqId {
				reply.Err = OK
				if args.DoHash {
					reply.PreviousValue = server.kvMap[key]
				}
			}
		} else{

				if server.view.Backup != "" && server.role == 0{
					success := call(server.view.Backup, "KVServer.Put", &args, &reply)
					if !(!success || reply.Err != OK ){ // only process req if backup succeedes
						if args.DoHash {
							oldValue := server.kvMap[key]
							hash := hash(oldValue + value)
							server.kvMap[key] = strconv.Itoa(int(hash)) // value stored as string
							// do we need to check that hash is same in primary and backup?
							reply.PreviousValue = oldValue
						} else{
							server.kvMap[key] = value
						}
						
						server.lastReq[clientId] = reqId
					}
				} else {
					if args.DoHash {
						oldValue := server.kvMap[key]
						hash := hash(oldValue + value)
						server.kvMap[key] = strconv.Itoa(int(hash)) // value stored as string

						reply.PreviousValue = oldValue
					} else{
						server.kvMap[key] = value
					}
					
					server.lastReq[clientId] = reqId
					reply.Err = OK
				}
				
			
		}

	} else {

		reply.Err = ErrWrongServer

	}

	server.serverLock.Unlock()

	return nil
}

func (server *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	server.serverLock.Lock()

	if (server.role == 0 || server.role == 1) && server.view.Primary == args.PrimaryID {

		key := args.Key
		value, exists := server.kvMap[key]

		if server.view.Backup != "" && server.role == 0{
			success := call(server.view.Backup, "KVServer.Get", &args, &reply)
			if !(!success || reply.Err != OK ){ // only process req if backup succeedes
				if !exists {
					reply.Err = ErrNoKey
					reply.Value = ""
				} else {
					reply.Err = OK
					reply.valid = value
				}
			}
		} else {
			if !exists {
				reply.Err = ErrNoKey
				reply.Value = ""
			} else {
				reply.Err = OK
				reply.valid = value
			}
		}
	} else {

		reply.Err = ErrWrongServer

	}

	server.serverLock.Unlock()
	return nil
}

func (server *KVServer) Sync(args *SyncArgs, reply *SyncReply) error {
	server.serverLock.Lock()

	if server.role != 1 {
        reply.Err = ErrWrongServer
        return nil
    }

	server.kvMap = make(map[string]string)
    for k, v := range args.KVMap {
        server.kvMap[k] = v
    }

	server.lastClientReq = make(map[string]int)
    for clientId, reqId := range args.LastClientReq {
        server.lastClientReq[clientId] = reqId
    }
    
    reply.Err = OK
	server.serverLock.Unlock()

    return nil
}

// ping the view server periodically.
func (server *KVServer) tick() {

	// This line will give an error initially as view and err are not used.
	view, err := server.monitorClnt.Ping(server.view.Viewnum)

	// Your code here.

	if err != nil {
		return
	}

	server.serverLock.Lock()

	if view.Vienum != server.view.Vienum {
		prevRole := server.role // do we need to check if i was primary and became backup?

		if view.Primary == server.id {
			server.role = 0
		} else if view.Backup == server.id {
			server.role = 1
		} else {
			server.role = 2
		}

		// check if primary has new backup
		if server.Role == 0 && view.Backup != "" && view.Backup != server.view.Backup {
			args := &SyncArgs{ KVMap:server.kvMap, LastClientReq: server.lastClientReq,}
            var reply SyncReply
            
            success := call(view.Backup, "KVServer.Sync", args, &reply)
			// what if sync fails?
		}
		
	}

	server.view = view

}

// tell the server to shut itself down.
// please do not change this function.
func (server *KVServer) Kill() {
	server.dead = true
	server.l.Close()
}

func StartKVServer(monitorServer string, id string) *KVServer {
	server := new(KVServer)
	server.id = id
	server.monitorClnt = sysmonitor.MakeClient(id, monitorServer)
	server.view = sysmonitor.View{}
	server.finish = make(chan interface{})

	// Add your server initializations here
	// ==================================
	server.kvMap = make(map[string]string)
	server.lastClientReq = make(map[string]int)
	//====================================

	rpcs := rpc.NewServer()
	rpcs.Register(server)

	os.Remove(server.id)
	l, e := net.Listen("unix", server.id)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	server.l = l

	// please do not make any changes in the following code,
	// or do anything to subvert it.

	go func() {
		for server.dead == false {
			conn, err := server.l.Accept()
			if err == nil && server.dead == false {
				if server.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if server.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				} else {
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && server.dead == false {
				fmt.Printf("KVServer(%v) accept: %v\n", id, err.Error())
				server.Kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", server.id)
		server.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(server.finish)
	}()

	server.done.Add(1)
	go func() {
		for server.dead == false {
			server.tick()
			time.Sleep(sysmonitor.PingInterval)
		}
		server.done.Done()
	}()

	return server
}
