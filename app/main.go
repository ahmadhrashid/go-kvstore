package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	// Database
	db        = make(map[string]string)
	expiryDB  = make(map[string]time.Time)
	streams   = make(map[string][]streamEntry)
	streamsMu sync.Mutex

	// Configuration
	dbfilename   = ""
	port         = ""
	replicaof    = ""
	masterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	dirFlag      *string
	isReplica    bool

	// Replication
	replicaConns   = make(map[net.Conn]struct{})
	replicaConnsMu sync.Mutex
	masterOffset   int64
	replicaOffsets = make(map[net.Conn]int64)
	pendingWaits   []waitReq
	waitMu         sync.Mutex

	// Blocking XREAD
	waitingXReads     []waitingXRead
	waitingXReadsMu   sync.Mutex
	streamNotifiers   = make(map[string]chan struct{})
	streamNotifiersMu sync.Mutex

	// Lists
	lists = make(map[string][]string)

	// Pub/Sub
	subscribeMode = false
	subscriptions []string
	clients       = make(map[io.Writer]*clientState)
)

type waitReq struct {
	reqOffset int64
	numNeeded int
	done      chan int
	deadline  time.Time
}

type waitingXRead struct {
	conn    io.Writer
	streams []string
	lastIDs []string
	timeout time.Time
	done    chan bool
}

type streamEntry struct {
	ID     string
	Fields map[string]string
}

type clientState struct {
	conn        io.Writer
	subscribed  map[string]struct{}
	inSubscribe bool
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	dirFlag = flag.String("dir", "", "Directory for RDB file")
	dbfilenameFlag := flag.String("dbfilename", "", "RDB filename")
	portFlag := flag.String("port", "6379", "Port to listen on")
	replicaFlag := flag.String("replicaof", "", "Master Redis Server")
	flag.Parse()

	dbfilename = *dbfilenameFlag
	port = *portFlag
	replicaof = *replicaFlag
	if replicaof == "" {
		masterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	}

	fmt.Printf("Using dir: %s, dbfilename: %s, port: %s\n", *dirFlag, dbfilename, port)

	if *dirFlag != "" && dbfilename != "" {
		fmt.Println("Loading RDB file from ", *dirFlag, "/", dbfilename)
		rdbPath := *dirFlag + "/" + dbfilename
		if _, err := os.Stat(rdbPath); err == nil {
			fmt.Println("RDB file found")
			kv, exp, err := LoadRDBFile(rdbPath)
			if err == nil {
				for k, v := range kv {
					db[k] = v
				}
				for k, t := range exp {
					expiryDB[k] = t
				}
			} else {
				fmt.Println("Failed to load RDB:", err)
			}
		}
	}

	// Start listening immediately
	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	// If we're a replica, start the handshake in a goroutine
	if replicaof != "" {
		go connectToMasterAndHandshake(replicaof)
		fmt.Printf("Replica of: %s", replicaof)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Printf("[SERVER] Accepted connection from %v\n", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	var inMulti = false
	var multiQueue [][]string

	clients[conn] = &clientState{
		conn:        conn,
		subscribed:  make(map[string]struct{}),
		inSubscribe: false,
	}

	// Check if we're running as a replica
	isReplica = replicaof != ""

	// Clean up waiting XREAD commands when connection closes
	defer func() {
		waitingXReadsMu.Lock()
		for i := 0; i < len(waitingXReads); {
			if waitingXReads[i].conn == conn {
				// Signal the waiting goroutine to stop
				select {
				case waitingXReads[i].done <- true:
				default:
				}
				// Remove from list
				waitingXReads = append(waitingXReads[:i], waitingXReads[i+1:]...)
			} else {
				i++
			}
		}
		waitingXReadsMu.Unlock()
	}()

	defer handleDisconnect(conn)

	for {
		commands, err := parseRESPArray(reader)
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Client disconnected")
			} else {
				fmt.Println("Error parsing RESP:", err.Error())
			}
			return
		}

		if len(commands) == 0 {
			continue
		}

		command := strings.ToUpper(commands[0])
		state := clients[conn]

		if state.inSubscribe {
			handleSubscribeMode(conn, commands)
			continue
		}

		switch command {
		case "MULTI":
			handleMulti(conn, commands, &inMulti, &multiQueue)
			continue

		case "DISCARD":
			handleDiscard(conn, commands, &inMulti, &multiQueue)
			continue

		case "EXEC":
			handleExec(conn, commands, &inMulti, &multiQueue)
			continue

		default:
			if inMulti {
				// any other command during MULTI just gets queued
				multiQueue = append(multiQueue, commands)
				conn.Write([]byte("+QUEUED\r\n"))
				continue
			}
		}
		switch command {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			handleEcho(conn, commands)
		case "SET":
			handleSet(conn, commands)
		case "GET":
			handleGet(conn, commands)
		case "CONFIG":
			handleConfig(conn, commands)
		case "KEYS":
			handleKeys(conn, commands)
		case "INFO":
			handleInfo(conn)
		case "REPLCONF":
			conn.Write([]byte("+OK\r\n"))
		case "PSYNC":
			handlePSYNC(conn, commands)
			return
		case "WAIT":
			handleWait(conn, commands)
		case "TYPE":
			handleType(conn, commands)
		case "XADD":
			handleXAdd(conn, commands)
		case "XRANGE":
			handleXRange(conn, commands)
		case "XREAD":
			handleXRead(conn, commands)
		case "INCR":
			handleIncr(conn, commands)
		case "RPUSH":
			handleRPush(conn, commands)
		case "LRANGE":
			handleLRange(conn, commands)
		case "LPUSH":
			handleLPush(conn, commands)
		case "LLEN":
			handleLLen(conn, commands)
		case "LPOP":
			handleLPop(conn, commands)
		case "BLPOP":
			handleBLPop(conn, commands)
		case "SUBSCRIBE":
			handleSubscribe(state, commands)
		case "PUBLISH":
			handlePublish(conn, commands)
		case "UNSUBSCRIBE":
			handleUnsubscribe(conn, commands)
		default:
			conn.Write([]byte("-ERR unknown command '" + commands[0] + "'\r\n"))
		}
	}
}
