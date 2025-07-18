package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	db                 = make(map[string]string)
	expiry_db          = make(map[string]time.Time)
	dir                = ""
	dbfilename         = ""
	port               = ""
	replicaof          = ""
	master_replid      = ""
	master_repl_offset = 0
	replicaConns       = make(map[net.Conn]struct{})
	replicaConnsMu     sync.Mutex
)

func main() {
	fmt.Println("Logs from your program will appear here!")
	// Define flags
	dirFlag := flag.String("dir", "", "Directory for RDB file")
	dbfilenameFlag := flag.String("dbfilename", "", "RDB filename")
	portFlag := flag.String("port", "6379", "Port to listen on")
	replicaFlag := flag.String("replicaof", "", "Master Redis Server")

	// Parse flags
	flag.Parse()

	// Assign to global variables (if you want to keep using them)
	dir = *dirFlag
	dbfilename = *dbfilenameFlag
	port = *portFlag
	replicaof = *replicaFlag
	if replicaof == "" {
		master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	}

	fmt.Printf("Using dir: %s, dbfilename: %s, port: %s\n", dir, dbfilename, port)

	// --- RDB loading for extensibility ---
	if dir != "" && dbfilename != "" {
		fmt.Println("Loading RDB file from ", dir, "/", dbfilename)
		rdbPath := dir + "/" + dbfilename
		if _, err := os.Stat(rdbPath); err == nil {
			fmt.Println("RDB file found")
			kv, exp, err := LoadRDBFile(rdbPath)
			if err == nil {
				for k, v := range kv {
					db[k] = v
				}
				for k, t := range exp {
					expiry_db[k] = t
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
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Check if we're running as a replica
	isReplica := replicaof != ""

	for {
		// Parse the RESP array
		commands, err := parseRESPArray(reader)
		if err != nil && err.Error() == "EOF" {
			fmt.Println("Client disconnected")
			return
		} else if err != nil {
			fmt.Println("Error parsing RESP: ", err.Error())
			return
		}

		if len(commands) == 0 {
			continue
		}

		// Handle commands (case-insensitive)
		command := strings.ToUpper(commands[0])

		switch command {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(commands) >= 2 {
				// Echo back the argument
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(commands[1]), commands[1])
				conn.Write([]byte(response))
			} else {
				// Error: ECHO requires an argument
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
			}
		case "SET":
			if len(commands) >= 3 {
				// Set the key-value pair
				db[commands[1]] = commands[2]
				if len(commands) == 5 && strings.ToUpper(commands[3]) == "PX" {
					db_expire_time, err := strconv.Atoi(commands[4])
					if err != nil {
						if !isReplica {
							conn.Write([]byte("-ERR invalid expiration time\r\n"))
						}
						return
					}
					expiry_db[commands[1]] = time.Now().Add(time.Duration(db_expire_time) * time.Millisecond)

				}
				if !isReplica {
					conn.Write([]byte("+OK\r\n"))
					propagateToReplicas(commands)
				}
			} else {
				// Error: SET requires at least two arguments
				if !isReplica {
					conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				}
			}

		case "GET":
			if len(commands) >= 2 {
				key := commands[1]
				val, exists := db[key]
				expiry, has_expiry := expiry_db[key]
				if has_expiry && time.Now().After(expiry) {
					// Key expired, delete it
					delete(db, key)
					delete(expiry_db, key)
					exists = false // Mark as not existing after deletion
					conn.Write([]byte("$-1\r\n"))
				} else if !exists {
					conn.Write([]byte("$-1\r\n"))
				} else {
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
					conn.Write([]byte(response))
				}
			} else {
				// Error: GET requires an argument
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
			}
		case "CONFIG":
			if len(commands) != 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'redis-cli' command\r\n"))
				continue
			}
			key := commands[2]
			if key == "dir" {
				conn.Write([]byte(fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(dir), dir)))
			} else if key == "dbfilename" {
				conn.Write([]byte(fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(dbfilename), dbfilename)))
			}
		case "KEYS":
			if len(commands) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'keys' command\r\n"))
				continue
			}
			pattern := commands[1]
			keys := make([]string, 0)
			if pattern == "*" {
				for k := range db {
					keys = append(keys, k)
				}
			}
			if len(keys) == 0 {
				conn.Write([]byte("*0\r\n"))
			}
			conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(keys))))
			for _, k := range keys {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)))
			}
		case "INFO":
			response := ""
			if replicaof == "" {
				response = fmt.Sprintf("role:master master_replid:%s master_repl_offset:%d", master_replid, master_repl_offset)
			} else {
				response = "role:slave"
			}
			if replicaof != "" {

			}

			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)))

		case "REPLCONF":
			conn.Write([]byte("+OK\r\n"))
		case "PSYNC":
			// Respond with +FULLRESYNC <REPL_ID> 0\r\n
			response := fmt.Sprintf("+FULLRESYNC %s 0\r\n", master_replid)
			conn.Write([]byte(response))
			// Send empty RDB file as $<length>\r\n<contents>
			var emptyRDB = []byte{
				0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,
				0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72,
				0x06, 0x36, 0x2E, 0x30, 0x2E, 0x31, 0x36,
				0xFF, 0x89, 0x3B, 0xB7, 0x4E, 0xF8, 0x0F, 0x77, 0x19,
			}
			rdbHeader := fmt.Sprintf("$%d\r\n", len(emptyRDB))
			conn.Write([]byte(rdbHeader))
			conn.Write(emptyRDB)
			replicaConnsMu.Lock()
			replicaConns[conn] = struct{}{}
			replicaConnsMu.Unlock()

		case "WAIT":
			msg := fmt.Sprintf(":%d\r\n", len(replicaConns))
			conn.Write([]byte(msg))
		default:
			// Unknown command
			conn.Write([]byte("-ERR unknown command '" + commands[0] + "'\r\n"))
		}
	}
}

func parseRESPArray(reader *bufio.Reader) ([]string, error) {
	// Read the first line to get the array indicator
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	// Remove \r\n
	line = strings.TrimSpace(line)

	// Check if it's an array
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("expected array indicator '*'")
	}

	// Parse the number of elements
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %v", err)
	}

	// Parse each element
	commands := make([]string, count)
	for i := 0; i < count; i++ {
		command, err := parseRESPBulkString(reader)
		if err != nil {
			return nil, err
		}
		commands[i] = command
	}

	return commands, nil
}

func parseRESPBulkString(reader *bufio.Reader) (string, error) {
	// Read the length line
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	// Remove \r\n
	line = strings.TrimSpace(line)

	// Check if it's a bulk string
	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("expected bulk string indicator '$'")
	}

	// Parse the length
	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %v", err)
	}

	// Read the actual string
	data := make([]byte, length)
	_, err = reader.Read(data)
	if err != nil {
		return "", err
	}

	// Read the trailing \r\n
	_, err = reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func connectToMasterAndHandshake(replicaof string) {
	parts := strings.Fields(replicaof)
	if len(parts) != 2 {
		fmt.Println("Invalid --replicaof format. Use: --replicaof <host> <port>")
		os.Exit(1)
	}
	masterHost := parts[0]
	masterPort := parts[1]
	fmt.Printf("Connecting to master at %s:%s...\n", masterHost, masterPort)
	conn, err := net.Dial("tcp", masterHost+":"+masterPort)
	if err != nil {
		fmt.Println("Failed to connect to master:", err)
		os.Exit(1)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Track offset (number of bytes processed)
	offset := 0

	// Send PING
	ping := encodeRESPArray("PING")
	_, err = conn.Write([]byte(ping))
	if err != nil {
		fmt.Println("Failed to send PING to master:", err)
		os.Exit(1)
	}
	// Wait for response
	resp, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read PING response from master:", err)
		os.Exit(1)
	}
	fmt.Printf("Received after PING: %s", resp)

	// Send REPLCONF listening-port
	replconf := encodeRESPArray("REPLCONF", "listening-port", port)
	_, err = conn.Write([]byte(replconf))
	if err != nil {
		fmt.Println("Failed to send REPLCONF to master")
		os.Exit(1)
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read REPLCONF response from master:", err)
		os.Exit(1)
	}
	fmt.Printf("Received after REPLCONF listening-port: %s", resp)

	// Send REPLCONF capa psync2
	replconf = encodeRESPArray("REPLCONF", "capa", "psync2")
	_, err = conn.Write([]byte(replconf))
	if err != nil {
		fmt.Println("Failed to send REPLCONF to master")
		os.Exit(1)
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read REPLCONF capa response from master:", err)
		os.Exit(1)
	}
	fmt.Printf("Received after REPLCONF capa: %s", resp)

	// Send PSYNC ? -1
	psync := encodeRESPArray("PSYNC", "?", "-1")
	_, err = conn.Write([]byte(psync))
	if err != nil {
		fmt.Println("Failed to send PSYNC to master")
		os.Exit(1)
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read PSYNC response from master:", err)
		os.Exit(1)
	}
	fmt.Printf("Received after PSYNC: %s", resp)

	// --- Read the RDB file sent by the master ---
	// Read the bulk string header manually to avoid consuming extra bytes
	rdbHeader, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read RDB header from master:", err)
		os.Exit(1)
	}
	if !strings.HasPrefix(rdbHeader, "$") {
		fmt.Println("Invalid RDB header from master:", rdbHeader)
		os.Exit(1)
	}
	lengthStr := strings.TrimSpace(rdbHeader[1:]) // remove '$' and \r\n
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		fmt.Println("Invalid RDB length from master:", lengthStr)
		os.Exit(1)
	}
	rdbData := make([]byte, length)
	_, err = io.ReadFull(reader, rdbData)
	if err != nil {
		fmt.Println("Failed to read RDB data from master:", err)
		os.Exit(1)
	}
	fmt.Printf("Read RDB file of %d bytes from master\n", length)

	// Now continuously read and process propagated commands from the master
	for {
		// Peek at the next few bytes to see what we're about to read
		peekBytes, err := reader.Peek(10)
		if err != nil {
			fmt.Println("Error peeking at next bytes:", err)
			return
		}
		fmt.Printf("About to read bytes: %q\n", peekBytes)

		commands, err := parseRESPArray(reader)
		if err != nil {
			// If we get EOF or connection closed, that's normal - no more commands
			if err.Error() == "EOF" || strings.Contains(err.Error(), "connection") {
				fmt.Println("No more propagated commands from master")
				return
			}
			fmt.Println("Error reading propagated command from master:", err)
			return
		}
		if len(commands) == 0 {
			continue
		}

		// Calculate the size of the command we just read
		commandSize := len(encodeRESPArray(commands...))
		fmt.Printf("Received propagated command: %v (size: %d bytes)\n", commands, commandSize)

		// Process the command (same logic as handleConnection but without sending responses)
		command := strings.ToUpper(commands[0])
		switch command {
		case "SET":
			if len(commands) >= 3 {
				db[commands[1]] = commands[2]
				if len(commands) == 5 && strings.ToUpper(commands[3]) == "PX" {
					db_expire_time, err := strconv.Atoi(commands[4])
					if err != nil {
						fmt.Println("Invalid expiration time in propagated command:", err)
						continue
					}
					expiry_db[commands[1]] = time.Now().Add(time.Duration(db_expire_time) * time.Millisecond)
				}
				fmt.Printf("Applied propagated SET %s = %s\n", commands[1], commands[2])
			}
		case "REPLCONF":
			if strings.ToUpper(commands[1]) == "GETACK" {
				// Respond with current offset (before processing this GETACK command)
				resp := encodeRESPArray("REPLCONF", "ACK", fmt.Sprintf("%d", offset))
				_, err = conn.Write([]byte(resp))
				if err != nil {
					fmt.Println("Failed to send GETACK response to master")
					os.Exit(1)
				}
				fmt.Printf("Sent REPLCONF ACK %d\n", offset)
			}
		case "PING":
			// Silently process PING commands from master
			fmt.Printf("Received PING from master\n")
		default:
			fmt.Printf("Received propagated command: %s\n", command)
		}

		// Update offset after processing the command
		offset += commandSize
		fmt.Printf("Updated offset to: %d\n", offset)
	}
}

func encodeRESPArray(args ...string) string {
	resp := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	return resp
}

func propagateToReplicas(commands []string) {
	msg := encodeRESPArray(commands...)
	replicaConnsMu.Lock()
	defer replicaConnsMu.Unlock()
	for c := range replicaConns {
		_, err := c.Write([]byte(msg))
		if err != nil {
			// Remove dead connections
			c.Close()
			delete(replicaConns, c)
		}
	}
}
