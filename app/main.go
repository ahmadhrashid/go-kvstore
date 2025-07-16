package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var db = make(map[string]string)
var expiry_db = make(map[string]time.Time)
var dir = ""
var dbfilename = ""
var port = ""
var replicaof = ""
var master_replid = ""
var master_repl_offset = 0

func main() {
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
	fmt.Printf("Replica of: %s", replicaof)

	fmt.Println("Logs from your program will appear here!")

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

	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
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
						conn.Write([]byte("-ERR invalid expiration time\r\n"))
						return
					}
					expiry_db[commands[1]] = time.Now().Add(time.Duration(db_expire_time) * time.Millisecond)

				}
				conn.Write([]byte("+OK\r\n"))
			} else {
				// Error: SET requires at least two arguments
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
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
