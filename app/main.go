package main

import (
	"bufio"
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

func main() {
	fmt.Println("Logs from your program will appear here!")

	if len(os.Args) > 1 {
		if len(os.Args) < 5 {
			fmt.Println("Usage: go run main.go --dir <directory> --dbfilename <filename>")
			os.Exit(1)
		}
		dir = os.Args[2]
		dbfilename = os.Args[4]
	}

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

	l, err := net.Listen("tcp", "0.0.0.0:6379")
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
