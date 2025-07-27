package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

func handleEcho(conn io.Writer, commands []string) {
	if len(commands) >= 2 {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(commands[1]), commands[1])
		conn.Write([]byte(response))
	} else {
		conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
	}
}

func handleGet(conn io.Writer, commands []string) {
	if len(commands) >= 2 {
		key := commands[1]
		val, exists := getValue(key)
		if !exists {
			conn.Write([]byte("$-1\r\n"))
		} else {
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
			conn.Write([]byte(response))
		}
	} else {
		conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
	}
}

func handleConfig(conn io.Writer, commands []string) {
	if len(commands) != 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'config' command\r\n"))
		return
	}
	key := commands[2]
	if key == "dir" {
		conn.Write([]byte(fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len((*dirFlag)), *dirFlag)))
	} else if key == "dbfilename" {
		conn.Write([]byte(fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(dbfilename), dbfilename)))
	}
}

func handleInfo(conn io.Writer) {
	var response string
	if replicaof == "" {
		masterReplOffset := 0
		response = fmt.Sprintf("role:master master_replid:%s master_repl_offset:%d", masterReplID, masterReplOffset)
	} else {
		response = "role:slave"
	}
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)))
}

func handleType(conn io.Writer, commands []string) {
	if len(commands) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'type' command\r\n"))
		return
	}
	key := commands[1]
	val, exists := getValue(key)
	streamsMu.Lock()
	_, isStream := streams[key]
	streamsMu.Unlock()
	if !exists && !isStream {
		conn.Write([]byte("+none\r\n"))
	} else if isStream {
		conn.Write([]byte("+stream\r\n"))
	} else {
		conn.Write([]byte(fmt.Sprintf("+%T\r\n", val)))
	}
}

func handleSet(conn io.Writer, commands []string) {
	if len(commands) >= 3 {
		db[commands[1]] = commands[2]
		if len(commands) == 5 && strings.ToUpper(commands[3]) == "PX" {
			dbExpireTime, err := strconv.Atoi(commands[4])
			if err != nil {
				if !isReplica {
					conn.Write([]byte("-ERR invalid expiration time\r\n"))
				}
				return
			}
			expiryDB[commands[1]] = time.Now().Add(time.Duration(dbExpireTime) * time.Millisecond)
		}
		if !isReplica {
			conn.Write([]byte("+OK\r\n"))
			propagateToReplicas(commands)
			waitMu.Lock()
			masterOffset += int64(len(encodeRESPArray(commands...)))
			waitMu.Unlock()
		}
	} else {
		if !isReplica {
			conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
		}
	}
}

func handleKeys(conn io.Writer, commands []string) {
	if len(commands) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'keys' command\r\n"))
		return
	}

	pattern := commands[1]
	var keys []string
	if pattern == "*" {
		for k := range db {
			keys = append(keys, k)
		}
	}

	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(keys))))
	for _, k := range keys {
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)))
	}
}
