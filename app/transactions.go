package main

import (
	"fmt"
	"net"
	"strconv"
)

func handleIncr(conn net.Conn, commands []string) {
	if len(commands) != 2 {
		conn.Write([]byte("-ERR incorrect number of arguments for INCR"))
	}
	key := commands[1]
	if val, exists := db[key]; exists {
		if num, err := strconv.Atoi(val); err == nil {
			db[key] = strconv.Itoa(num + 1)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", num+1)))
		} else {
			conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
		}

	} else {
		db[key] = "1"
		conn.Write([]byte(":1\r\n"))
	}
}
