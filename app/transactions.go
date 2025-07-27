package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

func handleIncr(conn io.Writer, commands []string) {
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

func handleMulti(conn net.Conn, commands []string) {
	if inMulti {
		conn.Write([]byte("-ERR MULTI calls can not be nested\r\n"))
	} else {
		inMulti = true
		multiQueue = multiQueue[:0]
		conn.Write([]byte("+OK\r\n"))
	}
}

func handleDiscard(conn net.Conn, commands []string) {
	if !inMulti {
		conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
	} else {
		inMulti = false
		multiQueue = multiQueue[:0]
		conn.Write([]byte("+OK\r\n"))
	}
}

func handleExec(conn net.Conn, commands []string) {
	if !inMulti {
		conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
	} else {
		// execute queued commands
		conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(multiQueue))))
		for _, queued := range multiQueue {
			// here you can call a helper that takes []string and
			// returns its RESP output as []byte, e.g. execOne(queued)
			conn.Write(execOne(queued))
		}
		inMulti = false
		multiQueue = multiQueue[:0]
	}
}

func execOne(commands []string) []byte {
	var buf bytes.Buffer

	switch strings.ToUpper(commands[0]) {
	case "PING":
		buf.Write([]byte("+PONG\r\n"))
	case "ECHO":
		handleEcho(&buf, commands)
	case "SET":
		handleSet(&buf, commands)
	case "GET":
		handleGet(&buf, commands)
	case "CONFIG":
		handleConfig(&buf, commands)
	case "KEYS":
		handleKeys(&buf, commands)
	case "INFO":
		handleInfo(&buf)
	case "REPLCONF":
		buf.Write([]byte("+OK\r\n"))

	case "WAIT":
		handleWait(&buf, commands)
	case "TYPE":
		handleType(&buf, commands)
	case "XADD":
		handleXAdd(&buf, commands)
	case "XRANGE":
		handleXRange(&buf, commands)
	case "XREAD":
		handleXRead(&buf, commands)
	case "INCR":
		handleIncr(&buf, commands)
	default:
		buf.Write([]byte("-ERR unknown command '" + commands[0] + "'\r\n"))
	}
	return buf.Bytes()
}
