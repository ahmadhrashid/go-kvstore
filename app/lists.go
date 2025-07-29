package main

import (
	"fmt"
	"io"
)

func handleRPush(conn io.Writer, commands []string) {
	if len(commands) < 3 {
		conn.Write([]byte("-ERR incorrect number of arguments for RPUSH"))
		return
	}
	key := commands[1]
	for _, val := range commands[2:] {
		lists[key] = append(lists[key], val)
	}
	lists[key] = append(lists[key], commands[2])
	fmt.Fprintf(conn, ":%d\r\n", len(lists[key]))
}
