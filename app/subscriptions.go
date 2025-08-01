package main

import (
	"fmt"
	"io"
	"net"
	"strings"
)

func handleSubscribe(state *clientState, commands []string) {
	if len(commands) < 2 {
		fmt.Fprint(state.conn, "-ERR wrong number of arguments for 'subscribe'\r\n")
		return
	}

	state.inSubscribe = true // client now enters subscribe mode

	for _, channel := range commands[1:] {
		state.subscribed[channel] = struct{}{}
		fmt.Fprintf(state.conn, "*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
			len(channel), channel, len(state.subscribed))
	}
}

func handleSubscribeMode(conn io.Writer, commands []string) {
	command := strings.ToUpper(commands[0])

	switch command {
	case "SUBSCRIBE":
		state := clients[conn]
		handleSubscribe(state, commands)
	case "UNSUBSCRIBE":
		return
	case "PSUBSCRIBE":
		return
	case "PUNSUBSCRIBE":
		return
	case "PING":
		handleSubPing(conn, commands)
	case "QUIT":
		return
	default:
		fmt.Fprintf(conn, "-ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n", command)
	}

}

func handleSubPing(conn io.Writer, commands []string) {
	fmt.Fprint(conn, "*2\r\n$4\r\npong\r\n$0\r\n\r\n")
}

func handleDisconnect(conn net.Conn) {
	delete(clients, conn)
}
