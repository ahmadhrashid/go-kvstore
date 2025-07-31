package main

import (
	"fmt"
	"io"
)

func handleSubscribe(conn io.Writer, commands []string) {
	if len(commands) != 2 {
		fmt.Fprint(conn, "-ERR Invalid number of arguments for SUBSCIRBE")
		return
	}
	channel := commands[1]
	subscriptions = append(subscriptions, channel)
	fmt.Fprintf(conn, "*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
		len(channel), channel, len(subscriptions))
	subscribeMode = true
}
