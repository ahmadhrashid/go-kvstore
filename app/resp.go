package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// parseRESPArray parses a RESP array from the reader
func parseRESPArray(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("expected array indicator '*'")
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %v", err)
	}

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

// parseRESPBulkString parses a RESP bulk string from the reader
func parseRESPBulkString(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	line = strings.TrimSpace(line)
	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("expected bulk string indicator '$'")
	}

	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %v", err)
	}

	data := make([]byte, length)
	_, err = reader.Read(data)
	if err != nil {
		return "", err
	}

	_, err = reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// encodeRESPArray encodes a slice of strings into a RESP array
func encodeRESPArray(args ...string) string {
	resp := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	return resp
}
