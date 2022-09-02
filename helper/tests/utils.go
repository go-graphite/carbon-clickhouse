package tests

import (
	"net"
	"strings"
)

func Max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

func GetFreeTCPPort(name string) (string, error) {
	if len(name) == 0 {
		name = "127.0.0.1:0"
	} else if !strings.Contains(name, ":") {
		name = name + ":0"
	}
	addr, err := net.ResolveTCPAddr("tcp", name)
	if err != nil {
		return name, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return name, err
	}
	defer l.Close()
	return l.Addr().String(), nil
}
