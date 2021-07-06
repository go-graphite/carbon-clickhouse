package main

import (
	"bytes"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"
)

type Verify struct {
	Query  string   `yaml:"query"`
	Output []string `yaml:"output"`
}

func sendPlain(network, address string, input []string) error {
	if conn, err := net.DialTimeout(network, address, time.Second); err != nil {
		return err
	} else {
		for _, m := range input {
			conn.SetDeadline(time.Now().Add(time.Second))
			if _, err = conn.Write([]byte(m + "\n")); err != nil {
				return err
			}
		}
		return conn.Close()
	}
}

func verifyOut(address string, verify Verify) []string {
	var errs []string

	q := []byte(verify.Query)
	req, err := http.NewRequest("POST", "http://"+address+"/", bytes.NewBuffer(q))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return []string{err.Error()}
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []string{err.Error()}
	}
	s := strings.TrimRight(string(body), "\n")
	if resp.StatusCode != 200 {
		return []string{"response status is '" + resp.Status + "', " + s}
	}

	s = strings.ReplaceAll(s, "\t", " ")
	ss := strings.Split(s, "\n")
	if len(ss) == 1 && len(ss[0]) == 0 {
		ss = []string{} /* results is empthy */
	}

	max := len(ss)
	if max < len(verify.Output) {
		max = len(verify.Output)
	}
	for i := 0; i < max; i++ {
		if i >= len(ss) {
			errs = append(errs, "WANT '"+verify.Output[i]+"', GOT -")
		} else if i >= len(verify.Output) {
			errs = append(errs, "WANT '-', GOT '"+ss[i]+"'")
		} else if ss[i] != verify.Output[i] {
			errs = append(errs, "WANT '"+verify.Output[i]+"', GOT '"+ss[i]+"'")
		}
	}
	return errs
}
