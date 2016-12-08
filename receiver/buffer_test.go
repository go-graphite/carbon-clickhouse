package receiver

import "testing"

func TestBuffer(t *testing.T) {
	b := GetBuffer()

	b.Write([]byte("hello"))
	b.Write([]byte("world"))

	if string(b.Body[:b.Used]) != "helloworld" {
		t.FailNow()
	}

	b.Release()
}
