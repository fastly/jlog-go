package jlog_test

import (
	"github.com/fastly/jlog"
	"log"
	"testing"
)

func TestReading(t *testing.T) {
	writeCount := 1000000
	initialize(t)

	writer, _ := jlog.NewWriter(pathname, nil)
	writer.AddSubscriber("reading", jlog.BEGIN)
	writer.AddSubscriber("readlater", jlog.BEGIN)
	e := writer.Open()
	if e != nil {
		t.Errorf("cannot open for writing, %v", writer.ErrString())
	}
	for i := 0; i < writeCount; i++ {
		writer.Write([]byte("hello"))
	}

	// Open both readers. They will both point to the start
	reader, _ := jlog.NewReader(pathname, nil)
	reader.Open("reading")

	readerLaterer, _ := jlog.NewReader(pathname, nil)
	readerLaterer.Open("readlater")
	// Read once from the one that will be "saved" later
	// (goroutine switches from this goroutine to next *right* after read)
	latererBytesRead, _ := readerLaterer.Read()

	// Read from the other goroutine, consume everything in logs and force a munumap
	bytes, e := reader.Read()
	log.Printf("string: %v", string(bytes))
	for {
		bytesTemp, e := reader.Read()
		if bytesTemp == nil || e != nil {
			break
		}
	}
	for i := 0; i < writeCount; i++ {
		writer.Write([]byte("goodbye"))
	}

	// Read again from other gorotuine. The original bytes would've been saved by now, but latererBytesRead isn't.
	bytes2, e := reader.Read()
	log.Printf("string2: %v", string(bytes2))
	log.Printf("string: %v", string(bytes))
	// Expected they wouldn't be equal.

	// This needs to be "hello"
	log.Printf("nowRead: %v", string(latererBytesRead))
}
