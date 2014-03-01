package jlog_test

import (
	"github.com/fastly/jlog-go"
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
	// GetMessage once from the one that will be "saved" later
	// (goroutine switches from this goroutine to next *right* after read)
	latererBytesRead, _ := readerLaterer.GetMessage()

	// GetMessage from the other goroutine, consume everything in logs and force a munumap
	bytes, e := reader.GetMessage()
	log.Printf("string: %v", string(bytes))
	for {
		bytesTemp, e := reader.GetMessage()
		if bytesTemp == nil || e != nil {
			break
		}
	}
	for i := 0; i < writeCount; i++ {
		writer.Write([]byte("goodbye"))
	}

	// GetMessage again from other gorotuine. The original bytes would've been saved by now, but latererBytesRead isn't.
	bytes2, e := reader.GetMessage()
	log.Printf("string2: %v", string(bytes2))
	log.Printf("string: %v", string(bytes))
	// Expected they wouldn't be equal.

	// This needs to be "hello"
	log.Printf("nowRead: %v", string(latererBytesRead))
}
