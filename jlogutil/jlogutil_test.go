package jlogutil

import (
	"os"
	"testing"

	"github.com/fastly/jlog-go"
)

const (
	testfile = "testjlog"
	testsub  = "testsub"
)

func TestNewReader(t *testing.T) {
	jwhis, err := NewReader(testfile, testsub)
	if err != nil {
		t.Errorf("unable to new, err: %v", err)
	}

	subs, err := jwhis.ListSubscribers()
	if err != nil {
		t.Errorf("unable to list subs, err: %v", err)
	}

	var found bool
	for _, sub := range subs {
		if sub == testsub {
			found = true
		}
	}

	if !found {
		t.Errorf("unable to find testsub subscriber")
	}

	_, err = NewReader(testfile, testsub)
	if err == nil {
		t.Errorf("should have errored that testsub exists")
	}

	// establish a write after the jlog has been opened
	logverify, err := jlog.NewWriter(testfile, nil)
	if err != nil {
		t.Fatalf("unable to new writer, err: %v", err)
	}
	err = logverify.Open()
	if err != nil {
		t.Fatalf("unable to open new writer, err: %v", err)
	}
	// cleanup after everything
	defer func() {
		jwhis.Close()
		logverify.Close()
		os.RemoveAll(testfile)
	}()

	n, err := logverify.SendMessage([]byte("test"))
	if err != nil {
		t.Errorf("unable to write test message, err: %v", err)
	}
	if n != 4 {
		t.Errorf("amt written (%v) != len(test)", n)
	}

	msg, err := jwhis.GetMessage()
	if err != nil {
		t.Errorf("should have message")
	}
	if string(msg) != "test" {
		t.Errorf("message (%s) not what was written", msg)
	}
}

func TestClose(t *testing.T) {
	jwhis, err := NewReader(testfile, testsub)
	if err != nil {
		t.Errorf("unable to new, err: %v", err)
	}

	jwhis.Close()
	logverify, err := jlog.NewWriter(testfile, nil)
	if err != nil {
		t.Fatalf("unable to new writer, err: %v", err)
	}
	defer func() {
		logverify.Close()
		os.RemoveAll(testfile)
	}()
	subs, err := logverify.ListSubscribers()
	if err != nil {
		t.Errorf("unable to list subs, err: %v", err)
	}
	if len(subs) != 0 {
		t.Errorf("should be no subs on closed testfile", err)
	}
}
