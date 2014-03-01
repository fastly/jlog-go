package jlog_test

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/fastly/jlog-go"
	"github.com/twmb/message"
)

var payload string = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
var pcnt int = 1000000
var pathname string

func initialize(t *testing.T) {
	f, e := ioutil.TempFile("/tmp", "gojlogtest.")
	if e != nil {
		t.Errorf("unable to create tempfile")
	}
	pathname = f.Name()
	log.Println(pathname)
	f.Close()
	e = os.Remove(pathname)
	if e != nil {
		t.Errorf("unable to remove tempfile")
	}
	ctx, e := jlog.NewWriter(pathname, nil)
	if e != nil {
		t.Errorf("unable to new, error %s", ctx.ErrString())
	}
	ctx.Close()
}

func usageSubscriber(subscriber string, t *testing.T) {
	log.Println("adding subscriber", subscriber, "to pathname", pathname)
	ctx, _ := jlog.NewWriter(pathname, nil)
	e := ctx.AddSubscriber(subscriber, jlog.BEGIN)
	if e != nil && ctx.Err() != jlog.ERR_SUBSCRIBER_EXISTS {
		t.Errorf("test subscriber, error %s != subscriber exists",
			ctx.ErrString())
	}
}

func assertUsageSubscriber(subscriber string, expectingIt bool, t *testing.T) {
	log.Println("checking subscriber", subscriber)
	ctx, _ := jlog.NewReader(pathname, nil)
	subs, e := ctx.ListSubscribers()
	if e != nil {
		t.Errorf("assert subscriber, error %s", ctx.ErrString())
	}
	for _, v := range subs {
		if subscriber == v {
			if expectingIt {
				return
			} else {
				t.Errorf("found matching subcriber %v but not expecting it",
					subscriber)
			}
		}
	}
	if expectingIt {
		t.Errorf("Unable to find the expected subscriber %v", subscriber)
	}
}

func usageWritePayloads(cnt int, t *testing.T) {
	ctx, _ := jlog.NewWriter(pathname, nil)
	e := ctx.Open()
	if e != nil {
		t.Errorf("Unable to open writer, error %v", ctx.ErrString())
	}
	log.Printf("writing out %d %d byte payloads", cnt, len(payload))
	bytePayload := []byte(payload)
	for i := 0; i < cnt; i++ {
		ctx.Write(bytePayload)
	}
	log.Printf("written")
}

func usageReadCheck(subscriber string, expect int, sizeup bool, t *testing.T) {
	cnt := 0
	ctx, _ := jlog.NewReader(pathname, nil)
	e := ctx.Open(subscriber)
	if e != nil {
		t.Errorf("Unable to open reader, error %v", ctx.ErrString())
	}
	start := ctx.RawSize()
	for {
		b, e := ctx.GetMessage()
		if cnt > pcnt {
			log.Printf("cnt > pcnt, just read %v", string(b))
		}
		if e != nil && e != message.EOMs {
			t.Errorf("Unable to read message, error %v", ctx.ErrString())
			break
		}
		if b == nil {
			break
		}
		cnt++
	}
	if cnt != expect {
		t.Errorf("got wrong read count: %v != expect %v", cnt, expect)
	}
	end := ctx.RawSize()
	if sizeup {
		log.Printf("checking that size increased")
	} else {
		log.Printf("checking that size decreased")
	}
	if sizeup && end < start {
		t.Errorf("size didn't increase as expected")
	}
	if !sizeup && end > start {
		t.Errorf("size didn't decrease as expected")
	}
}

// A test ripped from the java test file.
func TestUsage(t *testing.T) {
	initialize(t)
	usageSubscriber("testing", t)
	assertUsageSubscriber("testing", true, t)
	usageSubscriber("witness", t)
	assertUsageSubscriber("witness", true, t)
	assertUsageSubscriber("badguy", false, t)
	usageWritePayloads(pcnt, t)
	usageReadCheck("witness", pcnt, true, t)
	usageReadCheck("testing", pcnt, false, t)
}
