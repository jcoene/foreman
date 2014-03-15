package foreman

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/bitly/go-nsq"
	"github.com/jcoene/gologger"
)

type Foreman struct {
	addr string
	rs   []*nsq.Reader
	wg   sync.WaitGroup
	exit chan int
}

var log = logger.NewDefaultLogger("foreman")

func New(addr string) *Foreman {
	return &Foreman{addr: addr}
}

func (f *Foreman) AddHandler(topic string, channel string, count int, fn func(string, string, int) nsq.Handler) (r *nsq.Reader, err error) {
	if r, err = nsq.NewReader(topic, channel); err != nil {
		return
	}

	log.Info("spawning %d handlers for %s.%s", count, topic, channel)
	for i := 0; i < count; i++ {
		r.AddHandler(fn(topic, channel, i))
	}

	r.MaxAttemptCount = 0
	r.SetMaxInFlight(count)

	f.wg.Add(1)

	go func(r *nsq.Reader) {
		<-r.ExitChan
		log.Info("reader %s.%s stopped", r.TopicName, r.ChannelName)
		f.wg.Done()
	}(r)

	f.rs = append(f.rs, r)

	return
}

func (f *Foreman) Run() (err error) {
	for _, r := range f.rs {
		log.Info("connecting reader %s.%s to lookupd at %s", r.TopicName, r.ChannelName, f.addr)
		if err = r.ConnectToLookupd(f.addr); err != nil {
			err = errors.New(fmt.Sprintf("error connecting reader %s.%s to %s: %s", r.TopicName, r.ChannelName, f.addr, err))
			return
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		for {
			sig := <-sig
			log.Info("received signal: %s", sig)
			for _, r := range f.rs {
				log.Info("stopping reader %s.%s", r.TopicName, r.ChannelName)
				r.Stop()
			}
		}
	}()

	log.Info("waiting on all readers")
	f.wg.Wait()
	log.Info("all readers stopped, closing")

	return
}
