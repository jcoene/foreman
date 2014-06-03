package foreman

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/jcoene/gologger"
)

type Foreman struct {
	addr      string
	consumers []*consumer
	wg        sync.WaitGroup
	exit      chan int
}

type consumer struct {
	nsqConsumer    *nsq.Consumer
	topic, channel string
}

var log = logger.NewDefaultLogger("foreman")

func New(addr string) *Foreman {
	return &Foreman{addr: addr}
}

func (f *Foreman) AddHandler(topic string, channel string, count int, fn func(string, string) nsq.Handler) (r *nsq.Consumer, err error) {
	config := nsq.NewConfig()
	if err = config.Set("max_attempts", uint16(0)); err != nil {
		return
	}
	if err = config.Set("max_in_flight", count); err != nil {
		return
	}
	if r, err = nsq.NewConsumer(topic, channel, config); err != nil {
		return
	}

	log.Info("spawning %d handlers for %s.%s", count, topic, channel)
	r.SetConcurrentHandlers(fn(topic, channel), count)

	f.wg.Add(1)

	f.consumers = append(f.consumers, &consumer{nsqConsumer: r, topic: topic, channel: channel})

	return
}

func (f *Foreman) Run() (err error) {
	for _, c := range f.consumers {
		log.Info("connecting consumer %s.%s to lookupd at %s", c.topic, c.channel, f.addr)
		if err = c.nsqConsumer.ConnectToNSQLookupd(f.addr); err != nil {
			err = errors.New(fmt.Sprintf("error connecting consumer %s.%s to %s: %s", c.topic, c.channel, f.addr, err))
			return
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		for {
			sig := <-sig
			log.Info("received signal: %s", sig)
			for _, c := range f.consumers {
				log.Info("stopping consumer %s.%s", c.topic, c.channel)
				c.nsqConsumer.Stop()
				select {
				case <-c.nsqConsumer.StopChan:
					log.Info("stopped consumer %s.%s", c.topic, c.channel)
					f.wg.Done()
				case <-time.After(1 * time.Minute):
					log.Warn("timeout while stopping consumer %s.%s", c.topic, c.channel)
				}
			}
		}
	}()

	log.Info("waiting on all consumers")
	f.wg.Wait()
	log.Info("all consumers stopped, closing")

	return
}
