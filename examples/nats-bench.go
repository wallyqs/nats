// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats/bench"
)

// Some sane defaults
const (
	DefaultNumMsgs     = 100000
	DefaultNumPubs     = 1
	DefaultNumSubs     = 0
	DefaultMessageSize = 128
)

func usage() {
	log.Fatalf("Usage: nats-bench [-s server (%s)] [--tls] [-np NUM_PUBLISHERS] [-ns NUM_SUBSCRIBERS] [-n NUM_MSGS] [-ms MESSAGE_SIZE] [-csv csvfile] <subject>\n", nats.DefaultURL)
}

var benchmark *bench.Benchmark

func main() {
	var (
		urls           string
		tls            bool
		numPubs        int
		numSubs        int
		numMsgs        int
		msgSize        int
		csvFile        string
		rootCACertFile string
		clientCertFile string
		clientKeyFile  string
	)
	flag.StringVar(&urls, "s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	flag.BoolVar(&tls, "tls", false, "Use TLS Secure Connection")
	flag.IntVar(&numPubs, "np", DefaultNumPubs, "Number of Concurrent Publishers")
	flag.IntVar(&numSubs, "ns", DefaultNumSubs, "Number of Concurrent Subscribers")
	flag.IntVar(&numMsgs, "n", DefaultNumMsgs, "Number of Messages to Publish")
	flag.IntVar(&msgSize, "ms", DefaultMessageSize, "Size of the message.")
	flag.StringVar(&csvFile, "csv", "", "Save bench data to csv file")
	flag.StringVar(&rootCACertFile, "cacert", "", "Root CA Certificate File")
	flag.StringVar(&clientCertFile, "cert", "", "Client Certificate File")
	flag.StringVar(&clientKeyFile, "key", "", "Client Private key")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		usage()
	}
	if numMsgs <= 0 {
		log.Fatal("Number of messages should be greater than zero.")
	}

	opts := make([]nats.Option, 0)
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		if nc.LastError() == nil {
			return
		}
		fmt.Printf("Got disconnected!\n")
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		fmt.Printf("Got reconnected to %v!\n", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		if nc.LastError() == nil {
			return
		}
		fmt.Printf("Connection closed. Reason: %v\n", nc.LastError())
	}))
	opts = append(opts, nats.DiscoveredServersHandler(func(nc *nats.Conn) {
		fmt.Printf("Discovered Servers: %+v\n", nc.DiscoveredServers())
	}))
	if len(rootCACertFile) > 0 {
		opts = append(opts, nats.RootCAs(rootCACertFile))
	}
	if len(clientCertFile) > 0 && len(clientKeyFile) > 0 {
		opts = append(opts, nats.ClientCert(clientCertFile, clientKeyFile))
	}

	benchmark = bench.NewBenchmark("NATS", numSubs, numPubs)

	var startwg sync.WaitGroup
	var donewg sync.WaitGroup

	donewg.Add(numPubs + numSubs)

	// Run Subscribers first
	startwg.Add(numSubs)
	for i := 0; i < numSubs; i++ {
		go runSubscriber(&startwg, &donewg, urls, opts, numMsgs, msgSize)
	}
	startwg.Wait()

	// Now Publishers
	startwg.Add(numPubs)
	pubCounts := bench.MsgsPerClient(numMsgs, numPubs)
	for i := 0; i < numPubs; i++ {
		go runPublisher(&startwg, &donewg, urls, opts, pubCounts[i], msgSize)
	}

	log.Printf("Starting benchmark [msgs=%d, msgsize=%d, pubs=%d, subs=%d]\n", numMsgs, msgSize, numPubs, numSubs)

	startwg.Wait()
	donewg.Wait()

	benchmark.Close()

	fmt.Print(benchmark.Report())

	if len(csvFile) > 0 {
		csv := benchmark.CSV()
		ioutil.WriteFile(csvFile, []byte(csv), 0644)
		fmt.Printf("Saved metric data in csv file %s\n", csvFile)
	}
}

func runPublisher(startwg, donewg *sync.WaitGroup, urls string, opts []nats.Option, numMsgs int, msgSize int) {
	nc, err := nats.Connect(urls, opts...)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	defer nc.Close()
	startwg.Done()

	args := flag.Args()
	subj := args[0]
	var msg []byte
	if msgSize > 0 {
		msg = make([]byte, msgSize)
	}

	start := time.Now()

	for i := 0; i < numMsgs; i++ {
		nc.Publish(subj, msg)
	}
	nc.Flush()
	benchmark.AddPubSample(bench.NewSample(numMsgs, msgSize, start, time.Now(), nc))

	donewg.Done()
}

func runSubscriber(startwg, donewg *sync.WaitGroup, urls string, opts []nats.Option, numMsgs int, msgSize int) {
	nc, err := nats.Connect(urls, opts...)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	args := flag.Args()
	subj := args[0]

	received := 0
	start := time.Now()
	nc.Subscribe(subj, func(msg *nats.Msg) {
		received++
		if received >= numMsgs {
			benchmark.AddSubSample(bench.NewSample(numMsgs, msgSize, start, time.Now(), nc))
			donewg.Done()
			nc.Close()
		}
	})
	nc.Flush()
	startwg.Done()
}
