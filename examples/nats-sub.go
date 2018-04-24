// Copyright 2012-2018 The NATS Authors
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

// +build ignore

package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"

	"github.com/nats-io/go-nats"
)

// NOTE: Use tls scheme for TLS, e.g. nats-sub -s tls://demo.nats.io:4443 foo
func usage() {
	log.Fatalf("Usage: nats-sub [-s server] [-t] <subject> \n")
}

func printMsg(m *nats.Msg, i int) {
	log.Printf("[#%d] Received on [%s]: '%s'\n", i, m.Subject, string(m.Data))
}

func main() {
	var (
		urls           string
		showTime       bool
		rootCACertFile string
		clientCertFile string
		clientKeyFile  string
	)
	flag.StringVar(&urls, "s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	flag.BoolVar(&showTime, "t", false, "Display timestamps")
	flag.StringVar(&rootCACertFile, "cacert", "", "Root CA Certificate File")
	flag.StringVar(&clientCertFile, "cert", "", "Client Certificate File")
	flag.StringVar(&clientKeyFile, "key", "", "Client Private key")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	opts := make([]nats.Option, 0)
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		fmt.Printf("Got disconnected!\n")
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		fmt.Printf("Got reconnected to %v!\n", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		fmt.Printf("Connection closed. Reason: %q\n", nc.LastError())
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
	nc, err := nats.Connect(urls, opts...)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	subj, i := args[0], 0

	nc.Subscribe(subj, func(msg *nats.Msg) {
		i += 1
		printMsg(msg, i)
	})
	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on [%s]\n", subj)
	if showTime {
		log.SetFlags(log.LstdFlags)
	}

	runtime.Goexit()
}
