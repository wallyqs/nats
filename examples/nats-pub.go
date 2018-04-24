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
	"time"

	"github.com/nats-io/go-nats"
)

// NOTE: Use tls scheme for TLS, e.g. nats-pub -s tls://demo.nats.io:4443 foo hello
func usage() {
	log.Fatalf("Usage: nats-pub [-s server (%s)] <subject> <msg> \n", nats.DefaultURL)
}

func main() {
	var (
		urls           string
		forever        bool
		rootCACertFile string
		clientCertFile string
		clientKeyFile  string
	)
	flag.StringVar(&urls, "s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	flag.BoolVar(&forever, "d", false, "Publishes forever until killed")
	flag.StringVar(&rootCACertFile, "cacert", "", "Root CA Certificate File")
	flag.StringVar(&clientCertFile, "cert", "", "Client Certificate File")
	flag.StringVar(&clientKeyFile, "key", "", "Client Private key")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
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
		log.Fatal(err)
	}
	defer nc.Close()

	subj, msg := args[0], []byte(args[1])

	if forever {
		log.Printf("Publishing [%s] : '%s'\n", subj, msg)
		for range time.NewTicker(100 * time.Millisecond).C {
			if !nc.IsConnected() {
				continue
			}

			nc.Publish(subj, msg)
		}
	} else {
		nc.Publish(subj, msg)
		nc.Flush()
	}

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Published [%s] : '%s'\n", subj, msg)
	}
}
