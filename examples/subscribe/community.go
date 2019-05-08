package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/d1str0/hpfeeds"
	"github.com/olivere/elastic"
)

func main() {
	var (
		host    string
		port    int
		ident   string
		auth    string
		channel string
	)
	flag.StringVar(&host, "host", "mhnbroker.threatstream.com", "target host")
	flag.IntVar(&port, "port", 10000, "hpfeeds port")
	flag.StringVar(&ident, "ident", "mhn-community-v2-ingester", "ident username")
	flag.StringVar(&auth, "secret", "test-secret", "ident secret")
	flag.StringVar(&channel, "channel", "mhn-community-v2.events", "channel to subscribe to")

	flag.Parse()

	hp := hpfeeds.NewClient(host, port, ident, auth)
	hp.Log = true
	messages := make(chan hpfeeds.Message)

	go processPayloads(messages)

	for {
		fmt.Println("Connecting to hpfeeds server.")
		hp.Connect()

		// Subscribe to "flotest" and print everything coming in on it
		hp.Subscribe(channel, messages)

		// Wait for disconnect
		<-hp.Disconnected
		fmt.Println("Disconnected, attempting to reconnect in 10 seconds...")
		time.Sleep(10 * time.Second)
	}
}

type payload struct {
	app string
}

func processPayloads(messages chan hpfeeds.Message) {
	client, err := elastic.NewClient()
	if err != nil {
		log.Fatalf("Error creating new elastic client: %v", err)
	}

	n := 0

	bulkRequest := client.Bulk()

	var p payload
	for mes := range messages {
		n++

		if err = json.Unmarshal(mes.Payload, &p); err != nil {
			log.Printf("Error unmarshaling json: %s\n", err.Error())
			continue
		}
		req := elastic.NewBulkIndexRequest().Index("mhn-" + p.app).UseEasyJSON(true).Doc(mes.Payload)
		bulkRequest = bulkRequest.Add(req)

		if n%100 == 0 {
			ctx := context.Background()
			fmt.Println("Processing batch...")
			_, err = bulkRequest.Do(ctx)
			if err != nil {
				log.Println(err)
				log.Printf("Done with %d records\n", n)
				break
			} else {
				log.Println("Error processing batch: %s\n", err.Error())
			}

		}
	}
}

/*
func writeToLog(payloads []string) {
	fmt.Println("Writing to logfile")

	// open output file
	fo, err := os.OpenFile("/var/log/mhn-community.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()

	// make a write buffer
	w := bufio.NewWriter(fo)
	for _, s := range payloads {

		if _, err := w.Write([]byte(s)); err != nil {
			panic(err)
		}
		if err := w.WriteByte('\n'); err != nil {
			panic(err)
		}
		if err = w.Flush(); err != nil {
			panic(err)
		}
	}
}
*/
