package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"reflect"

	"github.com/ledgerwatch/erigon/zk/datastream/client"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/nsf/jsondiff"
)

var (
	stream1 = ""
	stream2 = ""

	consoleOptions = jsondiff.DefaultConsoleOptions()
)

func main() {
	ctx := context.Background()
	flag.StringVar(&stream1, "stream1", "", "the first stream to pull data from")
	flag.StringVar(&stream2, "stream2", "", "the second stream to pull data from")
	flag.Parse()

	client1 := client.NewClient(ctx, stream1, 0, 0, 0, 0)
	client2 := client.NewClient(ctx, stream2, 0, 0, 0, 0)

	go client1.StartStreaming()
	go client2.StartStreaming()

	client1.GetProgressAtomic().Store(0)

	data1, err := readFromClient(client1, 5000)
	if err != nil {
		fmt.Printf("error: %v", err)
	}

	data2, err := readFromClient(client2, 5000)
	if err != nil {
		fmt.Printf("error: %v", err)
	}

	for i := 0; i < len(data1); i++ {
		d1 := data1[i]
		d2 := data2[i]
		if !reflect.DeepEqual(d1, d2) {
			d1j, _ := json.Marshal(d1)
			d2j, _ := json.Marshal(d2)
			_, report := jsondiff.Compare(d1j, d2j, &consoleOptions)
			fmt.Printf("error comparing stream at index %v", i)
			fmt.Println(report)
		}
	}

	fmt.Println("test complete...")
}

func readFromClient(client *client.StreamClient, total int) ([]interface{}, error) {
	go func() {
		err := client.ReadAllEntries()
		if err != nil {
			fmt.Printf("error: %v", err)
			return
		}
	}()

	data := make([]interface{}, 0)
	count := 0

	entries := client.GetSliceManager().ConsumeCurrentItems()

	for _, entry := range entries {
		switch entry.(type) {
		case types.FullL2Block:
		case types.GerUpdate:
			data = append(data, entry)
			count++
		default:
		}

		if count == total {
			break
		}
	}

	return data, nil
}
