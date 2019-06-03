package main

import (
	"github.com/workflow-interoperability/bulk-buyer/worker"
	"github.com/zeebe-io/zeebe/clients/go/zbc"
)

const brokerAddr = "127.0.0.1:26500"

func main() {
	client, err := zbc.NewZBClient(brokerAddr)
	if err != nil {
		panic(err)
	}

	stopChan := make(chan bool, 0)

	go func() {
		placeOrderWorker := client.NewJobWorker().JobType("placeOrder").Handler(worker.PlaceOrderWorker).Open()
		defer placeOrderWorker.Close()
		placeOrderWorker.AwaitClose()
	}()
	signOrderWorker := client.NewJobWorker().JobType("signOrder").Handler(worker.SignOrderWorker).Open()
	defer signOrderWorker.Close()
	signOrderWorker.AwaitClose()

	<-stopChan
}
