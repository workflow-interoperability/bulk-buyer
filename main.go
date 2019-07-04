package main

import (
	"log"
	"time"

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
	go func() {
		receiveReportWorker := client.NewJobWorker().JobType("receiveReport").Handler(worker.ReceiveReportWorker).Open()
		defer receiveReportWorker.Close()
		receiveReportWorker.AwaitClose()
	}()
	go func() {
		signOrderWorker := client.NewJobWorker().JobType("signOrder").Handler(worker.SignOrderWorker).Open()
		defer signOrderWorker.Close()
		signOrderWorker.AwaitClose()
	}()
	var data map[string]interface{}
	for i := 0; i < 100; i++ {
		request, err := client.NewCreateInstanceCommand().BPMNProcessId("bulk-buyer").LatestVersion().VariablesFromMap(data)
		if err != nil {
			log.Println(err)
			return
		}
		request.Send()
		time.Sleep(90 * time.Second)
	}

	<-stopChan
}
