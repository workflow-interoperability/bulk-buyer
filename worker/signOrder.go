package worker

import (
	"encoding/json"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/workflow-interoperability/bulk-buyer/lib"
	"github.com/workflow-interoperability/bulk-buyer/types"
	"github.com/zeebe-io/zeebe/clients/go/entities"
	"github.com/zeebe-io/zeebe/clients/go/worker"
)

// SignOrderWorker receive order
func SignOrderWorker(client worker.JobClient, job entities.Job) {
	processID := "bulk-buyer"
	iesmid := "3"
	jobKey := job.GetKey()
	log.Println("Start sign order " + strconv.Itoa(int(jobKey)))
	payload, err := job.GetVariablesAsMap()
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	request, err := client.NewCompleteJobCommand().JobKey(jobKey).VariablesFromMap(payload)
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}

	// waiting for IM from sender
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:3001", Path: ""}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	defer c.Close()
	for {
		finished := false
		_, msg, err := c.ReadMessage()
		if err != nil {
			log.Println(err)
			lib.FailJob(client, job)
			return
		}
		// check message type and handle
		var structMsg map[string]interface{}
		err = json.Unmarshal(msg, &structMsg)
		if err != nil {
			log.Println(err)
			lib.FailJob(client, job)
			return
		}
		switch structMsg["$class"].(string) {
		case "org.sysu.wf.IMCreatedEvent":
			// get piis
			processData, err := lib.GetIM("http://127.0.0.1:3000/api/IM/" + structMsg["id"].(string))
			if err != nil {
				continue
			}
			if !(processData.Payload.WorkflowRelevantData.From.ProcessInstanceID == payload["fromProcessInstanceID"].(map[string]interface{})["manufacturer"] && processData.Payload.WorkflowRelevantData.To.IESMID == iesmid && processData.Payload.WorkflowRelevantData.To.ProcessInstanceID == payload["processInstanceID"].(string)) {
				continue
			}
			// create piis
			id := lib.GenerateXID()
			newPIIS := types.PIIS{
				ID: id,
				From: types.FromToData{
					ProcessID:         processID,
					ProcessInstanceID: payload["processInstanceID"].(string),
					IESMID:            iesmid,
				},
				To: processData.Payload.WorkflowRelevantData.From,
				SubscriberInformation: types.SubscriberInformation{
					Roles: []string{},
					ID:    "manufacturer",
				},
			}
			pPIIS := types.PublishPIIS{newPIIS}
			body, err := json.Marshal(&pPIIS)
			if err != nil {
				log.Println(err)
				lib.FailJob(client, job)
				return
			}
			err = lib.BlockchainTransaction("http://127.0.0.1:3000/api/PublishPIIS", string(body))
			if err != nil {
				log.Println(err)
				lib.FailJob(client, job)
				return
			}
			finished = true
		}
		if finished {
			log.Println("Send PIIS success.")
			break
		}
	}
	// record end time
	stime := strconv.Itoa(int(time.Now().Unix()))
	stime = payload["processInstanceID"].(string) + "," + stime + "\n"
	f, _ := os.OpenFile("stop.txt", os.O_APPEND|os.O_WRONLY, 0600)
	defer f.Close()
	f.WriteString(stime)
	request.Send()
}
