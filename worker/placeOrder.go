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

// PlaceOrderWorker place order
func PlaceOrderWorker(client worker.JobClient, job entities.Job) {
	processID := "bulk-buyer"
	IESMID := "1"
	jobKey := job.GetKey()
	log.Println("Start place order " + strconv.Itoa(int(jobKey)))

	// generate processid
	id := lib.GenerateXID()

	// record start time
	stime := strconv.Itoa(int(time.Now().Unix()))
	stime = id + "," + stime + "\n"
	f, _ := os.OpenFile("start.txt", os.O_APPEND|os.O_WRONLY, 0600)
	defer f.Close()
	f.WriteString(stime)

	// start
	payload, err := job.GetVariablesAsMap()
	if err != nil {
		log.Println(err)
		payload = map[string]interface{}{}
	}
	payload["processInstanceID"] = id
	// create blockchain IM instance
	id = lib.GenerateXID()
	aData, err := json.Marshal(payload)
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
	}
	newIM := types.IM{
		ID: id,
		Payload: types.Payload{
			ApplicationData: types.ApplicationData{
				URL: string(aData),
			},
			WorkflowRelevantData: types.WorkflowRelevantData{
				From: types.FromToData{
					ProcessID:         processID,
					ProcessInstanceID: payload["processInstanceID"].(string),
					IESMID:            IESMID,
				},
				To: types.FromToData{
					ProcessID:         "manufacturer",
					ProcessInstanceID: "-1",
					IESMID:            "1",
				},
			},
		},
		SubscriberInformation: types.SubscriberInformation{
			Roles: []string{},
			ID:    "manufacturer",
		},
	}
	pim := types.PublishIM{IM: newIM}
	body, err := json.Marshal(&pim)
	if err != nil {
		log.Println(err)
		return
	}
	err = lib.BlockchainTransaction("http://127.0.0.1:3000/api/PublishIM", string(body))
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	payload["processID"] = id
	log.Println("Publish IM success")

	// waiting for PIIS from receiver
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:3001", Path: ""}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer c.Close()
	for {
		finished := false
		_, msg, err := c.ReadMessage()
		if err != nil {
			log.Println(err)
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
		case "org.sysu.wf.PIISCreatedEvent":
			if ok, err := publishPIIS(structMsg["id"].(string), &newIM, "manufacturer", c); err != nil {
				continue
			} else if ok {
				payload["fromProcessInstanceID"] = map[string]string{}
				payload["fromProcessInstanceID"].(map[string]string)["manufacturer"] = newIM.Payload.WorkflowRelevantData.To.ProcessInstanceID
				finished = true
				break
			}
		default:
			continue
		}
		if finished {
			log.Println("Publish PIIS success")
			break
		}
	}

	request, err := client.NewCompleteJobCommand().JobKey(jobKey).VariablesFromMap(payload)
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	request.Send()
}
