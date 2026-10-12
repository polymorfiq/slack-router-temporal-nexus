package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"nexus_service"
	"os"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

func main() {
	botToken := os.Getenv("SLACK_BOT_TOKEN")
	signingSecret := os.Getenv("SLACK_SIGNING_SECRET")

	c, err := client.Dial(envconfig.MustLoadDefaultClientOptions())
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	api := slack.New(botToken, slack.OptionDebug(true), slack.OptionLog(nil))

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("could not read incoming slack body: %v\n", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		sv, err := slack.NewSecretsVerifier(r.Header, signingSecret)
		if err != nil {
			fmt.Printf("could not read incoming slack headers: %v\n", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, err := sv.Write(body); err != nil {
			fmt.Printf("could not check signing secret: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := sv.Ensure(); err != nil {
			fmt.Printf("error writing payload: %v\n", err)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		eventsAPIEvent, err := slackevents.ParseEvent(json.RawMessage(body), slackevents.OptionNoVerifyToken())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		fmt.Println("[INFO] Received event:", eventsAPIEvent.Type)
		switch eventsAPIEvent.Type {
		case slackevents.URLVerification:
			var r *slackevents.ChallengeResponse
			err := json.Unmarshal(body, &r)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text")
			w.Write([]byte(r.Challenge))
		case slackevents.CallbackEvent:
			innerEvent := eventsAPIEvent.InnerEvent
			fmt.Println("[INFO] Received inner event:", innerEvent.Type)
			switch ev := innerEvent.Data.(type) {
			case *slackevents.AppMentionEvent:
				api.PostMessage(ev.Channel, slack.MsgOptionText("Yes, hello.", false))
			case *slackevents.MessageEvent:
				fmt.Printf("[INFO] Message from %s: %s\n", ev.User, ev.Text)
				if len(ev.Blocks.BlockSet) > 0 {
					fmt.Printf("[INFO] Message contains %d block(s):\n", len(ev.Blocks.BlockSet))
					for i, block := range ev.Blocks.BlockSet {
						fmt.Printf("[INFO]   Block %d: type=%s\n", i, block.BlockType())
					}
				}

				workflowId := nexus_service.GetChannelWorkflowId(ev.Channel)
				startOptions := client.StartWorkflowOptions{
					ID:        workflowId,
					TaskQueue: nexus_service.SlackApiTaskQueue,
				}

				_, err := c.SignalWithStartWorkflow(
					context.Background(),
					workflowId,
					nexus_service.MessageEventChannel,
					ev,
					startOptions,
					nexus_service.SlackChannel,
					[]interface{}{
						nexus_service.ChannelState{ChannelID: ev.Channel},
					}...,
				)
				if err != nil {
					log.Fatalln("Error executing SignalWithStartWorkflow:", err)
				}
			}
		}
	})
	fmt.Println("[INFO] Server listening on :3000")
	http.ListenAndServe(":3000", nil)
}
