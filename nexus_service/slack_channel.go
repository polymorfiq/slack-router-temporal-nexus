package nexus_service

import (
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/slack-go/slack/slackevents"
	"go.temporal.io/sdk/workflow"
)

const SubscribeChannel = "subscribe"
const UnsubscribeChannel = "unsubscribe"
const ContinueAsNewChannel = "continue-as-new"
const MessageEventChannel = "message-event"

func GetChannelWorkflowId(channelId string) string {
	return fmt.Sprintf("channel-%s", channelId)
}

func SlackChannel(ctx workflow.Context, state ChannelState) error {
	selector := workflow.NewSelector(ctx)

	err := workflow.SetQueryHandler(ctx, "subscribers", func() ([]ChannelSubscriber, error) {
		return state.Subscribers, nil
	})
	if err != nil {
		return err
	}

	var signalVal ChannelSubscriber

	doContinueAsNew := false

	for {
		subscribe := workflow.GetSignalChannel(ctx, SubscribeChannel)
		selector.AddReceive(subscribe, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &signalVal)
			fmt.Printf("Subscribing - %v\n", signalVal)
			state.Subscribers = append(state.Subscribers, signalVal)
		})

		unsubscribe := workflow.GetSignalChannel(ctx, UnsubscribeChannel)
		selector.AddReceive(unsubscribe, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &signalVal)
			fmt.Printf("Unsubscribing - %v\n", signalVal)

			filteredSubs := make([]ChannelSubscriber, 0, len(state.Subscribers))
			for _, sub := range state.Subscribers {
				if sub.Endpoint != signalVal.Endpoint || sub.Operation != signalVal.Operation {
					filteredSubs = append(filteredSubs, sub)
				}
			}

			state.Subscribers = filteredSubs
		})

		var currMsgEvent slackevents.MessageEvent
		messageEvent := workflow.GetSignalChannel(ctx, MessageEventChannel)
		selector.AddReceive(messageEvent, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &currMsgEvent)

			fmt.Printf("MessageEvent: %s\n", currMsgEvent.Text)

			for _, sub := range state.Subscribers {
				nexusClient := workflow.NewNexusClient(sub.Endpoint, sub.Service)
				fmt.Printf("Message to sub - %v\n", sub)

				opRef := nexus.NewOperationReference[slackevents.MessageEvent, MessageOutcome](sub.Operation)
				err := nexusClient.ExecuteOperation(ctx, opRef, currMsgEvent, workflow.NexusOperationOptions{})
				if err != nil {
					fmt.Printf("Error executing operation (%v) : %v\n", sub, err)
				}
			}
		})

		continueAsNew := workflow.GetSignalChannel(ctx, ContinueAsNewChannel)
		selector.AddReceive(continueAsNew, func(c workflow.ReceiveChannel, more bool) {
			nothing := false
			c.Receive(ctx, &nothing)

			doContinueAsNew = true
		})

		selector.Select(ctx)

		info := workflow.GetInfo(ctx)
		if info.GetContinueAsNewSuggested() || doContinueAsNew {
			return workflow.NewContinueAsNewError(ctx, SlackChannel, state)
		}
	}
}

type ChannelState struct {
	ChannelID   string
	Subscribers []ChannelSubscriber `json:"subscribers"`
}

type ChannelSubscriber struct {
	Channel   string `json:"channel"`
	Endpoint  string `json:"endpoint"`
	Service   string `json:"service"`
	Operation string `json:"operation"`
}

type EventFilters struct {
	OnChannelMessage bool
	OnThreadMessage  bool
	OnEmoji          bool
	OnBotMessage     bool
}

type MessageOutcome struct{}
