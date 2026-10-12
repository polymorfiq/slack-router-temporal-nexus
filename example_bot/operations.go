package example_bot

import (
	"context"
	"nexus_service"

	"github.com/slack-go/slack/slackevents"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
)

const BotServiceName = "example-bot"
const MessageEventOperationName = "message-event"

var MessageEventOperation = temporalnexus.MustNewTemporalOperation(
	temporalnexus.TemporalOperationOptions[slackevents.MessageEvent, nexus_service.MessageOutcome]{
		Name: MessageEventOperationName,
		Start: func(
			ctx context.Context,
			nc temporalnexus.NexusClient,
			evt slackevents.MessageEvent,
			options temporalnexus.StartTemporalOperationOptions,
		) (temporalnexus.TemporalOperationResult[nexus_service.MessageOutcome], error) {
			c := temporalnexus.GetClient(ctx)

			_, err := c.SignalWithStartWorkflow(
				ctx,
				GetUserWorkflowId(evt.User),
				SawMessage,
				evt,
				client.StartWorkflowOptions{
					ID:        GetUserWorkflowId(evt.User),
					TaskQueue: ExampleBotTaskQueue,
				},
				UserMessages,
				UserMessagesState{User: evt.User},
			)
			if err != nil {
				return temporalnexus.NewSyncResult(nexus_service.MessageOutcome{}), err
			}

			return temporalnexus.NewSyncResult(nexus_service.MessageOutcome{}), nil
		},
	})
