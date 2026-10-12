package example_bot

import (
	"nexus_service"
	"os"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/workflow"
)

var subscriptions = []nexus_service.ChannelSubscriber{
	nexus_service.ChannelSubscriber{Channel: "CJHFD5201", Endpoint: os.Getenv("BOT_NEXUS_ENDPOINT"), Service: BotServiceName, Operation: MessageEventOperationName},
}

func SubscribeToChannels(ctx workflow.Context) error {
	nexusClient := workflow.NewNexusClient(os.Getenv("SLACK_API_NEXUS_ENDPOINT"), nexus_service.SlackServiceName)

	opRef := nexus.NewOperationReference[nexus_service.ChannelSubscriber, nexus_service.SubscribeOutcome](nexus_service.SubscribeOperationName)

	for _, sub := range subscriptions {
		var result nexus_service.SubscribeOutcome
		err := nexusClient.ExecuteOperation(ctx, opRef, sub, workflow.NexusOperationOptions{}).Get(ctx, &result)
		if err != nil {
			return err
		}
	}

	return nil
}
