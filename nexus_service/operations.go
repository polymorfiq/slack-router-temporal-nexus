package nexus_service

import (
	"context"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
)

const SlackServiceName = "slack"
const SubscribeOperationName = "subscribe"
const UnsubscribeOperationName = "unsubscribe"

type SubscribeOutcome struct {
	Subscribed bool `json:"subscribed"`
}

var SubscribeOperation = temporalnexus.MustNewTemporalOperation(
	temporalnexus.TemporalOperationOptions[ChannelSubscriber, SubscribeOutcome]{
		Name: SubscribeOperationName,
		Start: func(
			ctx context.Context,
			nc temporalnexus.NexusClient,
			sub ChannelSubscriber,
			options temporalnexus.StartTemporalOperationOptions,
		) (temporalnexus.TemporalOperationResult[SubscribeOutcome], error) {
			c := temporalnexus.GetClient(ctx)

			_, err := c.SignalWithStartWorkflow(
				ctx,
				GetChannelWorkflowId(sub.Channel),
				SubscribeChannel,
				sub,
				client.StartWorkflowOptions{
					ID:        GetChannelWorkflowId(sub.Channel),
					TaskQueue: SlackApiTaskQueue,
				},
				SlackChannel,
				ChannelState{ChannelID: sub.Channel},
			)
			if err != nil {
				return temporalnexus.NewSyncResult(SubscribeOutcome{}), err
			}

			return temporalnexus.NewSyncResult(SubscribeOutcome{Subscribed: true}), nil
		},
	})

var UnsubscribeOperation = temporalnexus.MustNewTemporalOperation(
	temporalnexus.TemporalOperationOptions[ChannelSubscriber, SubscribeOutcome]{
		Name: UnsubscribeOperationName,
		Start: func(
			ctx context.Context,
			nc temporalnexus.NexusClient,
			sub ChannelSubscriber,
			options temporalnexus.StartTemporalOperationOptions,
		) (temporalnexus.TemporalOperationResult[SubscribeOutcome], error) {
			c := temporalnexus.GetClient(ctx)

			_, err := c.SignalWithStartWorkflow(
				ctx,
				GetChannelWorkflowId(sub.Channel),
				UnsubscribeChannel,
				sub,
				client.StartWorkflowOptions{
					ID:        GetChannelWorkflowId(sub.Channel),
					TaskQueue: SlackApiTaskQueue,
				},
				ChannelState{ChannelID: sub.Channel},
			)
			if err != nil {
				return temporalnexus.NewSyncResult(SubscribeOutcome{}), err
			}

			return temporalnexus.NewSyncResult(SubscribeOutcome{Subscribed: false}), nil
		},
	})
