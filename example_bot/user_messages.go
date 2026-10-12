package example_bot

import (
	"fmt"

	"github.com/slack-go/slack/slackevents"
	"go.temporal.io/sdk/workflow"
)

const SawMessage = "saw-message"

func GetUserWorkflowId(userId string) string {
	return fmt.Sprintf("user-%s", userId)
}

func UserMessages(ctx workflow.Context, state UserMessagesState) error {
	selector := workflow.NewSelector(ctx)

	err := workflow.SetQueryHandler(ctx, "messages", func() ([]string, error) {
		return state.Messages, nil
	})
	if err != nil {
		return err
	}

	var messageEvt slackevents.MessageEvent

	for {
		subscribe := workflow.GetSignalChannel(ctx, SawMessage)
		selector.AddReceive(subscribe, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &messageEvt)
			state.Messages = append(state.Messages, messageEvt.Text)
		})

		selector.Select(ctx)

		info := workflow.GetInfo(ctx)
		if info.GetContinueAsNewSuggested() {
			return workflow.NewContinueAsNewError(ctx, UserMessages, state)
		}
	}
}

type UserMessagesState struct {
	User     string
	Messages []string
}
