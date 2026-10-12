package nexus_service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type SlackTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *SlackTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *SlackTestSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

func TestSlackTestSuite(t *testing.T) {
	suite.Run(t, new(SlackTestSuite))
}

func (s *SlackTestSuite) Test_SlackChannel_ContinuesAsNew() {
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow("continue-as-new", true)
	}, 20*time.Millisecond)

	s.env.ExecuteWorkflow(SlackChannel, ChannelState{})

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	s.NotNil(err)

	var continueAsNewErr *workflow.ContinueAsNewError
	s.True(errors.As(err, &continueAsNewErr))
	s.Equal("SlackChannel", continueAsNewErr.WorkflowType.Name)
}

func (s *SlackTestSuite) Test_SlackChannel_Subscribe() {
	s.env.RegisterDelayedCallback(func() {
		val, err := s.env.QueryWorkflow("subscribers")
		s.NoError(err)

		var subs []ChannelSubscriber
		err = val.Get(&subs)
		s.NoError(err)

		s.Equal(0, len(subs))

	}, 10*time.Millisecond)

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow("subscribe", ChannelSubscriber{
			Endpoint:  "test-sub",
			Operation: "test-op",
			OnEvents:  EventFilters{OnChannelMessage: true},
		})
	}, 20*time.Millisecond)

	s.env.RegisterDelayedCallback(func() {
		val, err := s.env.QueryWorkflow("subscribers")
		s.NoError(err)

		var subs []ChannelSubscriber
		err = val.Get(&subs)
		s.NoError(err)

		s.Equal(1, len(subs))
		s.Equal("test-sub", subs[0].Endpoint)
		s.Equal("test-op", subs[0].Operation)
		s.Equal(true, subs[0].OnEvents.OnChannelMessage)

	}, 30*time.Millisecond)

	s.env.ExecuteWorkflow(SlackChannel, ChannelState{})
}

func (s *SlackTestSuite) Test_SlackChannel_Unsubscribe() {
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow("subscribe", ChannelSubscriber{Endpoint: "test-sub", Operation: "test-op", OnEvents: EventFilters{OnChannelMessage: true}})
		s.env.SignalWorkflow("unsubscribe", ChannelSubscriber{Endpoint: "test-sub", Operation: "test-op", OnEvents: EventFilters{OnChannelMessage: true}})
		val, err := s.env.QueryWorkflow("subscribers")
		s.NoError(err)

		var subs []ChannelSubscriber
		err = val.Get(&subs)
		s.NoError(err)

		s.Equal(len(subs), 0)

	}, 10*time.Millisecond)

	s.env.ExecuteWorkflow(SlackChannel, ChannelState{})
}
