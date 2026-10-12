package main

import (
	"example_bot"
	"log"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(envconfig.MustLoadDefaultClientOptions())
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, example_bot.ExampleBotTaskQueue, worker.Options{})
	w.RegisterWorkflow(example_bot.UserMessages)
	w.RegisterWorkflow(example_bot.SubscribeToChannels)

	service := nexus.NewService(example_bot.BotServiceName)
	err = service.Register(example_bot.MessageEventOperation)
	if err != nil {
		log.Fatalln("Unable to register operations", err)
	}
	w.RegisterNexusService(service)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
