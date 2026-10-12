package main

import (
	"log"
	"nexus_service"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/worker"
)

func main() {
	// Empty Options defaults to the "default" Namespace.
	c, err := client.Dial(envconfig.MustLoadDefaultClientOptions())
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, nexus_service.SlackApiTaskQueue, worker.Options{})
	w.RegisterWorkflow(nexus_service.SlackChannel)

	service := nexus.NewService(nexus_service.SlackServiceName)
	err = service.Register(nexus_service.SubscribeOperation)
	err = service.Register(nexus_service.UnsubscribeOperation)
	if err != nil {
		log.Fatalln("Unable to register operations", err)
	}
	w.RegisterNexusService(service)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
