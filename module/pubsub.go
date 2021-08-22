package module

import (
	"context"
	"os"

	"cloud.google.com/go/pubsub"
)

type PubsubClient struct {
	*pubsub.Client
}

var Pubsub PubsubClient

func InitializePubsubClient() error {
	ctx := context.Background()
	projectID := os.Getenv("PROJECT_ID")

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return err
	}

	Pubsub = PubsubClient{
		client,
	}

	return nil
}
