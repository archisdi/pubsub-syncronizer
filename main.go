package main

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub"
)

func main() {
	ctx := context.Background()
	projectID := "reystaging"

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	topic := client.Topic("payment-created")
	isExist, _ := topic.Exists(ctx)

	fmt.Println(isExist)

}
