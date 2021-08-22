package model

import (
	"context"
	"fmt"
	"reypubsub/module"

	"cloud.google.com/go/pubsub"
)

type Event struct {
	Topic       string       `yaml:"topic"`
	Subscribers []Subscriber `yaml:"subscribers"`
	IsActive    bool         `yaml:"is_active"`
}

func (e *Event) Sync() error {
	var topic *pubsub.Topic
	context := context.Background()

	topic = module.Pubsub.Client.Topic(e.Topic)
	isExist, err := topic.Exists(context)
	if err != nil {
		return err
	}

	fmt.Println()
	if !isExist {
		fmt.Println("creating new topic " + e.Topic)
		_, errCreateTopic := module.Pubsub.Client.CreateTopic(context, e.Topic)
		if errCreateTopic != nil {
			return errCreateTopic
		}
	} else {
		fmt.Println("topic " + e.Topic + " exist")
	}

	return e.syncSubscribers(topic)
}

func (e Event) String() string {
	return e.Topic + ", has " + fmt.Sprint(len(e.Subscribers)) + " subscriber(s)"
}

func (e *Event) syncSubscribers(topic *pubsub.Topic) error {
	for _, subscriber := range e.Subscribers {
		if err := subscriber.Sync(topic); err != nil {
			return err
		}
	}
	return nil
}
