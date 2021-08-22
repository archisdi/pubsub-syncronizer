package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
)

type Subscriber struct {
	Service    string  `yaml:"service"`
	DeadLetter *int    `yaml:"dead_letter"`
	Deadline   *int    `yaml:"deadline"`
	Retry      *string `yaml:"retry"`
	Ordering   *bool   `yaml:"ordering"`
	PushTo     *string `yaml:"push_to"`
	IsActive   *bool   `yaml:"is_active"`
}

type Event struct {
	Topic       string       `yaml:"topic"`
	Subscribers []Subscriber `yaml:"subscribers"`
	IsActive    bool         `yaml:"is_active"`
}

func (e *Event) Sync() error {
	var topic *pubsub.Topic
	context := context.Background()

	topic = Pubsub.Client.Topic(e.Topic)
	isExist, err := topic.Exists(context)
	if err != nil {
		return err
	}

	if !isExist {
		fmt.Println("creating new topic " + e.Topic)
		_, errCreateTopic := Pubsub.Client.CreateTopic(context, e.Topic)
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

func (s *Subscriber) createConfig(topic *pubsub.Topic) pubsub.SubscriptionConfig {
	config := pubsub.SubscriptionConfig{
		Topic: topic,
	}

	if s.Deadline != nil {
		config.AckDeadline = time.Duration(*s.Deadline)
	}

	if s.Ordering != nil {
		config.EnableMessageOrdering = *s.Ordering
	}

	if s.DeadLetter != nil {
		config.DeadLetterPolicy = &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     "dead-letter",
			MaxDeliveryAttempts: *s.DeadLetter,
		}
	}

	if s.Retry != nil {
		val := strings.Split(*s.Retry, ",")
		min, _ := time.ParseDuration(val[0])
		max, _ := time.ParseDuration(val[1])
		config.RetryPolicy = &pubsub.RetryPolicy{
			MinimumBackoff: min,
			MaximumBackoff: max,
		}
	}

	return config
}

func (s *Subscriber) Sync(topic *pubsub.Topic) error {
	var subscription *pubsub.Subscription
	context := context.Background()
	subName := s.Service + "-" + topic.ID()

	subscription = Pubsub.Client.Subscription(subName)
	isExist, err := subscription.Exists(context)
	if err != nil {
		return err
	}

	if !isExist {
		fmt.Println("creating new subscription " + subName)
		_, errCreateSubs := Pubsub.Client.CreateSubscription(context, subName, s.createConfig(topic))
		if errCreateSubs != nil {
			return errCreateSubs
		}
	} else {
		fmt.Println("subscription " + subName + " exist")
	}

	return nil
}
