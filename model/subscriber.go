package model

import (
	"context"
	"fmt"
	"os"
	"reypubsub/module"
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

func (s *Subscriber) createConfig(topic *pubsub.Topic) pubsub.SubscriptionConfig {
	config := pubsub.SubscriptionConfig{
		Topic: topic,
	}

	if s.Deadline != nil {
		ackDeadline := time.Duration(*s.Deadline * int(time.Second))
		config.AckDeadline = ackDeadline
	}

	if s.Ordering != nil {
		config.EnableMessageOrdering = *s.Ordering
	}

	if s.DeadLetter != nil {
		projectId := os.Getenv("PUBSUB_PROJECT_ID")
		config.DeadLetterPolicy = &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     "projects/" + projectId + "/topics/dead-letter",
			MaxDeliveryAttempts: *s.DeadLetter,
		}
	}

	if s.Retry != nil {
		val := strings.Split(*s.Retry, ",")
		min, _ := time.ParseDuration(val[0] + "s")
		max, _ := time.ParseDuration(val[1] + "s")
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

	subscription = module.Pubsub.Client.Subscription(subName)
	isExist, err := subscription.Exists(context)
	if err != nil {
		return err
	}

	if !isExist {
		fmt.Println("creating new subscription " + subName)
		_, errCreateSubs := module.Pubsub.Client.CreateSubscription(context, subName, s.createConfig(topic))
		if errCreateSubs != nil {
			return errCreateSubs
		}
	} else {
		fmt.Println("subscription " + subName + " exist")
	}

	return nil
}
