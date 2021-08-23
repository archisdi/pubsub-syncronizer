package model

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reypubsub/module"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
)

type Subscriber struct {
	Service    *string `yaml:"service"`
	Name       *string `yaml:"name"`
	DeadLetter *int    `yaml:"dead_letter"`
	Deadline   *int    `yaml:"deadline"`
	Retry      *string `yaml:"retry"`
	Ordering   *bool   `yaml:"ordering"`
	PushTo     *string `yaml:"push_to"`
	IsActive   bool    `yaml:"is_active"`
}

func (s *Subscriber) getDeadLetterPolicy() *pubsub.DeadLetterPolicy {
	projectId := os.Getenv("PUBSUB_PROJECT_ID")
	return &pubsub.DeadLetterPolicy{
		DeadLetterTopic:     "projects/" + projectId + "/topics/dead-letter",
		MaxDeliveryAttempts: *s.DeadLetter,
	}
}

func (s *Subscriber) getRetryPolicy() *pubsub.RetryPolicy {
	val := strings.Split(*s.Retry, ",")
	min, _ := time.ParseDuration(val[0] + "s")
	max, _ := time.ParseDuration(val[1] + "s")
	return &pubsub.RetryPolicy{
		MinimumBackoff: min,
		MaximumBackoff: max,
	}
}

func (s *Subscriber) getAckDeadline() time.Duration {
	return time.Duration(*s.Deadline * int(time.Second))
}

func (s *Subscriber) getSubscriptionName(topicName string) (string, error) {
	if s.Service == nil && s.Name == nil {
		return "", errors.New("subscription name or service must exist")
	}

	if s.Name != nil {
		return *s.Name, nil
	}

	return *s.Service + "-" + topicName, nil
}

func (s *Subscriber) createConfig(topic *pubsub.Topic) pubsub.SubscriptionConfig {
	config := pubsub.SubscriptionConfig{
		Topic:            topic,
		ExpirationPolicy: time.Duration(0), // never expires
	}

	if s.Ordering != nil {
		config.EnableMessageOrdering = *s.Ordering
	}

	if s.Deadline != nil {
		config.AckDeadline = s.getAckDeadline()
	}

	if s.DeadLetter != nil {
		config.DeadLetterPolicy = s.getDeadLetterPolicy()
	}

	if s.Retry != nil {
		config.RetryPolicy = s.getRetryPolicy()
	}

	return config
}

func (s *Subscriber) createUpdateConfig() pubsub.SubscriptionConfigToUpdate {
	config := pubsub.SubscriptionConfigToUpdate{
		ExpirationPolicy: time.Duration(0), // never expires
	}

	if s.Deadline != nil {
		config.AckDeadline = s.getAckDeadline()
	}

	if s.DeadLetter != nil {
		config.DeadLetterPolicy = s.getDeadLetterPolicy()
	}

	if s.Retry != nil {
		config.RetryPolicy = s.getRetryPolicy()
	}

	return config
}

func (s *Subscriber) Sync(topic *pubsub.Topic) error {
	var subscription *pubsub.Subscription
	context := context.Background()

	subscriptionName, err := s.getSubscriptionName(topic.ID())
	if err != nil {
		return err
	}

	subscription = module.Pubsub.Client.Subscription(subscriptionName)
	isExist, err := subscription.Exists(context)
	if err != nil {
		return err
	}

	if !isExist && s.IsActive {
		fmt.Println("creating new subscription " + subscriptionName)
		if _, err := module.Pubsub.Client.CreateSubscription(context, subscriptionName, s.createConfig(topic)); err != nil {
			return err
		}
	} else if isExist && s.IsActive {
		fmt.Print("updating subscription " + subscriptionName)
		if _, err := subscription.Update(context, s.createUpdateConfig()); err != nil {
			fmt.Print(" (skipped)")
		}
		fmt.Println()
	} else if isExist && !s.IsActive {
		fmt.Println("deleting subscription " + subscriptionName)
		if err := subscription.Delete(context); err != nil {
			return err
		}
	}

	return nil
}
