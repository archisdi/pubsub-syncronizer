package main

import "fmt"

type Subscriber struct {
	Service    string `yaml:"service"`
	DeadLetter int    `yaml:"dead_letter"`
	Deadline   int    `yaml:"deadline"`
	Retry      string `yaml:"retry"`
	Ordering   bool   `yaml:"ordering"`
	PushTo     string `yaml:"push_to"`
	IsActive   bool   `yaml:"is_active"`
}

type Event struct {
	Topic       string       `yaml:"topic"`
	Subscribers []Subscriber `yaml:"subscribers"`
	IsActive    bool         `yaml:"is_active"`
}

func (e *Event) Sync() error {

	// check if topic exist
	// if not, create topic

	return e.syncSubscribers()
}

func (e Event) String() string {
	return e.Topic + ", has " + fmt.Sprint(len(e.Subscribers)) + " subscriber(s)"
}

func (e *Event) syncSubscribers() error {
	for _, subscriber := range e.Subscribers {
		if err := subscriber.Sync(e.Topic); err != nil {
			return err
		}
	}
	return nil
}

func (s *Subscriber) Sync(topic string) error {

	// check if subscriber exist
	// if not, create subscriber

	return nil
}
