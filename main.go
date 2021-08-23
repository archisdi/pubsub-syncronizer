package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"reypubsub/model"
	"reypubsub/module"
	"strings"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v2"
)

func loadEvents(topic string) ([]model.Event, error) {
	var events []model.Event
	files, _ := ioutil.ReadDir("./events")

	for _, file := range files {
		yamlFile, errYaml := ioutil.ReadFile("./events/" + file.Name())

		// Filter topic to match flag
		if topic != "all" && topic != strings.Split(file.Name(), ".")[0] {
			continue
		}

		if errYaml != nil {
			return nil, errYaml
		}

		var event model.Event
		errMarshal := yaml.Unmarshal(yamlFile, &event)
		if errMarshal != nil {
			return nil, errMarshal
		}
		events = append(events, event)
	}

	return events, nil
}

func logErrorAndExit(err error) {
	log.Fatal(err)
	os.Exit(1)
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
		os.Exit(1)
	}

	if errClient := module.InitializePubsubClient(); errClient != nil {
		logErrorAndExit(errClient)
	}

	topicFlag := flag.String("topic", "all", "to filter topic")
	flag.Parse()

	events, errEvents := loadEvents(*topicFlag)
	if errEvents != nil {
		logErrorAndExit(errEvents)
	}

	// enforce dead letter topic
	deadLetter := model.Event{
		Topic: "dead-letter",
	}
	deadLetter.Sync()

	for _, event := range events {
		if errSync := event.Sync(); errSync != nil {
			logErrorAndExit(errSync)
		}
	}
}
