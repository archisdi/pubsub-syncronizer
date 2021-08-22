package main

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v2"
)

func loadEvents() ([]Event, error) {
	var events []Event
	files, _ := ioutil.ReadDir("./events")

	for _, file := range files {
		yamlFile, errYaml := ioutil.ReadFile("./events/" + file.Name())
		if errYaml != nil {
			return nil, errYaml
		}

		var event Event
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

	if errClient := InitializePubsubClient(); errClient != nil {
		logErrorAndExit(errClient)
	}

	events, errEvents := loadEvents()
	if errEvents != nil {
		logErrorAndExit(errEvents)
	}

	for _, event := range events {
		if errSync := event.Sync(); errSync != nil {
			logErrorAndExit(errSync)
		}
	}
}
