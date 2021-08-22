package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
		os.Exit(1)
	}

	if errClient := InitializePubsubClient(); errClient != nil {
		log.Fatal(errClient)
		os.Exit(1)
	}
}
