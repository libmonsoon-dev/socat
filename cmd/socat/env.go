package main

import (
	"fmt"
	"os"
)

func getEnv(key string) string {
	result := os.Getenv(key)
	if result == "" {
		panic(fmt.Sprintf("environment variable %s not set", key))
	}

	return result
}
