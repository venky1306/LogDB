package main

import (
	"flag"
	"log"
)

func main() {
	// Parse command line flags
	var configFile string
	flag.StringVar(&configFile, "config", "config.yaml", "Path to config file")
	flag.Parse()

	// Load server configuration from file
	serverConfig, err := LoadServerConfig(configFile)
	if err != nil {
		log.Fatalf("Error loading server config: %v", err)
	}

	// Create a new server instance
	srv, err := NewServer(serverConfig)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}

	// Start the server
	if err := srv.Start(); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
