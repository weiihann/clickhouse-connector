package main

import (
	"context"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/weiihann/clickhouse-connector/golang/clickhouse"
)

func main() {
	// Load .env file (optional - won't fail if file doesn't exist)
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found or error loading .env file")
	}

	// Get the DSN from the environment variable
	dsn := os.Getenv("CLICKHOUSE_DSN")
	if dsn == "" {
		log.Fatal("CLICKHOUSE_DSN environment variable is not set")
	}

	client := clickhouse.New(&clickhouse.Config{
		DSN: dsn,
	})

	ctx := context.Background()

	if err := client.Start(ctx); err != nil {
		log.Fatalf("Failed to start ClickHouse client: %v", err)
	}
	defer client.Stop()

	expBlock := uint64(10000000)

	var err error
	err = client.ExecOnExpiredAccounts(ctx, expBlock, func(address string) {
		log.Printf("Processing account: %s", address)
	})
	if err != nil {
		log.Fatalf("Failed to execute on expired accounts: %v", err)
	}

	err = client.ExecOnExpiredSlots(ctx, expBlock, func(address, slot string) {
		log.Printf("Processing slot: %s for address: %s", slot, address)
	})
	if err != nil {
		log.Fatalf("Failed to execute on expired slots: %v", err)
	}

	log.Println("Processing completed")
}
