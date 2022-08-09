package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const inputFile = "../access-log.json"
const targetIndex = "bulk-data-test"

func sendBulkRequest(client *http.Client, host, apiKey string, bulkActions []string, withGzip bool) error {
	var buf bytes.Buffer
	var bodyWriter io.Writer

	if withGzip {
		bodyWriter = gzip.NewWriter(&buf)
	} else {
		bodyWriter = &buf
	}

	for _, line := range bulkActions {
		bodyWriter.Write([]byte(fmt.Sprintf("%s\n", line)))
	}

	if t, ok := bodyWriter.(*gzip.Writer); ok {
		t.Close()
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("https://%s/%s/_bulk", host, targetIndex), &buf)
	if err != nil {
		return fmt.Errorf("failed to construct request: %w", err)
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("ApiKey %s", apiKey))

	if withGzip {
		req.Header.Add("Content-Encoding", "gzip")
	}

	fmt.Printf("Content-Length: %vKB\n", float64(req.ContentLength)/(1000))

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed with message: %v", string(body))
	}

	return nil
}

func main() {
	apiKey := os.Getenv("ELASTICSEARCH_APIKEY")
	if apiKey == "" {
		log.Fatal("ELASTICSEARCH_APIKEY must be set")
	}

	esHost := os.Getenv("ELASTICSEARCH_HOST")
	if esHost == "" {
		log.Fatal("ELASTICSEARCH_HOST must be set")
	}

	f, err := os.Open(inputFile)
	if err != nil {
		log.Fatal("Error when opening file: ", err)
	}
	defer f.Close()

	var bulkActions []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		bulkActions = append(bulkActions, "{\"index\": {}}", scanner.Text())
	}

	client := &http.Client{}

	fmt.Println("Sending gzipped bulk request")
	start := time.Now()
	err = sendBulkRequest(client, esHost, apiKey, bulkActions, true)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Took %v\n\n", time.Since(start))

	fmt.Println("Sending regular bulk request")
	start = time.Now()
	err = sendBulkRequest(client, esHost, apiKey, bulkActions, false)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Took %v\n", time.Since(start))
}
