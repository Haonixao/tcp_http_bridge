package main

// tcp_http_bridge — represents a 'tcp to http' connector that uses a remote application as a proxy to connect to a closed resource via raw TCP. For example, it allows establishing a connection to a database through JDBC

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type Config struct {
	RemoteAPI      string            `yaml:"remote_api"`
	LocalAddr      string            `yaml:"local_addr"`
	PollIntervalMS int               `yaml:"poll_interval_ms"`
	Headers        map[string]string `yaml:"headers"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func main() {
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		log.Fatalf("[ERR] Failed to load config: %v", err)
	}

	ln, err := net.Listen("tcp", cfg.LocalAddr)
	if err != nil {
		log.Fatalf("[ERR] Failed to listen on %s: %v", cfg.LocalAddr, err)
	}
	log.Printf("[INFO] Bridge listening on %s -> %s", cfg.LocalAddr, cfg.RemoteAPI)

	if err := fetchAndPrintMeta(cfg); err != nil {
		log.Printf("[WARN] Failed to fetch meta: %v", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[ERR] Accept error: %v", err)
			continue
		}
		go handleConnection(conn, cfg)
	}
}

func handleConnection(tcpConn net.Conn, cfg *Config) {
	defer tcpConn.Close()
	sessionID := uuid.New().String()
	log.Printf("[SESSION %s] New connection from %s", sessionID, tcpConn.RemoteAddr())

	done := make(chan struct{})

	go func() {
		defer close(done)
		const maxBatchSize = 16384
		batchBuf := new(bytes.Buffer)
		var bufMu sync.Mutex
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		sendBatch := func() error {
			bufMu.Lock()
			if batchBuf.Len() == 0 {
				bufMu.Unlock()
				return nil
			}
			data := make([]byte, batchBuf.Len())
			copy(data, batchBuf.Bytes())
			batchBuf.Reset()
			bufMu.Unlock()

			return sendToRemote(sessionID, data, cfg)
		}

		go func() {
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					sendBatch()
				}
			}
		}()

		readBuf := make([]byte, 16384)
		for {
			n, err := tcpConn.Read(readBuf)
			if n > 0 {
				bufMu.Lock()
				batchBuf.Write(readBuf[:n])
				full := batchBuf.Len() >= maxBatchSize
				bufMu.Unlock()

				if full {
					if err := sendBatch(); err != nil {
						log.Printf("[SESSION %s] HTTP Send error: %v", sessionID, err)
						return
					}
				}
			}

			if err != nil {
				if err != io.EOF {
					log.Printf("[SESSION %s] TCP Read error: %v", sessionID, err)
				}
				sendBatch()
				return
			}
		}
	}()

	ticker := time.NewTicker(time.Duration(cfg.PollIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			log.Printf("[SESSION %s] Closing session", sessionID)
			return
		case <-ticker.C:
			data, err := getFromRemote(sessionID, cfg)
			if err != nil {
				log.Printf("[SESSION %s] HTTP Get error: %v", sessionID, err)
				return
			}
			if len(data) > 0 {
				if _, err := tcpConn.Write(data); err != nil {
					log.Printf("[SESSION %s] TCP Write error: %v", sessionID, err)
					return
				}
			}
		}
	}
}

func sendToRemote(sessionID string, data []byte, cfg *Config) error {
	url := fmt.Sprintf("%s/send_data?id=%s", cfg.RemoteAPI, sessionID)
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusGone {
		return fmt.Errorf("session gone")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("remote returned status %d", resp.StatusCode)
	}
	return nil
}

func getFromRemote(sessionID string, cfg *Config) ([]byte, error) {
	url := fmt.Sprintf("%s/get_data?id=%s", cfg.RemoteAPI, sessionID)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode == http.StatusGone {
		return nil, fmt.Errorf("session gone")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote returned status %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func fetchAndPrintMeta(cfg *Config) error {
	url := fmt.Sprintf("%s/get_meta", cfg.RemoteAPI)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("remote returned status %d", resp.StatusCode)
	}

	var meta map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return err
	}

	log.Printf("[INFO] Remote Metadata:")
	for k, v := range meta {
		log.Printf("  - %s: %v", k, v)
	}
	return nil
}
