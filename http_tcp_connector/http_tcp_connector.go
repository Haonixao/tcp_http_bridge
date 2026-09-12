package http_tcp_connector

// http_tcp_connector — a package that represents an integration layer with tcp_http_bridge. It translates HTTP requests into a TCP raw stream to the specified backend. For example, it allows connecting to a database via JDBC using the application as a proxy.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

var (
	// RemoteDBAddr — backend address
	RemoteDBAddr = "localhost:5432"

	// Metadata — arbitrary meta-information that can be provided to the client
	Metadata = make(map[string]interface{})

	sessions = make(map[string]*Session)
	mu       sync.RWMutex

	closedSessions = make(map[string]time.Time)
	csMu           sync.Mutex
)

func init() {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			csMu.Lock()
			now := time.Now()
			for id, closedAt := range closedSessions {
				if now.Sub(closedAt) > 10*time.Minute {
					delete(closedSessions, id)
				}
			}
			csMu.Unlock()
		}
	}()
}

// Session represents an active connection between JDBC and the database
type Session struct {
	ID       string
	Conn     net.Conn
	Buffer   chan []byte
	Ctx      context.Context
	Cancel   context.CancelFunc
	LastSeen time.Time
}

func GetSession(id string, createIfMissing bool) (*Session, error) {
	mu.RLock()
	s, ok := sessions[id]
	mu.RUnlock()

	if ok {
		s.LastSeen = time.Now()
		return s, nil
	}

	csMu.Lock()
	if _, closed := closedSessions[id]; closed {
		csMu.Unlock()
		return nil, fmt.Errorf("session already closed")
	}
	csMu.Unlock()

	if !createIfMissing {
		return nil, fmt.Errorf("session not found")
	}

	mu.Lock()
	defer mu.Unlock()

	if s, ok := sessions[id]; ok {
		return s, nil
	}

	conn, err := net.DialTimeout("tcp", RemoteDBAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s = &Session{
		ID:       id,
		Conn:     conn,
		Buffer:   make(chan []byte, 1000), // Увеличено до 1000 пакетов
		Ctx:      ctx,
		Cancel:   cancel,
		LastSeen: time.Now(),
	}

	sessions[id] = s

	go s.readFromDBLoop()

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-s.Ctx.Done():
				return
			case <-ticker.C:
				if time.Since(s.LastSeen) > 5*time.Minute {
					CloseSession(s.ID)
					return
				}
			}
		}
	}()

	return s, nil
}

func (s *Session) readFromDBLoop() {
	defer CloseSession(s.ID)
	const maxBatchSize = 16384
	batchBuf := new(bytes.Buffer)
	var bufMu sync.Mutex
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	pushBatch := func() {
		bufMu.Lock()
		if batchBuf.Len() == 0 {
			bufMu.Unlock()
			return
		}
		data := make([]byte, batchBuf.Len())
		copy(data, batchBuf.Bytes())
		batchBuf.Reset()
		bufMu.Unlock()

		select {
		case s.Buffer <- data:
		case <-s.Ctx.Done():
		default:
			// Буфер переполнен
			log.Printf("[SERVER SESSION %s] Buffer overflow", s.ID)
		}
	}

	go func() {
		for {
			select {
			case <-s.Ctx.Done():
				return
			case <-ticker.C:
				pushBatch()
			}
		}
	}()

	readBuf := make([]byte, 16384)
	for {
		n, err := s.Conn.Read(readBuf)
		if n > 0 {
			bufMu.Lock()
			batchBuf.Write(readBuf[:n])
			full := batchBuf.Len() >= maxBatchSize
			bufMu.Unlock()

			if full {
				pushBatch()
			}
		}

		if err != nil {
			if err != io.EOF {
				log.Printf("[SERVER SESSION %s] DB Read error: %v", s.ID, err)
			}
			pushBatch()
			return
		}
	}
}

func CloseSession(id string) {
	mu.Lock()
	defer mu.Unlock()
	if s, ok := sessions[id]; ok {
		s.Cancel()
		s.Conn.Close()
		delete(sessions, id)
		log.Printf("[SERVER SESSION %s] Session closed", id)

		csMu.Lock()
		closedSessions[id] = time.Now()
		csMu.Unlock()
	}
}

// --- GIN handlers ---

func HandleSendDataGin(c *gin.Context) {
	id := c.Query("id")
	if id == "" {
		c.String(400, "Missing id")
		return
	}

	s, err := GetSession(id, true)
	if err != nil {
		c.String(500, err.Error())
		return
	}

	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.String(400, "Failed to read body")
		return
	}

	_, err = s.Conn.Write(data)
	if err != nil {
		c.String(500, "Failed to write to DB")
		CloseSession(id)
		return
	}

	c.Status(200)
}

func HandleGetDataGin(c *gin.Context) {
	id := c.Query("id")
	if id == "" {
		c.String(400, "Missing id")
		return
	}

	s, err := GetSession(id, true)
	if err != nil {
		c.Status(410)
		return
	}

	// Long Polling
	select {
	case data := <-s.Buffer:
		c.Data(200, "application/octet-stream", data)
	case <-time.After(30 * time.Second):
		c.Status(204)
	case <-s.Ctx.Done():
		select {
		case data := <-s.Buffer:
			c.Data(200, "application/octet-stream", data)
		default:
			c.Status(410) // Gone
		}
	}
}

func HandleGetMetaGin(c *gin.Context) {
	c.JSON(200, Metadata)
}

// --- net/http handlers ---

func HandleSendDataHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Missing id", 400)
		return
	}

	s, err := GetSession(id, true)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", 400)
		return
	}

	_, err = s.Conn.Write(data)
	if err != nil {
		http.Error(w, "Failed to write to DB", 500)
		CloseSession(id)
		return
	}
	w.WriteHeader(200)
}

func HandleGetDataHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Missing id", 400)
		return
	}

	s, err := GetSession(id, true)
	if err != nil {
		w.WriteHeader(410)
		return
	}

	select {
	case data := <-s.Buffer:
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
		w.Write(data)
	case <-time.After(30 * time.Second):
		w.WriteHeader(204)
	case <-s.Ctx.Done():
		select {
		case data := <-s.Buffer:
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(200)
			w.Write(data)
		default:
			w.WriteHeader(410)
		}
	}
}

func HandleGetMetaHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Metadata)
}
