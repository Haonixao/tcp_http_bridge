# TCP-over-HTTP Bridge

Tunneling solution designed to proxy raw TCP connections (such as database connections via JDBC) through a standard HTTP API. This tool is particularly useful when a remote database or service is behind a restrictive firewall that only allows HTTP/HTTPS traffic.

## Overview

The project consists of two main components:
1. **Bridge (Client)**: A local TCP server that listens for incoming connections (e.g., from an IDE or database client) and encapsulates their data into HTTP requests.
2. **Connector (Server)**: A Go package designed for seamless integration into your existing remote service (supports both `Gin` and standard `net/http`). It decapsulates the data and proxies it to the actual target (e.g., a PostgreSQL database).

## Key Features

- **Full TCP Tunneling**: Transparently proxies any raw TCP stream through HTTP POST requests.
- **Session Management**: Supports multiple concurrent sessions using UUIDs, allowing multiple database connections through a single bridge.
- **Performance Optimized**: 
    - **Data Batching**: Combines small packets into larger chunks (up to 16KB) or flushes them every 250ms to reduce HTTP overhead.
    - **Long Polling**: The server holds data retrieval requests for up to 30 seconds if no data is available, significantly reducing polling noise.
- **Flexible Configuration**: Use `config.yaml` to define API endpoints, local listening ports, and custom HTTP headers (e.g., for Authorization).
- **Metadata Support**: Built-in `/get_meta` endpoint to share remote server information (version, environment, etc.) with the bridge upon startup.
- **Robustness**: 
    - **Tombstones**: Tracks closed sessions to prevent accidental re-creation.
    - **Auto-Cleanup**: Automatically terminates inactive sessions after 5 minutes.

## Project Structure

- `main.go`: The local bridge application.
- `config.yaml`: Configuration file for the bridge.
- `http_tcp_connector/`: The server-side integration package.
    - `http_tcp_connector.go`: Core logic for session handling and proxying.
- `server_example.go`: A reference implementation using the Gin framework.

## Getting Started

### 1. Configure the Bridge
Edit `config.yaml` to point to your remote service:

```yaml
remote_api: "https://your-service.com/api/remotedb"
local_addr: ":5431"
poll_interval_ms: 250
headers:
  Authorization: "Bearer your-token"
  X-Custom-Header: "value"
```

### 2. Run the Bridge
```bash
go run main.go
```

### 3. Integrate the Connector
Import the package into your server and register the handlers:

```go
import "your-project/http_tcp_connector"

func main() {
    // Set the actual target address
    http_tcp_connector.RemoteDBAddr = "localhost:5432"
    
    // Set arbitrary metadata (optional)
    http_tcp_connector.Metadata["user"] = "user"
    http_tcp_connector.Metadata["password"] = "password"
    http_tcp_connector.Metadata["db"] = "db"

    r := gin.Default()
    r.POST("/api/remote_db/send_data", http_tcp_connector.HandleSendDataGin)
    r.POST("/api/remote_db/get_data", http_tcp_connector.HandleGetDataGin)
    r.GET("/api/remote_db/get_meta", http_tcp_connector.HandleGetMetaGin)
    
    r.Run(":8080")
}
```

## How It Works

1. **Connection**: A client (like DataGrip) connects to `localhost:5431`.
2. **Encapsulation**: The Bridge creates a unique session ID and starts reading data from the TCP socket.
3. **Transmission**: Data is batched and sent via `POST /send_data`. Simultaneously, the Bridge polls `POST /get_data` for responses.
4. **Proxying**: The Connector receives the data, establishes a TCP connection to the real database, and writes the data to it.
5. **Retrieval**: The Connector reads responses from the database, buffers them, and delivers them to the Bridge via the long-polling request.

## Disclaimer
This project is an early MVP. Data loading (for example, the initial collection of information about the database) can be slow if there is a large amount of data. Nevertheless, this small utility performs its task.
