package main

import (
	"log"

	"tcp_http/http_tcp_connector"

	"github.com/gin-gonic/gin"
)

func main() {
	http_tcp_connector.RemoteDBAddr = "localhost:5432"

	// Set metadata (for example, from the environment) to communicate it to the client via /get_meta
	http_tcp_connector.Metadata["user"] = "user"
	http_tcp_connector.Metadata["password"] = "password"
	http_tcp_connector.Metadata["db"] = "db"

	r := gin.Default()

	remoteDB := r.Group("/api/remote_db")
	{
		remoteDB.POST("/send_data", http_tcp_connector.HandleSendDataGin)
		remoteDB.POST("/get_data", http_tcp_connector.HandleGetDataGin)
		remoteDB.GET("/get_meta", http_tcp_connector.HandleGetMetaGin)
	}

	log.Println("[INFO] Remote Proxy Server started on :8080")
	r.Run(":8080")
}
