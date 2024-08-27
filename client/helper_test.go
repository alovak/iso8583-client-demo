package client_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
)

func startEchoServer(ctx context.Context) {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		slog.Error("Failed to listen on port 8080", "error", err)
		return
	}

	// Channel to notify goroutines to stop
	done := make(chan struct{})

	// close everything when the context is done
	// so the next gorooutine can return either because of the done channel or
	// the listener is closed
	go func() {
		<-ctx.Done()
		close(done)
		ln.Close()
		slog.Info("Server stopped")
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return // Stop accepting new connections
				default:
					slog.Error("Failed to accept connection", "error", err)
					continue
				}
			}
			go handleConnection(conn)
		}
	}()
}

// just echo back the received data
// until the connection is closed
func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Create a TeeReader that writes the data to a log while echoing it back
	reader := io.TeeReader(conn, logWriter{})

	// Copy the data from the reader (which logs it) back to the connection
	io.Copy(conn, reader)
}

// logWriter implements the io.Writer interface and logs the data it receives
type logWriter struct{}

func (logWriter) Write(p []byte) (n int, err error) {
	slog.Debug("Server received (raw) request data", "data", fmt.Sprintf("%X", p))
	return len(p), nil
}
