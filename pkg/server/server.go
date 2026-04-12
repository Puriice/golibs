package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	*http.Server
	Host     string
	Port     string
	Database *pgxpool.Pool
}

func NewServer(host string, port string, db *pgxpool.Pool) *Server {
	address := fmt.Sprintf("%s:%s", host, port)

	return &Server{
		Server: &http.Server{
			Addr: address,
		},
		Host:     host,
		Port:     port,
		Database: db,
	}
}

func (s *Server) Start() {
	go s.ListenAndServe()

	log.Printf("Server listening at %s", s.Addr)

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	<-quit

	if s.Database != nil {
		s.Database.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.Shutdown(ctx)
}
