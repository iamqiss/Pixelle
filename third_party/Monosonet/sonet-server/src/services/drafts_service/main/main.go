package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "sonet/src/services/drafts_service/proto"
	"sonet/src/services/drafts_service/repository"
	"sonet/src/services/drafts_service/service"
)

func main() {
	// Initialize database connection
	db, err := repository.NewDatabaseConnection()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Initialize repository
	draftsRepo := repository.NewDraftsRepository(db)

	// Initialize service
	draftsService := service.NewDraftsService(draftsRepo)

	// Create gRPC server
	server := grpc.NewServer()

	// Register service
	pb.RegisterDraftsServiceServer(server, draftsService)

	// Enable reflection for debugging
	reflection.Register(server)

	// Start server
	port := os.Getenv("DRAFTS_SERVICE_PORT")
	if port == "" {
		port = "9100"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("Drafts service starting on port %s", port)

	// Start server in goroutine
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down drafts service...")
	server.GracefulStop()
}