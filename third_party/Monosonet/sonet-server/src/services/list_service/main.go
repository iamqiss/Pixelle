package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "sonet/src/services/list_service/proto"
	"sonet/src/services/list_service/repository"
	"sonet/src/services/list_service/service"
)

func main() {
	// Get port from environment or use default
	port := os.Getenv("LIST_SERVICE_PORT")
	if port == "" {
		port = "9098"
	}

	// Initialize database connection
	dbConn, err := repository.NewDatabaseConnection()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer dbConn.Close()

	// Initialize repository
	listRepo := repository.NewListRepository(dbConn)

	// Initialize service
	listService := service.NewListService(listRepo)

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register service
	pb.RegisterListServiceServer(grpcServer, listService)

	// Enable reflection for debugging
	reflection.Register(grpcServer)

	// Create listener
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Start server
	log.Printf("List Service starting on port %s", port)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down List Service...")
	grpcServer.GracefulStop()
}