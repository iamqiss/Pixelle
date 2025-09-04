package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "sonet/src/services/starterpack_service/proto"
	"sonet/src/services/starterpack_service/repository"
	"sonet/src/services/starterpack_service/service"
)

func main() {
	// Get port from environment or use default
	port := os.Getenv("STARTERPACK_SERVICE_PORT")
	if port == "" {
		port = "9099"
	}

	// Initialize database connection
	dbConn, err := repository.NewDatabaseConnection()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer dbConn.Close()

	// Initialize repository
	starterpackRepo := repository.NewStarterpackRepository(dbConn)

	// Initialize service
	starterpackService := service.NewStarterpackService(starterpackRepo)

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register service
	pb.RegisterStarterpackServiceServer(grpcServer, starterpackService)

	// Enable reflection for debugging
	reflection.Register(grpcServer)

	// Create listener
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Start server
	log.Printf("Starterpack Service starting on port %s", port)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Starterpack Service...")
	grpcServer.GracefulStop()
}