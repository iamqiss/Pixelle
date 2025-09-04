package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "sonet/src/services/user_service_go/proto"
	"sonet/src/services/user_service_go/repository"
	"sonet/src/services/user_service_go/service"
)

func main() {
	// Initialize database connection
	db, err := repository.NewDatabaseConnection()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Initialize repository
	userRepo := repository.NewUserRepository(db)

	// Initialize service
	userService := service.NewUserService(userRepo)

	// Create gRPC server
	server := grpc.NewServer()

	// Register service
	pb.RegisterUserServiceServer(server, userService)

	// Enable reflection for debugging
	reflection.Register(server)

	// Start server
	port := os.Getenv("USER_SERVICE_PORT")
	if port == "" {
		port = "9090"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("User service starting on port %s", port)

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

	log.Println("Shutting down user service...")
	server.GracefulStop()
}