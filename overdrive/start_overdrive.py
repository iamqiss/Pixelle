#!/usr/bin/env python3
"""
Startup script for Overdrive ML services.
"""

import subprocess
import sys
import time
import signal
import os
from pathlib import Path

def run_service(name: str, command: list, cwd: str = None):
    """Run a service and return the process."""
    print(f"Starting {name}...")
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"✅ {name} started with PID {process.pid}")
        return process
    except Exception as e:
        print(f"❌ Failed to start {name}: {e}")
        return None

def check_service_health(service_name: str, health_url: str, max_retries: int = 30):
    """Check if a service is healthy."""
    import requests
    
    for i in range(max_retries):
        try:
            response = requests.get(health_url, timeout=5)
            if response.status_code == 200:
                print(f"✅ {service_name} is healthy")
                return True
        except:
            pass
        
        if i < max_retries - 1:
            print(f"⏳ Waiting for {service_name} to be ready... ({i+1}/{max_retries})")
            time.sleep(2)
    
    print(f"❌ {service_name} failed health check")
    return False

def main():
    """Main startup function."""
    print("🚀 Starting Overdrive ML Services...")
    print("=" * 50)
    
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    overdrive_dir = script_dir
    overdrive_serving_dir = script_dir.parent / "overdrive-serving"
    
    processes = []
    
    try:
        # 1. Start Redis (if not already running)
        print("\n📦 Checking Redis...")
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, db=0)
            r.ping()
            print("✅ Redis is already running")
        except:
            print("⚠️  Redis not running. Please start Redis manually:")
            print("   docker run -d -p 6379:6379 redis:alpine")
            print("   or: redis-server")
        
        # 2. Start Kafka (if not already running)
        print("\n📦 Checking Kafka...")
        try:
            from confluent_kafka import KafkaProducer
            producer = KafkaProducer({'bootstrap.servers': 'localhost:9092'})
            producer.flush()
            print("✅ Kafka is already running")
        except:
            print("⚠️  Kafka not running. Please start Kafka manually:")
            print("   docker-compose up -d kafka")
            print("   or: kafka-server-start /etc/kafka/server.properties")
        
        # 3. Start Python Overdrive service
        print("\n🐍 Starting Python Overdrive service...")
        python_service = run_service(
            "Python Overdrive Service",
            [sys.executable, "-m", "overdrive.cli", "api"],
            cwd=str(overdrive_dir)
        )
        if python_service:
            processes.append(("Python Overdrive Service", python_service))
        
        # Wait for Python service to be ready
        time.sleep(3)
        if not check_service_health("Python Overdrive", "http://localhost:8088/health"):
            print("❌ Python service failed to start properly")
            return 1
        
        # 4. Start Kafka consumer
        print("\n📨 Starting Kafka consumer...")
        consumer = run_service(
            "Kafka Consumer",
            [sys.executable, "-m", "overdrive.cli", "consumer"],
            cwd=str(overdrive_dir)
        )
        if consumer:
            processes.append(("Kafka Consumer", consumer))
        
        # 5. Start C++ Overdrive serving
        print("\n⚡ Starting C++ Overdrive serving...")
        if overdrive_serving_dir.exists():
            # Build if needed
            build_dir = overdrive_serving_dir / "build"
            if not build_dir.exists():
                print("🔨 Building C++ service...")
                build_dir.mkdir()
                subprocess.run(["cmake", ".."], cwd=build_dir, check=True)
                subprocess.run(["make", "-j4"], cwd=build_dir, check=True)
            
            # Start the service
            cpp_service = run_service(
                "C++ Overdrive Serving",
                ["./overdrive-serving"],
                cwd=str(build_dir)
            )
            if cpp_service:
                processes.append(("C++ Overdrive Serving", cpp_service))
        else:
            print("⚠️  C++ serving directory not found, skipping...")
        
        # 6. Start training (optional)
        print("\n🤖 Starting ML model training...")
        training = run_service(
            "ML Training",
            [sys.executable, "-m", "overdrive.training.train_models", 
             "--data-dir", "./data", "--output-dir", "./models", "--device", "cpu"],
            cwd=str(overdrive_dir)
        )
        if training:
            processes.append(("ML Training", training))
        
        print("\n" + "=" * 50)
        print("🎉 All Overdrive services started!")
        print("\n📊 Service Status:")
        print(f"   Python API: http://localhost:8088")
        print(f"   Health Check: http://localhost:8088/health")
        print(f"   Metrics: http://localhost:8088/metrics")
        print(f"   C++ Serving: gRPC on port 50051")
        print("\n🔧 Overdrive is automatically enabled:")
        print("   • No user configuration needed")
        print("   • x-use-overdrive header automatically sent")
        print("   • Matches TikTok's seamless ML approach")
        print("\n⏹️  Press Ctrl+C to stop all services")
        
        # Keep running until interrupted
        while True:
            time.sleep(1)
            
            # Check if any processes have died
            for name, process in processes:
                if process.poll() is not None:
                    print(f"❌ {name} has stopped unexpectedly")
                    return 1
    
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down services...")
        
        # Stop all processes
        for name, process in processes:
            print(f"Stopping {name}...")
            try:
                process.terminate()
                process.wait(timeout=5)
                print(f"✅ {name} stopped")
            except subprocess.TimeoutExpired:
                print(f"⚠️  {name} didn't stop gracefully, forcing...")
                process.kill()
                process.wait()
                print(f"✅ {name} force stopped")
        
        print("👋 All services stopped. Goodbye!")
        return 0
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())