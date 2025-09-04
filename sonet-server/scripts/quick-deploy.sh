#!/bin/bash

# Sonet Quick Production Deployment Script
# This script provides a rapid deployment option for production

set -euo pipefail

# Configuration
PRODUCTION_DIR="/opt/sonet"
CONFIG_DIR="/opt/sonet/config"
LOGS_DIR="/opt/sonet/logs"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Logging
log() { echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

# Check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        error "This script must be run as root"
    fi
}

# Check if setup script was run
check_setup() {
    if [[ ! -d "$PRODUCTION_DIR" ]]; then
        error "Production setup not completed. Run setup-production.sh first."
    fi
    
    if [[ ! -f "$CONFIG_DIR/production.env" ]]; then
        error "Production environment file not found. Run setup-production.sh first."
    fi
}

# Deploy services
deploy_services() {
    log "Deploying Sonet services..."
    
    cd "$PRODUCTION_DIR"
    
    # Start services
    docker-compose -f docker-compose.production.yml up -d
    
    log "Services deployed successfully"
}

# Wait for services to be healthy
wait_for_health() {
    log "Waiting for services to be healthy..."
    
    local timeout=300
    local services=("postgres" "redis" "moderation_service" "api_gateway" "nginx")
    
    while [[ $timeout -gt 0 ]]; do
        local healthy=true
        
        for service in "${services[@]}"; do
            if ! docker-compose -f docker-compose.production.yml ps "$service" | grep -q "healthy"; then
                healthy=false
                break
            fi
        done
        
        if [[ "$healthy" == true ]]; then
            log "All services are healthy"
            return 0
        fi
        
        log "Waiting for services to be healthy... (${timeout}s remaining)"
        sleep 10
        timeout=$((timeout - 10))
    done
    
    error "Services failed to become healthy within timeout"
}

# Run health checks
run_health_checks() {
    log "Running health checks..."
    
    # Check API health
    if curl -f -s "http://localhost:8080/health" > /dev/null; then
        log "API Gateway: OK"
    else
        warning "API Gateway: Failed"
    fi
    
    # Check Nginx health
    if curl -f -s "http://localhost/health" > /dev/null; then
        log "Nginx: OK"
    else
        warning "Nginx: Failed"
    fi
    
    # Check database connectivity
    if docker exec sonet_notegres_prod pg_isready -U sonet_app > /dev/null 2>&1; then
        log "Database: OK"
    else
        warning "Database: Failed"
    fi
    
    log "Health checks completed"
}

# Show deployment status
show_status() {
    log "Deployment Status:"
    echo "=================="
    
    # Service status
    cd "$PRODUCTION_DIR"
    docker-compose -f docker-compose.production.yml ps
    
    echo ""
    echo "Access URLs:"
    echo "============"
    echo "Main Application: https://sonet.com"
    echo "API Gateway: http://localhost:8080"
    echo "Prometheus: http://localhost:9090"
    echo "Grafana: http://localhost:3000 (admin/admin)"
    echo "Kibana: http://localhost:5601"
    
    echo ""
    echo "Useful Commands:"
    echo "================"
    echo "View logs: journalctl -u sonet.service -f"
    echo "Service status: systemctl status sonet.service"
    echo "Restart services: systemctl restart sonet.service"
    echo "Stop services: systemctl stop sonet.service"
}

# Main deployment function
main() {
    log "Starting Sonet quick deployment..."
    
    # Pre-deployment checks
    check_root
    check_setup
    
    # Deploy services
    deploy_services
    
    # Wait for health
    wait_for_health
    
    # Run health checks
    run_health_checks
    
    # Show status
    show_status
    
    log "Quick deployment completed successfully!"
    log "Check the status above and access your monitoring dashboards."
}

# Execute main function
main "$@"