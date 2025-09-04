#!/bin/bash

# Sonet Production Setup Script
# This script sets up the complete production infrastructure

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SETUP_LOG="/var/log/sonet/setup.log"
PRODUCTION_DIR="/opt/sonet"
BACKUP_DIR="/opt/sonet/backups"
DATA_DIR="/opt/sonet/data"
LOGS_DIR="/opt/sonet/logs"
CONFIG_DIR="/opt/sonet/config"
CERTS_DIR="/opt/sonet/certs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging
log() { echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$SETUP_LOG"; }
error() { echo -e "${RED}[ERROR]${NC} $1" | tee -a "$SETUP_LOG"; exit 1; }
warning() { echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$SETUP_LOG"; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$SETUP_LOG"; }

# Check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        error "This script must be run as root"
    fi
}

# Check system requirements
check_system_requirements() {
    log "Checking system requirements..."
    
    # Check OS
    if [[ ! -f /etc/os-release ]]; then
        error "Unsupported operating system"
    fi
    
    source /etc/os-release
    if [[ "$ID" != "ubuntu" && "$ID" != "debian" ]]; then
        warning "This script is optimized for Ubuntu/Debian. Other systems may require manual configuration."
    fi
    
    # Check available memory
    local mem_total=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    local mem_gb=$((mem_total / 1024 / 1024))
    
    if [[ $mem_gb -lt 8 ]]; then
        error "Minimum 8GB RAM required. Available: ${mem_gb}GB"
    fi
    
    # Check available disk space
    local disk_space=$(df / | awk 'NR==2 {print $4}')
    local disk_gb=$((disk_space / 1024 / 1024))
    
    if [[ $disk_gb -lt 50 ]]; then
        error "Minimum 50GB disk space required. Available: ${disk_gb}GB"
    fi
    
    # Check CPU cores
    local cpu_cores=$(nproc)
    if [[ $cpu_cores -lt 4 ]]; then
        warning "Recommended minimum 4 CPU cores. Available: ${cpu_cores}"
    fi
    
    success "System requirements check passed"
}

# Install system dependencies
install_system_dependencies() {
    log "Installing system dependencies..."
    
    # Update package list
    apt-get update
    
    # Install essential packages
    apt-get install -y \
        curl \
        wget \
        git \
        unzip \
        software-properties-common \
        apt-transport-https \
        ca-certificates \
        gnupg \
        lsb-release \
        htop \
        iotop \
        nethogs \
        net-tools \
        ufw \
        fail2ban \
        logrotate \
        cron \
        rsyslog \
        supervisor \
        nginx \
        postgresql-client \
        redis-tools \
        python3 \
        python3-pip \
        python3-venv \
        build-essential \
        pkg-config \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        libhiredis-dev \
        libboost-all-dev \
        cmake \
        make \
        gcc \
        g++ \
        clang \
        valgrind \
        cppcheck \
        clang-tidy
    
    success "System dependencies installed successfully"
}

# Install Docker
install_docker() {
    log "Installing Docker..."
    
    # Remove old versions
    apt-get remove -y docker docker-engine docker.io containerd runc || true
    
    # Add Docker GPG key
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    
    # Add Docker repository
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    # Install Docker
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    
    # Start and enable Docker
    systemctl start docker
    systemctl enable docker
    
    # Add current user to docker group
    usermod -aG docker $SUDO_USER
    
    success "Docker installed successfully"
}

# Install Docker Compose
install_docker_compose() {
    log "Installing Docker Compose..."
    
    # Install latest version
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
    
    # Create symlink
    ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
    
    success "Docker Compose installed successfully"
}

# Create production directory structure
create_directory_structure() {
    log "Creating production directory structure..."
    
    # Create main directories
    mkdir -p "$PRODUCTION_DIR"/{data,logs,backups,config,certs,scripts,monitoring}
    mkdir -p "$DATA_DIR"/{postgres,redis,prometheus,grafana,elasticsearch}
    mkdir -p "$LOGS_DIR"/{nginx,moderation,api,postgres,redis}
    mkdir -p "$CONFIG_DIR"/{nginx,monitoring,backup}
    mkdir -p "$CERTS_DIR"/{ssl,ca}
    mkdir -p /var/log/sonet
    
    # Set proper permissions
    chown -R sonet:sonet "$PRODUCTION_DIR" 2>/dev/null || true
    chmod -R 750 "$PRODUCTION_DIR"
    chmod -R 755 "$LOGS_DIR"
    chmod -R 600 "$CONFIG_DIR"
    chmod -R 600 "$CERTS_DIR"
    
    success "Directory structure created successfully"
}

# Create system user
create_system_user() {
    log "Creating system user for Sonet..."
    
    # Check if user exists
    if id "sonet" &>/dev/null; then
        log "User 'sonet' already exists"
    else
        # Create user and group
        useradd -r -s /bin/bash -d "$PRODUCTION_DIR" -c "Sonet Application User" sonet
        usermod -aG docker sonet
    fi
    
    # Set ownership
    chown -R sonet:sonet "$PRODUCTION_DIR"
    
    success "System user created successfully"
}

# Configure firewall
configure_firewall() {
    log "Configuring firewall..."
    
    # Reset UFW
    ufw --force reset
    
    # Set default policies
    ufw default deny incoming
    ufw default allow outgoing
    
    # Allow SSH
    ufw allow ssh
    
    # Allow HTTP/HTTPS
    ufw allow 80/tcp
    ufw allow 443/tcp
    
    # Allow internal services (restrict to localhost)
    ufw allow from 127.0.0.1 to any port 5432  # postgresql
    ufw allow from 127.0.0.1 to any port 6379  # Redis
    ufw allow from 127.0.0.1 to any port 50051 # gRPC
    ufw allow from 127.0.0.1 to any port 8080  # API Gateway
    ufw allow from 127.0.0.1 to any port 9090  # Prometheus
    ufw allow from 127.0.0.1 to any port 3000  # Grafana
    ufw allow from 127.0.0.1 to any port 9200  # Elasticsearch
    ufw allow from 127.0.0.1 to any port 5601  # Kibana
    
    # Enable firewall
    ufw --force enable
    
    success "Firewall configured successfully"
}

# Configure fail2ban
configure_fail2ban() {
    log "Configuring fail2ban..."
    
    # Create custom jail for Sonet
    cat > /etc/fail2ban/jail.local << 'EOF'
[sonet-ssh]
enabled = true
port = ssh
filter = sshd
logpath = /var/log/auth.log
maxretry = 3
bantime = 3600
findtime = 600

[sonet-api]
enabled = true
port = 80,443
filter = sonet-api
logpath = /var/log/nginx/access.log
maxretry = 10
bantime = 1800
findtime = 300
EOF
    
    # Create custom filter for API
    cat > /etc/fail2ban/filter.d/sonet-api.conf << 'EOF'
[Definition]
failregex = ^<HOST> .* "(GET|NOTE|PUT|DELETE) /api/.*" (4\d{2}|5\d{2}) .*$
ignoreregex = ^<HOST> .* "(GET|NOTE|PUT|DELETE) /api/health.*" 200 .*$
EOF
    
    # Restart fail2ban
    systemctl restart fail2ban
    systemctl enable fail2ban
    
    success "Fail2ban configured successfully"
}

# Configure logrotate
configure_logrotate() {
    log "Configuring logrotate..."
    
    cat > /etc/logrotate.d/sonet << 'EOF'
/var/log/sonet/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 sonet sonet
    noterotate
        systemctl reload rsyslog >/dev/null 2>&1 || true
    endscript
}

/opt/sonet/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 sonet sonet
}
EOF
    
    success "Logrotate configured successfully"
}

# Configure system limits
configure_system_limits() {
    log "Configuring system limits..."
    
    # Increase file descriptor limits
    cat > /etc/security/limits.d/sonet.conf << 'EOF'
sonet soft nofile 65536
sonet hard nofile 65536
sonet soft nproc 32768
sonet hard nproc 32768
EOF
    
    # Increase kernel limits
    cat > /etc/sysctl.d/99-sonet.conf << 'EOF'
# File descriptor limits
fs.file-max = 2097152
fs.nr_open = 2097152

# Network tuning
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 5000
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.ipv4.tcp_congestion_control = bbr
net.ipv4.tcp_slow_start_after_idle = 0
net.ipv4.tcp_tw_reuse = 1

# Memory tuning
vm.swappiness = 10
vm.dirty_ratio = 15
vm.dirty_background_ratio = 5
EOF
    
    # Apply sysctl changes
    sysctl -p /etc/sysctl.d/99-sonet.conf
    
    success "System limits configured successfully"
}

# Generate SSL certificates
generate_ssl_certificates() {
    log "Generating SSL certificates..."
    
    cd "$CERTS_DIR"
    
    # Generate CA private key
    openssl genrsa -out ca/ca.key 4096
    
    # Generate CA certificate
    openssl req -new -x509 -days 3650 -key ca/ca.key -out ca/ca.crt \
        -subj "/C=US/ST=CA/L=San Francisco/O=Sonet/OU=IT/CN=Sonet Root CA"
    
    # Generate server private key
    openssl genrsa -out ssl/server.key 2048
    
    # Generate server CSR
    openssl req -new -key ssl/server.key -out ssl/server.csr \
        -subj "/C=US/ST=CA/L=San Francisco/O=Sonet/OU=IT/CN=sonet.com"
    
    # Generate server certificate
    openssl x509 -req -days 365 -in ssl/server.csr -CA ca/ca.crt -CAkey ca/ca.key \
        -CAcreateserial -out ssl/server.crt
    
    # Set permissions
    chmod 600 ssl/server.key
    chmod 644 ssl/server.crt
    chmod 600 ca/ca.key
    chmod 644 ca/ca.crt
    
    success "SSL certificates generated successfully"
}

# Create production environment file
create_production_env() {
    log "Creating production environment file..."
    
    local env_file="$CONFIG_DIR/production.env"
    
    # Copy template
    cp "$PROJECT_ROOT/config/production.env" "$env_file"
    
    # Generate secure passwords
    local postgres_password=$(openssl rand -base64 32)
    local redis_password=$(openssl rand -base64 32)
    local jwt_secret=$(openssl rand -base64 64)
    
    # Replace placeholder values
    sed -i "s/your_ultra_secure_notegres_password_here/$postgres_password/g" "$env_file"
    sed -i "s/your_ultra_secure_redis_password_here/$redis_password/g" "$env_file"
    sed -i "s/your_ultra_secure_jwt_secret_key_here_minimum_256_bits/$jwt_secret/g" "$env_file"
    
    # Set permissions
    chmod 600 "$env_file"
    chown sonet:sonet "$env_file"
    
    success "Production environment file created successfully"
}

# Create systemd services
create_systemd_services() {
    log "Creating systemd services..."
    
    # Create Sonet service
    cat > /etc/systemd/system/sonet.service << 'EOF'
[Unit]
Description=Sonet Moderation System
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/sonet
ExecStart=/usr/bin/docker-compose -f /opt/sonet/docker-compose.production.yml up -d
ExecStop=/usr/bin/docker-compose -f /opt/sonet/docker-compose.production.yml down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF
    
    # Create backup service
    cat > /etc/systemd/system/sonet-backup.service << 'EOF'
[Unit]
Description=Sonet Database Backup Service
After=sonet.service

[Service]
Type=oneshot
WorkingDirectory=/opt/sonet
ExecStart=/opt/sonet/scripts/backup.sh
User=sonet
Group=sonet

[Install]
WantedBy=multi-user.target
EOF
    
    # Create backup timer
    cat > /etc/systemd/system/sonet-backup.timer << 'EOF'
[Unit]
Description=Run Sonet backup daily
Requires=sonet-backup.service

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
EOF
    
    # Reload systemd
    systemctl daemon-reload
    
    # Enable services
    systemctl enable sonet.service
    systemctl enable sonet-backup.timer
    
    success "Systemd services created successfully"
}

# Create monitoring configuration
create_monitoring_config() {
    log "Creating monitoring configuration..."
    
    # Copy monitoring files
    cp -r "$PROJECT_ROOT/monitoring" "$CONFIG_DIR/"
    
    # Create Prometheus rules
    mkdir -p "$CONFIG_DIR/monitoring/rules"
    
    cat > "$CONFIG_DIR/monitoring/rules/alerts.yml" << 'EOF'
groups:
  - name: sonet-moderation
    rules:
      - alert: HighErrorRate
        expr: rate(sonet_moderation_errors_total[5m]) > 0.1
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High error rate in Sonet moderation service"
          description: "Error rate is {{ $value }} errors per second"

      - alert: ServiceDown
        expr: up{job="sonet-moderation-service"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Sonet moderation service is down"
          description: "Service has been down for more than 1 minute"

      - alert: HighResponseTime
        expr: histogram_quantile(0.95, rate(sonet_moderation_request_duration_seconds_bucket[5m])) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High response time in Sonet moderation service"
          description: "95th percentile response time is {{ $value }} seconds"

      - alert: HighCPUUsage
        expr: 100 - (avg by(instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100) > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High CPU usage"
          description: "CPU usage is {{ $value }}%"

      - alert: HighMemoryUsage
        expr: (node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes * 100 > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High memory usage"
          description: "Memory usage is {{ $value }}%"

      - alert: HighDiskUsage
        expr: (node_filesystem_size_bytes - node_filesystem_free_bytes) / node_filesystem_size_bytes * 100 > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High disk usage"
          description: "Disk usage is {{ $value }}%"
EOF
    
    success "Monitoring configuration created successfully"
}

# Create backup script
create_backup_script() {
    log "Creating backup script..."
    
    cat > "$PRODUCTION_DIR/scripts/backup.sh" << 'EOF'
#!/bin/bash

# Sonet Backup Script
set -euo pipefail

BACKUP_DIR="/opt/sonet/backups"
BACKUP_NAME="sonet_backup_$(date +'%Y%m%d_%H%M%S')"
BACKUP_PATH="$BACKUP_DIR/$BACKUP_NAME"

# Create backup directory
mkdir -p "$BACKUP_PATH"

# Database backup
if docker ps --format "{{.Names}}" | grep -q "sonet_notegres_prod"; then
    echo "Creating database backup..."
    docker exec sonet_notegres_prod pg_dump -U sonet_app sonet_production > "$BACKUP_PATH/database.sql"
    gzip "$BACKUP_PATH/database.sql"
fi

# Configuration backup
echo "Creating configuration backup..."
cp -r /opt/sonet/config "$BACKUP_PATH/"
cp -r /opt/sonet/nginx "$BACKUP_PATH/"

# Logs backup
echo "Creating logs backup..."
tar -czf "$BACKUP_PATH/logs.tar.gz" -C /opt/sonet/logs .

# Create backup manifest
cat > "$BACKUP_PATH/manifest.json" << MANIFEST
{
    "backup_name": "$BACKUP_NAME",
    "created_at": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')",
    "backup_type": "full",
    "components": ["database", "config", "nginx", "logs"]
}
MANIFEST

# Cleanup old backups (keep last 30)
find "$BACKUP_DIR" -name "sonet_backup_*" -type d -mtime +30 -exec rm -rf {} \;

echo "Backup completed: $BACKUP_PATH"
EOF
    
    chmod +x "$PRODUCTION_DIR/scripts/backup.sh"
    chown sonet:sonet "$PRODUCTION_DIR/scripts/backup.sh"
    
    success "Backup script created successfully"
}

# Main setup function
main() {
    log "Starting Sonet production setup..."
    
    # Pre-setup checks
    check_root
    check_system_requirements
    
    # Install dependencies
    install_system_dependencies
    install_docker
    install_docker_compose
    
    # Create infrastructure
    create_directory_structure
    create_system_user
    configure_firewall
    configure_fail2ban
    configure_logrotate
    configure_system_limits
    
    # Generate certificates
    generate_ssl_certificates
    
    # Create configuration
    create_production_env
    create_systemd_services
    create_monitoring_config
    create_backup_script
    
    # Copy project files
    log "Copying project files..."
    cp -r "$PROJECT_ROOT" "$PRODUCTION_DIR/"
    chown -R sonet:sonet "$PRODUCTION_DIR"
    
    success "Production setup completed successfully!"
    log "Setup log: $SETUP_LOG"
    log "Production directory: $PRODUCTION_DIR"
    log "Next steps:"
    log "1. Edit /opt/sonet/config/production.env with your domain and settings"
    log "2. Run: systemctl start sonet.service"
    log "3. Check logs: journalctl -u sonet.service -f"
}

# Execute main function
main "$@"