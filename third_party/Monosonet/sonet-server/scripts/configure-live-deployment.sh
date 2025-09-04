#!/bin/bash

# Sonet Live Deployment Configuration Script
# This script configures the system for live production deployment

set -euo pipefail

# Configuration
PRODUCTION_DIR="/opt/sonet"
CONFIG_DIR="/opt/sonet/config"
CERTS_DIR="/opt/sonet/certs"
NGINX_DIR="/opt/sonet/nginx"
LOGS_DIR="/opt/sonet/logs"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging
log() { echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOGS_DIR/live-deployment.log"; }
error() { echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOGS_DIR/live-deployment.log"; exit 1; }
warning() { echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOGS_DIR/live-deployment.log"; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOGS_DIR/live-deployment.log"; }

# Check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        error "This script must be run as root"
    fi
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check if production setup is complete
    if [[ ! -d "$PRODUCTION_DIR" ]]; then
        error "Production setup not completed. Run setup-production.sh first."
    fi
    
    # Check if services are running
    if ! systemctl is-active --quiet sonet.service; then
        error "Sonet services are not running. Start them first with: systemctl start sonet.service"
    fi
    
    # Check if domain is provided
    if [[ -z "${DOMAIN:-}" ]]; then
        error "DOMAIN environment variable not set. Set it before running this script."
    fi
    
    success "Prerequisites check passed"
}

# Install Certbot for Let's Encrypt
install_certbot() {
    log "Installing Certbot for SSL certificates..."
    
    # Update package list
    apt-get update
    
    # Install Certbot and Nginx plugin
    apt-get install -y certbot python3-certbot-nginx
    
    success "Certbot installed successfully"
}

# Configure domain in environment
configure_domain() {
    log "Configuring domain: $DOMAIN"
    
    local env_file="$CONFIG_DIR/production.env"
    
    # Update domain in environment file
    sed -i "s/DOMAIN=.*/DOMAIN=$DOMAIN/g" "$env_file"
    sed -i "s|CORS_ORIGIN=.*|CORS_ORIGIN=https://$DOMAIN|g" "$env_file"
    
    # Update Nginx configuration
    sed -i "s/sonet.com/$DOMAIN/g" "$NGINX_DIR/nginx.conf"
    sed -i "s/www.sonet.com/www.$DOMAIN/g" "$NGINX_DIR/nginx.conf"
    
    success "Domain configuration updated"
}

# Generate SSL certificate with Let's Encrypt
generate_ssl_certificate() {
    log "Generating SSL certificate for $DOMAIN..."
    
    # Stop Nginx temporarily
    docker-compose -f "$PRODUCTION_DIR/docker-compose.production.yml" stop nginx
    
    # Generate certificate
    certbot certonly --standalone \
        --email admin@$DOMAIN \
        --agree-tos \
        --no-eff-email \
        --domains $DOMAIN,www.$DOMAIN \
        --cert-path "$CERTS_DIR/ssl" \
        --key-path "$CERTS_DIR/ssl"
    
    # Copy certificates to production location
    cp /etc/letsencrypt/live/$DOMAIN/fullchain.pem "$CERTS_DIR/ssl/server.crt"
    cp /etc/letsencrypt/live/$DOMAIN/privkey.pem "$CERTS_DIR/ssl/server.key"
    
    # Set proper permissions
    chmod 644 "$CERTS_DIR/ssl/server.crt"
    chmod 600 "$CERTS_DIR/ssl/server.key"
    chown sonet:sonet "$CERTS_DIR/ssl/server.crt"
    chown sonet:sonet "$CERTS_DIR/ssl/server.key"
    
    # Restart Nginx
    docker-compose -f "$PRODUCTION_DIR/docker-compose.production.yml" start nginx
    
    success "SSL certificate generated successfully"
}

# Configure SSL auto-renewal
configure_ssl_renewal() {
    log "Configuring SSL auto-renewal..."
    
    # Create renewal script
    cat > "$PRODUCTION_DIR/scripts/renew-ssl.sh" << 'EOF'
#!/bin/bash

# Sonet SSL Certificate Renewal Script
set -euo pipefail

DOMAIN="${DOMAIN:-sonet.com}"
CERTS_DIR="/opt/sonet/certs"
PRODUCTION_DIR="/opt/sonet"

# Renew certificate
certbot renew --quiet

# Copy renewed certificates
cp /etc/letsencrypt/live/$DOMAIN/fullchain.pem "$CERTS_DIR/ssl/server.crt"
cp /etc/letsencrypt/live/$DOMAIN/privkey.pem "$CERTS_DIR/ssl/server.key"

# Set permissions
chmod 644 "$CERTS_DIR/ssl/server.crt"
chmod 600 "$CERTS_DIR/ssl/server.key"
chown sonet:sonet "$CERTS_DIR/ssl/server.crt"
chown sonet:sonet "$CERTS_DIR/ssl/server.key"

# Reload Nginx
cd "$PRODUCTION_DIR"
docker-compose -f docker-compose.production.yml exec nginx nginx -s reload

echo "SSL certificate renewed successfully"
EOF
    
    chmod +x "$PRODUCTION_DIR/scripts/renew-ssl.sh"
    chown sonet:sonet "$PRODUCTION_DIR/scripts/renew-ssl.sh"
    
    # Add to crontab for auto-renewal
    (crontab -l 2>/dev/null; echo "0 12 * * * /opt/sonet/scripts/renew-ssl.sh") | crontab -
    
    success "SSL auto-renewal configured"
}

# Configure DNS settings
configure_dns() {
    log "Configuring DNS settings..."
    
    # Get server IP address
    local server_ip=$(curl -s ifconfig.me)
    
    cat > "$PRODUCTION_DIR/docs/dns-configuration.md" << EOF
# DNS Configuration for $DOMAIN

## Required DNS Records

### A Records
- **$DOMAIN** → $server_ip
- **www.$DOMAIN** → $server_ip

### CNAME Records
- **api.$DOMAIN** → $DOMAIN
- **cdn.$DOMAIN** → $DOMAIN

### MX Records (if using email)
- **$DOMAIN** → mail.$DOMAIN (priority 10)

### TXT Records
- **$DOMAIN** → "v=spf1 include:_spf.google.com ~all"
- **_dmarc.$DOMAIN** → "v=DMARC1; p=quarantine; rua=mailto:dmarc@$DOMAIN"

## DNS Provider Instructions

### Cloudflare
1. Add domain to Cloudflare
2. Update nameservers at your domain registrar
3. Set DNS records as above
4. Enable SSL/TLS encryption mode: "Full (strict)"
5. Enable "Always Use HTTPS" rule

### AWS Route 53
1. Create hosted zone for $DOMAIN
2. Update nameservers at your domain registrar
3. Create A records pointing to $server_ip
4. Enable health checks for monitoring

### Google Domains
1. Go to DNS settings
2. Add A records pointing to $server_ip
3. Enable DNSSEC if available

## Verification Commands
\`\`\`bash
# Check A record
nslookup $DOMAIN

# Check SSL certificate
openssl s_client -connect $DOMAIN:443 -servername $DOMAIN

# Test HTTPS
curl -I https://$DOMAIN
\`\`\`

## Important Notes
- DNS changes may take 24-48 hours to propagate globally
- Monitor SSL certificate status with: certbot certificates
- Test SSL renewal with: certbot renew --dry-run
EOF
    
    success "DNS configuration guide created"
    log "DNS configuration guide: $PRODUCTION_DIR/docs/dns-configuration.md"
    log "Server IP: $server_ip"
}

# Configure monitoring and alerting
configure_monitoring() {
    log "Configuring monitoring and alerting..."
    
    # Create monitoring configuration
    cat > "$CONFIG_DIR/monitoring/alertmanager.yml" << 'EOF'
global:
  smtp_smarthost: 'localhost:25'
  smtp_from: 'alertmanager@sonet.com'

route:
  group_by: ['alertname']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h
  receiver: 'web.hook'

receivers:
  - name: 'web.hook'
    webhook_configs:
      - url: 'http://127.0.0.1:5001/'
EOF
    
    # Create Grafana dashboard configuration
    cat > "$CONFIG_DIR/monitoring/grafana/dashboards/sonet-overview.json" << 'EOF'
{
  "dashboard": {
    "id": null,
    "title": "Sonet Production Overview",
    "tags": ["sonet", "production"],
    "timezone": "browser",
    "panels": [
      {
        "id": 1,
        "title": "Service Health",
        "type": "stat",
        "targets": [
          {
            "expr": "up{job=\"sonet-moderation-service\"}",
            "legendFormat": "Moderation Service"
          }
        ]
      },
      {
        "id": 2,
        "title": "API Response Time",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(sonet_api_request_duration_seconds_bucket[5m]))",
            "legendFormat": "95th Percentile"
          }
        ]
      },
      {
        "id": 3,
        "title": "Error Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(sonet_moderation_errors_total[5m])",
            "legendFormat": "Errors/sec"
          }
        ]
      }
    ]
  }
}
EOF
    
    # Create monitoring alerts
    cat > "$CONFIG_DIR/monitoring/rules/production-alerts.yml" << 'EOF'
groups:
  - name: sonet-production
    rules:
      - alert: HighErrorRate
        expr: rate(sonet_moderation_errors_total[5m]) > 0.05
        for: 2m
        labels:
          severity: warning
          environment: production
        annotations:
          summary: "High error rate in production"
          description: "Error rate is {{ $value }} errors per second"

      - alert: ServiceDown
        expr: up{job="sonet-moderation-service"} == 0
        for: 1m
        labels:
          severity: critical
          environment: production
        annotations:
          summary: "Sonet moderation service is down"
          description: "Service has been down for more than 1 minute"

      - alert: HighResponseTime
        expr: histogram_quantile(0.95, rate(sonet_api_request_duration_seconds_bucket[5m])) > 0.5
        for: 5m
        labels:
          severity: warning
          environment: production
        annotations:
          summary: "High response time in production"
          description: "95th percentile response time is {{ $value }} seconds"

      - alert: HighCPUUsage
        expr: 100 - (avg by(instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100) > 80
        for: 5m
        labels:
          severity: warning
          environment: production
        annotations:
          summary: "High CPU usage in production"
          description: "CPU usage is {{ $value }}%"

      - alert: HighMemoryUsage
        expr: (node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes * 100 > 80
        for: 5m
        labels:
          severity: warning
          environment: production
        annotations:
          summary: "High memory usage in production"
          description: "Memory usage is {{ $value }}%"

      - alert: HighDiskUsage
        expr: (node_filesystem_size_bytes - node_filesystem_free_bytes) / node_filesystem_size_bytes * 100 > 80
        for: 5m
        labels:
          severity: warning
          environment: production
        annotations:
          summary: "High disk usage in production"
          description: "Disk usage is {{ $value }}%"

      - alert: SSLExpiringSoon
        expr: probe_ssl_earliest_cert_expiry - time() < 86400 * 30
        for: 1m
        labels:
          severity: warning
          environment: production
        annotations:
          summary: "SSL certificate expiring soon"
          description: "SSL certificate expires in {{ $value }} seconds"

      - alert: HighRequestRate
        expr: rate(nginx_http_requests_total[5m]) > 1000
        for: 5m
        labels:
          severity: info
          environment: production
        annotations:
          summary: "High request rate in production"
          description: "Request rate is {{ $value }} requests per second"
EOF
    
    success "Monitoring and alerting configured"
}

# Configure backup and recovery
configure_backup_recovery() {
    log "Configuring backup and recovery..."
    
    # Create backup verification script
    cat > "$PRODUCTION_DIR/scripts/verify-backup.sh" << 'EOF'
#!/bin/bash

# Sonet Backup Verification Script
set -euo pipefail

BACKUP_DIR="/opt/sonet/backups"
LATEST_BACKUP=$(ls -t "$BACKUP_DIR"/sonet_backup_* | head -1)

if [[ -z "$LATEST_BACKUP" ]]; then
    echo "No backups found"
    exit 1
fi

echo "Verifying backup: $LATEST_BACKUP"

# Check backup integrity
cd "$LATEST_BACKUP"

# Verify database backup
if [[ -f "database.sql.gz" ]]; then
    echo "Verifying database backup..."
    gunzip -t database.sql.gz
    echo "Database backup: OK"
else
    echo "Database backup: MISSING"
fi

# Verify configuration backup
if [[ -d "config" ]]; then
    echo "Verifying configuration backup..."
    ls -la config/
    echo "Configuration backup: OK"
else
    echo "Configuration backup: MISSING"
fi

# Verify logs backup
if [[ -f "logs.tar.gz" ]]; then
    echo "Verifying logs backup..."
    tar -tzf logs.tar.gz > /dev/null
    echo "Logs backup: OK"
else
    echo "Logs backup: MISSING"
fi

# Check manifest
if [[ -f "manifest.json" ]]; then
    echo "Backup manifest:"
    cat manifest.json
fi

echo "Backup verification completed"
EOF
    
    chmod +x "$PRODUCTION_DIR/scripts/verify-backup.sh"
    chown sonet:sonet "$PRODUCTION_DIR/scripts/verify-backup.sh"
    
    # Create disaster recovery script
    cat > "$PRODUCTION_DIR/scripts/disaster-recovery.sh" << 'EOF'
#!/bin/bash

# Sonet Disaster Recovery Script
set -euo pipefail

BACKUP_DIR="/opt/sonet/backups"
PRODUCTION_DIR="/opt/sonet"

echo "=== SONET DISASTER RECOVERY ==="
echo "This script will restore the system from backup"
echo "WARNING: This will overwrite current data!"
echo ""

read -p "Are you sure you want to continue? (yes/no): " confirm

if [[ "$confirm" != "yes" ]]; then
    echo "Recovery cancelled"
    exit 1
fi

# List available backups
echo "Available backups:"
ls -la "$BACKUP_DIR"/sonet_backup_*

# Select backup
read -p "Enter backup name to restore from: " backup_name

if [[ ! -d "$BACKUP_DIR/$backup_name" ]]; then
    echo "Backup not found: $backup_name"
    exit 1
fi

echo "Starting recovery from: $backup_name"

# Stop services
echo "Stopping services..."
systemctl stop sonet.service

# Restore database
if [[ -f "$BACKUP_DIR/$backup_name/database.sql.gz" ]]; then
    echo "Restoring database..."
    gunzip -c "$BACKUP_DIR/$backup_name/database.sql.gz" | \
        docker exec -i sonet_notegres_prod psql -U sonet_app sonet_production
fi

# Restore configuration
if [[ -d "$BACKUP_DIR/$backup_name/config" ]]; then
    echo "Restoring configuration..."
    cp -r "$BACKUP_DIR/$backup_name/config"/* "$PRODUCTION_DIR/config/"
fi

# Restore Nginx configuration
if [[ -d "$BACKUP_DIR/$backup_name/nginx" ]]; then
    echo "Restoring Nginx configuration..."
    cp -r "$BACKUP_DIR/$backup_name/nginx"/* "$PRODUCTION_DIR/nginx/"
fi

# Start services
echo "Starting services..."
systemctl start sonet.service

echo "Recovery completed successfully"
echo "Please verify system functionality"
EOF
    
    chmod +x "$PRODUCTION_DIR/scripts/disaster-recovery.sh"
    chown sonet:sonet "$PRODUCTION_DIR/scripts/disaster-recovery.sh"
    
    success "Backup and recovery configured"
}

# Configure performance monitoring
configure_performance_monitoring() {
    log "Configuring performance monitoring..."
    
    # Create performance test script
    cat > "$PRODUCTION_DIR/scripts/performance-test.sh" << 'EOF'
#!/bin/bash

# Sonet Performance Test Script
set -euo pipefail

DOMAIN="${DOMAIN:-sonet.com}"
API_BASE="https://$DOMAIN/api/v1"

echo "=== SONET PERFORMANCE TEST ==="
echo "Testing API endpoints and performance"
echo ""

# Test health endpoint
echo "Testing health endpoint..."
health_response=$(curl -s -w "%{http_code}" -o /dev/null "$API_BASE/health")
echo "Health endpoint: HTTP $health_response"

# Test API response time
echo "Testing API response time..."
for i in {1..10}; do
    response_time=$(curl -s -w "%{time_total}" -o /dev/null "$API_BASE/health")
    echo "Request $i: ${response_time}s"
done

# Test load
echo "Testing load (10 concurrent requests)..."
for i in {1..10}; do
    (curl -s "$API_BASE/health" > /dev/null && echo "Request $i: OK") &
done
wait

echo "Performance test completed"
EOF
    
    chmod +x "$PRODUCTION_DIR/scripts/performance-test.sh"
    chown sonet:sonet "$PRODUCTION_DIR/scripts/performance-test.sh"
    
    success "Performance monitoring configured"
}

# Create go-live checklist
create_go_live_checklist() {
    log "Creating go-live checklist..."
    
    cat > "$PRODUCTION_DIR/docs/go-live-checklist.md" << 'EOF'
# 🚀 Sonet Go-Live Checklist

## Pre-Go-Live Verification

### Infrastructure
- [ ] Server running and stable
- [ ] All services healthy and responding
- [ ] SSL certificates valid and working
- [ ] Firewall configured and active
- [ ] Monitoring dashboards accessible
- [ ] Backup system working

### Security
- [ ] Founder IP whitelist configured
- [ ] Rate limiting active
- [ ] Security headers present
- [ ] Fail2ban blocking suspicious activity
- [ ] Logs being generated and rotated
- [ ] SSL/TLS configuration secure

### Performance
- [ ] Response times under 100ms
- [ ] Error rates below 1%
- [ ] Resource usage within limits
- [ ] Database performance optimized
- [ ] Caching working correctly

### Functionality
- [ ] Founder login working
- [ ] Moderation actions functional
- [ ] API endpoints responding
- [ ] Database connections stable
- [ ] Backup/restore tested

## Go-Live Steps

### 1. Final Verification
```bash
# Check all services
systemctl status sonet.service

# Verify SSL certificate
openssl s_client -connect $DOMAIN:443 -servername $DOMAIN

# Test API endpoints
curl -f https://$DOMAIN/api/v1/health

# Check monitoring
curl -f http://localhost:9090/-/healthy
```

### 2. DNS Update
- Update A records to point to production server
- Wait for DNS propagation (24-48 hours)
- Verify with: `nslookup $DOMAIN`

### 3. SSL Certificate
- Verify Let's Encrypt certificate is active
- Test auto-renewal: `certbot renew --dry-run`
- Monitor certificate expiration

### 4. Monitoring Setup
- Configure alerting rules
- Set up notification channels
- Test alert delivery
- Monitor system metrics

### 5. Backup Verification
```bash
# Test backup system
/opt/sonet/scripts/backup.sh

# Verify backup integrity
/opt/sonet/scripts/verify-backup.sh
```

## Note-Go-Live Monitoring

### First 24 Hours
- Monitor all services continuously
- Check error rates and response times
- Verify backup completion
- Monitor resource usage

### First Week
- Review performance metrics
- Check security logs
- Verify monitoring alerts
- Test disaster recovery

### Ongoing
- Daily backup verification
- Weekly performance review
- Monthly security audit
- Quarterly disaster recovery drill

## Emergency Procedures

### Service Outage
1. Check service status: `systemctl status sonet.service`
2. Review logs: `journalctl -u sonet.service -f`
3. Check monitoring dashboards
4. Restart services if needed
5. Escalate if unresolved

### Security Incident
1. Block suspicious IPs immediately
2. Review access logs
3. Check for unauthorized access
4. Verify founder account security
5. Notify stakeholders

### Performance Issues
1. Check resource usage
2. Review database performance
3. Analyze API response times
4. Scale resources if needed
5. Optimize queries and caching

## Contact Information

### Engineering Team
- **Primary**: engineering@$DOMAIN
- **Emergency**: +1-XXX-XXX-XXXX
- **Slack**: #sonet-production

### Escalation Path
1. On-call engineer (24/7)
2. Engineering lead
3. CTO
4. CEO

## Success Criteria

### Technical
- 99.9% uptime
- <100ms API response time
- <1% error rate
- Successful daily backups
- All monitoring alerts green

### Business
- Founder can perform all moderation actions
- System maintains complete anonymity
- Professional appearance maintained
- User experience smooth and responsive

---

**Remember**: This is a live production system. Always test changes in staging first!
EOF
    
    success "Go-live checklist created"
}

# Main configuration function
main() {
    log "Starting Sonet live deployment configuration..."
    
    # Pre-configuration checks
    check_root
    check_prerequisites
    
    # Install and configure SSL
    install_certbot
    configure_domain
    generate_ssl_certificate
    configure_ssl_renewal
    
    # Configure DNS and monitoring
    configure_dns
    configure_monitoring
    configure_backup_recovery
    configure_performance_monitoring
    
    # Create go-live documentation
    create_go_live_checklist
    
    # Restart services with new configuration
    log "Restarting services with new configuration..."
    systemctl restart sonet.service
    
    success "Live deployment configuration completed successfully!"
    log "Configuration log: $LOGS_DIR/live-deployment.log"
    log ""
    log "Next steps:"
    log "1. Update DNS records as shown in: $PRODUCTION_DIR/docs/dns-configuration.md"
    log "2. Review go-live checklist: $PRODUCTION_DIR/docs/go-live-checklist.md"
    log "3. Test all functionality from founder account"
    log "4. Monitor system performance and health"
    log "5. Go live when ready!"
}

# Execute main function
main "$@"