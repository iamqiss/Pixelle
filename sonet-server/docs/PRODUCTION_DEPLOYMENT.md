# 🚀 Sonet Production Deployment Guide

## Overview
This guide provides step-by-step instructions for deploying the Sonet moderation system to production with enterprise-grade security, monitoring, and operational procedures.

## 🎯 Pre-Deployment Requirements

### System Requirements
- **OS**: Ubuntu 20.04+ or Debian 11+
- **CPU**: Minimum 4 cores, recommended 8+ cores
- **RAM**: Minimum 8GB, recommended 16GB+
- **Storage**: Minimum 50GB, recommended 100GB+ SSD
- **Network**: Stable internet connection with static IP

### Domain & SSL
- **Domain**: sonet.com (or your domain)
- **SSL Certificate**: Let's Encrypt or commercial certificate
- **DNS**: A record pointing to your server IP

### Security Requirements
- **Firewall**: UFW or iptables configured
- **SSH**: Key-based authentication only
- **Fail2ban**: Brute force protection
- **Regular Updates**: Automated security patches

## 🚀 Production Deployment Steps

### Step 1: Server Preparation
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install essential tools
sudo apt install -y curl wget git htop ufw fail2ban

# Configure SSH security
sudo nano /etc/ssh/sshd_config
# Set: PermitRootLogin no, PasswordAuthentication no
sudo systemctl restart ssh
```

### Step 2: Run Production Setup Script
```bash
# Clone Sonet repository
git clone https://github.com/sonet/sonet.git
cd sonet

# Make setup script executable
chmod +x scripts/setup-production.sh

# Run production setup (as root)
sudo ./scripts/setup-production.sh
```

### Step 3: Configure Environment Variables
```bash
# Edit production environment file
sudo nano /opt/sonet/config/production.env

# Update these critical values:
# - DOMAIN=your-domain.com
# - CORS_ORIGIN=https://your-domain.com
# - FOUNDER_IP_WHITELIST=your-ip-address
# - SMTP settings for notifications
# - Backup S3 credentials
```

### Step 4: Deploy Services
```bash
# Start Sonet services
sudo systemctl start sonet.service

# Check service status
sudo systemctl status sonet.service

# View logs
sudo journalctl -u sonet.service -f
```

### Step 5: Database Initialization
```bash
# Connect to postgresql
sudo docker exec -it sonet_notegres_prod psql -U sonet_app -d sonet_production

# Run database migrations
\i /opt/sonet/database/migrations/001_create_moderation_tables.sql

# Verify tables created
\dt
\dt moderation.*

# Exit postgresql
\q
```

### Step 6: SSL Certificate Setup
```bash
# Install Certbot
sudo apt install -y certbot python3-certbot-nginx

# Generate SSL certificate
sudo certbot --nginx -d sonet.com -d www.sonet.com

# Test auto-renewal
sudo certbot renew --dry-run
```

### Step 7: Monitoring Setup
```bash
# Access monitoring dashboards
# Prometheus: http://your-server:9090
# Grafana: http://your-server:3000 (admin/admin)
# Kibana: http://your-server:5601

# Import Grafana dashboards
# 1. Create Sonet Moderation dashboard
# 2. Add Prometheus data source
# 3. Import dashboard templates
```

## 🔒 Security Hardening Checklist

### Network Security
- [ ] Firewall configured (UFW)
- [ ] SSH access restricted to specific IPs
- [ ] Fail2ban enabled and configured
- [ ] Rate limiting enabled on API endpoints
- [ ] Internal services not exposed to internet

### Application Security
- [ ] Founder IP whitelist configured
- [ ] JWT secrets generated and secure
- [ ] Database passwords strong and unique
- [ ] SSL/TLS enabled with strong ciphers
- [ ] Security headers configured
- [ ] CORS policy restricted

### System Security
- [ ] Regular security updates enabled
- [ ] Non-root user for application
- [ ] File permissions restricted
- [ ] Log monitoring enabled
- [ ] Backup encryption enabled

## 📊 Monitoring & Alerting

### Key Metrics to Monitor
- **Service Health**: API response times, error rates
- **System Resources**: CPU, memory, disk usage
- **Database Performance**: Connection count, query times
- **Security Events**: Failed login attempts, suspicious activity
- **Business Metrics**: Moderation actions, user activity

### Alerting Rules
```yaml
# High Error Rate
- alert: HighErrorRate
  expr: rate(sonet_moderation_errors_total[5m]) > 0.1
  for: 2m
  labels:
    severity: warning

# Service Down
- alert: ServiceDown
  expr: up{job="sonet-moderation-service"} == 0
  for: 1m
  labels:
    severity: critical

# High Response Time
- alert: HighResponseTime
  expr: histogram_quantile(0.95, rate(sonet_moderation_request_duration_seconds_bucket[5m])) > 1
  for: 5m
  labels:
    severity: warning
```

### Dashboard Setup
1. **System Overview**: CPU, memory, disk, network
2. **Service Health**: Response times, error rates, throughput
3. **Security Monitoring**: Login attempts, rate limiting, IP blocks
4. **Business Metrics**: Moderation actions, user engagement
5. **Database Performance**: Connection pools, query performance

## 🔄 Backup & Recovery

### Automated Backups
```bash
# Backup runs daily at 2 AM
sudo systemctl status sonet-backup.timer

# Manual backup
sudo systemctl start sonet-backup.service

# Check backup status
sudo journalctl -u sonet-backup.service
```

### Backup Verification
```bash
# List available backups
ls -la /opt/sonet/backups/

# Verify backup integrity
cd /opt/sonet/backups/sonet_backup_YYYYMMDD_HHMMSS
gunzip -t database.sql.gz
cat manifest.json
```

### Recovery Procedures
```bash
# Stop services
sudo systemctl stop sonet.service

# Restore database
gunzip -c database.sql.gz | sudo docker exec -i sonet_notegres_prod psql -U sonet_app sonet_production

# Restore configuration
sudo cp -r config/* /opt/sonet/config/

# Start services
sudo systemctl start sonet.service
```

## 🚨 Incident Response

### Service Outage
1. **Immediate Response**
   - Check service status: `sudo systemctl status sonet.service`
   - View logs: `sudo journalctl -u sonet.service -f`
   - Check resource usage: `htop`, `df -h`

2. **Investigation**
   - Review error logs
   - Check monitoring dashboards
   - Verify database connectivity
   - Check network connectivity

3. **Recovery**
   - Restart failed services
   - Scale up resources if needed
   - Restore from backup if necessary

### Security Incident
1. **Immediate Response**
   - Block suspicious IPs
   - Review access logs
   - Check for unauthorized access
   - Verify founder account security

2. **Investigation**
   - Review audit logs
   - Check system integrity
   - Analyze network traffic
   - Review user activity

3. **Recovery**
   - Reset compromised credentials
   - Restore from clean backup
   - Update security policies
   - Notify stakeholders

## 📈 Performance Optimization

### Database Tuning
```sql
-- Check slow queries
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;

-- Analyze table statistics
ANALYZE;

-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;
```

### Application Tuning
```bash
# Check service performance
sudo docker stats

# Monitor API response times
curl -w "@curl-format.txt" -o /dev/null -s "https://sonet.com/api/v1/health"

# Check resource usage
sudo docker exec sonet_moderation_service_prod top
```

### Scaling Considerations
- **Horizontal Scaling**: Add more API gateway instances
- **Database Scaling**: Read replicas, connection pooling
- **Caching**: Redis optimization, CDN integration
- **Load Balancing**: HAProxy, Nginx upstream

## 🔧 Maintenance Procedures

### Regular Maintenance
```bash
# Weekly
- Review logs for errors
- Check backup status
- Monitor disk usage
- Review security events

# Monthly
- Update system packages
- Review performance metrics
- Check SSL certificate expiration
- Review access logs

# Quarterly
- Security audit
- Performance review
- Backup restoration test
- Disaster recovery drill
```

### Update Procedures
```bash
# Create backup before updates
sudo systemctl start sonet-backup.service

# Update application
cd /opt/sonet
git pull origin main

# Rebuild and restart services
sudo systemctl restart sonet.service

# Verify update success
sudo systemctl status sonet.service
curl -f https://sonet.com/health
```

## 📋 Note-Deployment Checklist

### Service Verification
- [ ] All services running and healthy
- [ ] API endpoints responding correctly
- [ ] Database connections working
- [ ] SSL certificates valid and working
- [ ] Monitoring dashboards accessible

### Security Verification
- [ ] Founder access working from whitelisted IPs
- [ ] Rate limiting functioning correctly
- [ ] Security headers present
- [ ] Fail2ban blocking suspicious activity
- [ ] Logs being generated and rotated

### Performance Verification
- [ ] Response times under 100ms
- [ ] Error rates below 1%
- [ ] Resource usage within limits
- [ ] Backup system working
- [ ] Monitoring alerts configured

### Documentation
- [ ] Runbook updated
- [ ] Contact information documented
- [ ] Escalation procedures defined
- [ ] Recovery procedures tested
- [ ] Team trained on operations

## 🆘 Support & Troubleshooting

### Common Issues
1. **Service Won't Start**: Check logs, verify dependencies
2. **Database Connection Failed**: Check credentials, network connectivity
3. **SSL Certificate Issues**: Verify certificate validity, check paths
4. **Performance Problems**: Monitor resources, check database queries
5. **Security Issues**: Review logs, check firewall rules

### Useful Commands
```bash
# Service management
sudo systemctl status sonet.service
sudo journalctl -u sonet.service -f

# Docker management
sudo docker ps
sudo docker logs sonet_moderation_service_prod

# Database access
sudo docker exec -it sonet_notegres_prod psql -U sonet_app -d sonet_production

# Log analysis
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/sonet/deployment.log

# System monitoring
htop
df -h
free -h
```

### Getting Help
- **Documentation**: Check this guide and code comments
- **Logs**: Review service and system logs
- **Monitoring**: Check Prometheus and Grafana dashboards
- **Community**: GitHub issues and discussions
- **Support**: Contact Sonet engineering team

## 🎉 Deployment Complete!

Congratulations! You've successfully deployed the Sonet moderation system to production. The system is now ready to handle enterprise-grade moderation with complete founder anonymity and professional appearance.

**Remember**: Regular monitoring, maintenance, and security updates are essential for keeping your production system healthy and secure.

---

*Last updated: 2024-01-01*
*Version: 1.0.0*
*Author: Sonet Engineering Team*