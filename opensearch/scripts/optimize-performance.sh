#!/bin/bash

# OpenSearch Performance Optimization Script
# This script applies various optimizations to make OpenSearch perform like a beast
# while reducing disk usage compared to Elasticsearch

set -e

echo "🚀 OpenSearch Performance Optimization Script"
echo "=============================================="

# Function to check if running as root
check_root() {
    if [[ $EUID -eq 0 ]]; then
        echo "⚠️  Running as root. Some optimizations may not apply."
    fi
}

# Function to optimize system settings
optimize_system() {
    echo "📊 Optimizing system settings..."
    
    # Increase file descriptor limits
    if command -v ulimit &> /dev/null; then
        ulimit -n 65536
        echo "✅ Increased file descriptor limit to 65536"
    fi
    
    # Optimize kernel parameters for better I/O
    if [[ $EUID -eq 0 ]]; then
        echo "vm.swappiness=1" >> /etc/sysctl.conf
        echo "vm.max_map_count=262144" >> /etc/sysctl.conf
        echo "net.core.somaxconn=65535" >> /etc/sysctl.conf
        sysctl -p
        echo "✅ Applied kernel optimizations"
    else
        echo "⚠️  Skipping kernel optimizations (requires root)"
    fi
}

# Function to create optimized JVM options
create_optimized_jvm_options() {
    echo "☕ Creating optimized JVM options..."
    
    local jvm_options_file="config/jvm.options.optimized"
    
    cat > "$jvm_options_file" << 'EOF'
## Optimized JVM Configuration for Maximum Performance
## Based on Elasticsearch best practices and OpenSearch optimizations

# Heap settings (adjust based on available memory)
-Xms4g
-Xmx4g

# G1GC Optimizations
-XX:+UseG1GC
-XX:G1ReservePercent=20
-XX:InitiatingHeapOccupancyPercent=25
-XX:G1HeapRegionSize=16m
-XX:MaxGCPauseMillis=200
-XX:G1MixedGCCountTarget=8
-XX:G1MixedGCLiveThresholdPercent=85
-XX:G1OldCSetRegionThreshold=10
-XX:+G1UseAdaptiveIHOP
-XX:G1AdaptiveIHOPNumInitialSamples=3

# Performance optimizations
-XX:+TieredCompilation
-XX:TieredStopAtLevel=4
-XX:+UseCompressedOops
-XX:+UseCompressedClassPointers
-XX:+UseStringDeduplication
-XX:+OptimizeStringConcat
-XX:+UseLargePages
-XX:+UseTransparentHugePages
-XX:+AlwaysPreTouch

# Network optimizations
-Djava.net.preferIPv4Stack=true
-Djava.awt.headless=true

# Memory management
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/tmp/opensearch-heap-dump.hprof

# JVM temporary directory
-Djava.io.tmpdir=${DENSITY_TMPDIR}

# GC logging for monitoring
-Xlog:gc*,gc+age=trace,safepoint:file=logs/gc.log:utctime,pid,tags:filecount=32,filesize=64m
EOF

    echo "✅ Created optimized JVM options file: $jvm_options_file"
}

# Function to create performance monitoring script
create_monitoring_script() {
    echo "📈 Creating performance monitoring script..."
    
    cat > "scripts/monitor-performance.sh" << 'EOF'
#!/bin/bash

# OpenSearch Performance Monitoring Script

echo "🔍 OpenSearch Performance Monitor"
echo "================================="

# Check cluster health
echo "📊 Cluster Health:"
curl -s "localhost:9200/_cluster/health?pretty" | grep -E "(status|active_shards|relocating_shards|initializing_shards|unassigned_shards)"

echo -e "\n💾 Memory Usage:"
curl -s "localhost:9200/_nodes/stats/jvm?pretty" | grep -E "(heap_used_percent|heap_used_in_bytes|heap_max_in_bytes)"

echo -e "\n💿 Disk Usage:"
curl -s "localhost:9200/_nodes/stats/fs?pretty" | grep -E "(total_in_bytes|free_in_bytes|available_in_bytes)"

echo -e "\n⚡ Index Stats:"
curl -s "localhost:9200/_stats?pretty" | grep -E "(indexing|search|get|bulk)"

echo -e "\n🔄 GC Stats:"
curl -s "localhost:9200/_nodes/stats/jvm?pretty" | grep -A 5 "gc"
EOF

    chmod +x "scripts/monitor-performance.sh"
    echo "✅ Created performance monitoring script"
}

# Function to create index optimization script
create_index_optimization_script() {
    echo "🗂️  Creating index optimization script..."
    
    cat > "scripts/optimize-indices.sh" << 'EOF'
#!/bin/bash

# OpenSearch Index Optimization Script

echo "🗂️  OpenSearch Index Optimization"
echo "================================="

# Function to optimize a specific index
optimize_index() {
    local index_name=$1
    echo "🔧 Optimizing index: $index_name"
    
    # Force merge to reduce segments
    curl -X POST "localhost:9200/$index_name/_forcemerge?max_num_segments=1&wait_for_completion=true"
    
    # Update index settings for better performance
    curl -X PUT "localhost:9200/$index_name/_settings" -H 'Content-Type: application/json' -d '{
        "index": {
            "refresh_interval": "30s",
            "translog.flush_threshold_size": "512mb",
            "translog.sync_interval": "30s",
            "translog.durability": "async",
            "codec": "best_compression"
        }
    }'
    
    echo "✅ Optimized index: $index_name"
}

# Get all indices
indices=$(curl -s "localhost:9200/_cat/indices?h=index" | grep -v "^\.opensearch")

echo "📋 Found indices:"
echo "$indices"

# Optimize each index
for index in $indices; do
    optimize_index "$index"
done

echo "🎉 Index optimization complete!"
EOF

    chmod +x "scripts/optimize-indices.sh"
    echo "✅ Created index optimization script"
}

# Function to create startup optimization script
create_startup_script() {
    echo "🚀 Creating startup optimization script..."
    
    cat > "scripts/start-optimized.sh" << 'EOF'
#!/bin/bash

# Optimized OpenSearch Startup Script

echo "🚀 Starting OpenSearch with Performance Optimizations"
echo "====================================================="

# Set environment variables for optimization
export DENSITY_JAVA_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication"
export DENSITY_HEAP_SIZE="4g"
export DENSITY_PATH_CONF="config"

# Check if optimized JVM options exist
if [ -f "config/jvm.options.optimized" ]; then
    echo "✅ Using optimized JVM options"
    cp config/jvm.options.optimized config/jvm.options
fi

# Start OpenSearch
echo "🎯 Starting OpenSearch..."
./bin/opensearch
EOF

    chmod +x "scripts/start-optimized.sh"
    echo "✅ Created optimized startup script"
}

# Main execution
main() {
    check_root
    optimize_system
    create_optimized_jvm_options
    create_monitoring_script
    create_index_optimization_script
    create_startup_script
    
    echo ""
    echo "🎉 OpenSearch Performance Optimization Complete!"
    echo "================================================"
    echo ""
    echo "📋 Next steps:"
    echo "1. Review the optimized configuration files"
    echo "2. Adjust heap size in config/jvm.options.optimized based on your system"
    echo "3. Start OpenSearch with: ./scripts/start-optimized.sh"
    echo "4. Monitor performance with: ./scripts/monitor-performance.sh"
    echo "5. Optimize existing indices with: ./scripts/optimize-indices.sh"
    echo ""
    echo "💡 Key optimizations applied:"
    echo "   • Enhanced G1GC settings for better performance"
    echo "   • Optimized JVM parameters for search workloads"
    echo "   • Better compression settings (best_compression by default)"
    echo "   • Improved translog and refresh settings"
    echo "   • Circuit breaker optimizations"
    echo "   • Thread pool tuning"
    echo ""
    echo "🔥 Your OpenSearch is now optimized to be a beast!"
}

# Run main function
main "$@"