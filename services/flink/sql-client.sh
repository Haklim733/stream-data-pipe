#!/bin/bash

# Script to start Flink SQL client with Iceberg support
# This script should be run from the flink service container

echo "Starting Flink SQL Client with Iceberg support..."

# Check if we're in the Flink container
if [ ! -f "/opt/flink/bin/sql-client.sh" ]; then
    echo "Error: This script must be run from within the Flink container"
    echo "Run: docker compose exec flink bash"
    echo "Then run: ./sql-client.sh"
    exit 1
fi

# Set environment variables for Iceberg
export FLINK_PLUGINS_DIR=/opt/flink/plugins
export FLINK_OPT_DIR=/opt/flink/opt

# Start the SQL client in embedded mode
echo "Starting SQL client in embedded mode..."
echo "Available JARs in /opt/flink/lib:"
ls -la /opt/flink/lib/

echo ""
echo "Starting Flink SQL Client..."
echo "You can now run Iceberg commands like:"
echo "  CREATE CATALOG iceberg WITH ('type'='iceberg', 'catalog-type'='rest', 'uri'='http://iceberg-rest:8181');"
echo "  SHOW CATALOGS;"
echo "  USE CATALOG iceberg;"
echo "  SHOW DATABASES;"
echo ""

/opt/flink/bin/sql-client.sh embedded 