#!/bin/bash
# Initialize MinIO iceberg bucket for Iceberg tables
# This script runs automatically when docker-compose starts

set -e

# Configuration
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY="admin"
MINIO_SECRET_KEY="password"
MINIO_REGION="us-east-1"

# Bucket name
ICEBERG_BUCKET="iceberg"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to wait for MinIO to be ready
wait_for_minio() {
    print_status "Waiting for MinIO to be ready..."
    until /usr/bin/mc alias set minio $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY; do
        print_warning "MinIO not ready yet, waiting..."
        sleep 2
    done
    print_success "MinIO is ready!"
}

# Function to initialize iceberg bucket
init_iceberg_bucket() {
    print_status "Initializing iceberg bucket..."
    
    # Remove existing iceberg bucket if it exists
    print_status "Removing existing iceberg bucket..."
    /usr/bin/mc rm -r --force minio/$ICEBERG_BUCKET 2>/dev/null || print_warning "No existing iceberg bucket to remove"
    
    # Create iceberg bucket
    print_status "Creating iceberg bucket..."
    /usr/bin/mc mb minio/$ICEBERG_BUCKET
    print_success "Created iceberg bucket: $ICEBERG_BUCKET"
    
    # Set public access policy
    print_status "Setting public access policy..."
    /usr/bin/mc policy set public minio/$ICEBERG_BUCKET
    print_success "Set public access policy for iceberg bucket"
}

# Function to display access information
display_access_info() {
    echo ""
    print_status "Access Information:"
    echo "┌─────────────────────────────────────────────────────────────┐"
    echo "│ MinIO Console: http://localhost:9001                        │"
    echo "│   Username: admin                                           │"
    echo "│   Password: password                                        │"
    echo "│                                                             │"
    echo "│ MinIO API: http://localhost:9000                            │"
    echo "│   Access Key: admin                                         │"
    echo "│   Secret Key: password                                      │"
    echo "│                                                             │"
    echo "└─────────────────────────────────────────────────────────────┘"
}

# Function to list all buckets
list_buckets() {
    echo ""
    print_status "Listing all buckets:"
    /usr/bin/mc ls minio/ || print_error "Could not list buckets"
}

# Main execution
main() {
    echo "🚀 Initializing MinIO Iceberg Bucket..."
    echo "=========================================="
    
    # Wait for MinIO to be ready
    wait_for_minio
    
    # Initialize iceberg bucket
    init_iceberg_bucket
    
    # Display results
    display_access_info
    list_buckets
    
    echo ""
    print_success "MinIO Iceberg bucket initialization complete!"
    echo "====================================================="
}

# Run main function
main "$@" 