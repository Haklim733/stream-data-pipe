#!/bin/bash

# Set the Polaris configuration file
POLARIS_CONF=polaris.conf

# Set the catalog name
CATALOG_NAME=test_catalog

# Set the role name
ROLE_NAME=test_role

# Run the Polaris script to create the catalog
polaris --conf $POLARIS_CONF catalog create $CATALOG_NAME

# Run the Polaris script to create the role
polaris --conf $POLARIS_CONF role create $ROLE_NAME

# Run the Polaris script to grant privileges to the role
polaris --conf $POLARIS_CONF role grant $ROLE_NAME USAGE ON CATALOG $CATALOG_NAME

echo "Catalog and role setup complete!"