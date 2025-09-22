#!/bin/bash

# Adjust permissions dynamically
echo "Ensuring correct permissions on $NHM_DIR..."
sudo chown -R $USERNAME:$USERNAME $NHM_DIR
sudo chmod -R 775 $NHM_DIR

# Execute the provided command
exec "$@"
