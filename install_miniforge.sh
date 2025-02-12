#!/bin/bash
set -e  # Exit immediately if any command fails

# Define variables
INSTALL_DIR="${HOME}/miniforge3"
INSTALLER="Miniforge3-Linux-x86_64.sh"
MINIFORGE_URL="https://github.com/conda-forge/miniforge/releases/latest/download/${INSTALLER}"

# Check if Miniforge is already installed
if [ -d "${INSTALL_DIR}" ]; then
  echo "Miniforge is already installed in ${INSTALL_DIR}."
  exit 0
fi

# Download the installer
echo "Downloading Miniforge installer from ${MINIFORGE_URL}..."
curl -LO ${MINIFORGE_URL}

# Make the installer executable
chmod +x ${INSTALLER}

# Run the installer in batch mode (-b) with the specified installation directory (-p)
echo "Installing Miniforge to ${INSTALL_DIR}..."
./${INSTALLER} -b -p "${INSTALL_DIR}"

# Initialize conda for bash; this adds the necessary commands to your shell configuration.
echo "Initializing conda for bash..."
"${INSTALL_DIR}/bin/conda" init bash

echo "Miniforge installation completed successfully."
