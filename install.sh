#!/bin/bash
# ====================
# perceptra-storage/install.sh - Installation Script
# ====================

set -e

echo "=================================="
echo "Perceptra Storage - Installation"
echo "=================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check Python version
echo "Checking Python version..."
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
REQUIRED_VERSION="3.9"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    echo -e "${RED}✗ Python 3.9+ required. Current: $PYTHON_VERSION${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python $PYTHON_VERSION${NC}"

# Create virtual environment
echo ""
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
else
    echo -e "${YELLOW}! Virtual environment already exists${NC}"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip wheel setuptools
echo -e "${GREEN}✓ pip upgraded${NC}"

# Install package
echo ""
echo "Select installation type:"
echo "1) Core only (local storage)"
echo "2) With S3 support"
echo "3) With Azure support"
echo "4) With MinIO support"
echo "5) All backends"
echo "6) Development (all + dev tools)"
echo ""
read -p "Enter choice [1-6]: " choice

case $choice in
    1)
        echo "Installing core package..."
        pip install -e .
        ;;
    2)
        echo "Installing with S3 support..."
        pip install -e ".[s3]"
        ;;
    3)
        echo "Installing with Azure support..."
        pip install -e ".[azure]"
        ;;
    4)
        echo "Installing with MinIO support..."
        pip install -e ".[minio]"
        ;;
    5)
        echo "Installing all backends..."
        pip install -e ".[all]"
        ;;
    6)
        echo "Installing development environment..."
        pip install -e ".[all,dev]"
        ;;
    *)
        echo -e "${RED}Invalid choice. Exiting.${NC}"
        exit 1
        ;;
esac

echo -e "${GREEN}✓ Package installed${NC}"

# Run tests
echo ""
read -p "Run tests? [y/N]: " run_tests
if [ "$run_tests" = "y" ] || [ "$run_tests" = "Y" ]; then
    echo "Running tests..."
    pytest tests/ -v
    echo -e "${GREEN}✓ Tests completed${NC}"
fi

echo ""
echo "=================================="
echo -e "${GREEN}Installation Complete!${NC}"
echo "=================================="
echo ""
echo "Next steps:"
echo "1. Activate the virtual environment:"
echo "   source venv/bin/activate"
echo ""
echo "2. Try the examples:"
echo "   python examples/01_basic_usage.py"
echo ""
echo "3. Read the quick start guide:"
echo "   cat QUICKSTART.md"
echo ""