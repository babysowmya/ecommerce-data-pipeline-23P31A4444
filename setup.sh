#!/bin/bash

echo "Setting up E-Commerce Data Pipeline Environment..."

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Load environment variables
if [ -f .env ]; then
  export $(cat .env | xargs)
fi

echo "Environment setup completed."