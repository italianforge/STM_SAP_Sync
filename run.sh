#!/bin/bash

ENV=${1:-development}

echo "Running SAP Sync in $ENV environment..."

case $ENV in
    "test")
        ENV=test python sync.py
        ;;
    "prod"|"production")
        ENV=production python sync.py
        ;;
    "dev"|"development"|*)
        ENV=development python sync.py
        ;;
esac
