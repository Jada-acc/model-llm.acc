#!/bin/bash

# Add all changes
git add .

# Create detailed commit message
cat << EOF > commit_message.txt
Phase 1 Complete: Infrastructure & Security Setup

Features:
- Implemented secure data pipeline with encryption
- Added JWT-based authentication
- Added rate limiting for failed attempts
- Added secure data storage and retrieval
- Added compression for old data
- Added detailed logging and debugging

Security Components:
- AuthManager: JWT token management
- DataEncryption: Data encryption/decryption
- SecurePipeline: Rate limiting and auth checks
- DataRetriever: Secure data access

Testing:
- Added integration tests
- Added security flow tests
- Added debug logging
EOF

# Commit with the detailed message
git commit -F commit_message.txt

# Push to main branch
git push origin main

# Clean up
rm commit_message.txt 