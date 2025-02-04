#!/bin/bash

# Add all changes
git add .

# Commit with timestamp
git commit -m "Phase 1 Complete: Infrastructure & Security Setup $(date)"

# Push to main branch
git push origin main 