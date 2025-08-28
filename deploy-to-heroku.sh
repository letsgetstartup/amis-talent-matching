#!/bin/bash

echo "HEROKU DEPLOYMENT SCRIPT"
echo "========================"

echo "Step 1: Login to Heroku"
echo "Please run: heroku login"
echo "This will open your browser to login to Heroku"
echo ""

echo "Step 2: Create Heroku app (after login)"
echo "heroku create amis-talent-matching-$(date +%s)"
echo ""

echo "Step 3: Set environment variables"
echo "You'll need to set these:"
echo "heroku config:set MONGODB_URL=\"your_mongodb_connection_string\""
echo "heroku config:set OPENAI_API_KEY=\"your_openai_api_key\""
echo "heroku config:set SECRET_KEY=\"your_secret_key\""
echo ""

echo "Step 4: Deploy"
echo "git push heroku main"
echo ""

echo "Step 5: Open your app"
echo "heroku open"
echo ""

echo "Run this script section by section when ready!"
