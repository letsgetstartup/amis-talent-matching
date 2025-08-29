# MongoDB Atlas Setup for Heroku

Since your app needs a cloud MongoDB connection, I'll guide you through setting up MongoDB Atlas (free tier).

## Quick Setup Steps:

1. **Go to MongoDB Atlas**: https://cloud.mongodb.com/
2. **Sign up/Login** with your email
3. **Create a free cluster**:
   - Choose "M0 Sandbox" (Free tier)
   - Select a cloud provider region (choose closest to your users)
   - Name your cluster (e.g., "amis-cluster")

4. **Configure Database Access**:
   - Go to "Database Access" in the left sidebar
   - Click "Add New Database User"
   - Choose "Password" authentication
   - Create username and password (save these!)
   - Grant "Atlas admin" privileges

5. **Configure Network Access**:
   - Go to "Network Access" in the left sidebar
   - Click "Add IP Address"
   - Choose "Allow access from anywhere" (0.0.0.0/0)
   - This allows Heroku to connect

6. **Get Connection String**:
   - Go to "Clusters" and click "Connect"
   - Choose "Connect your application"
   - Select "Python" and version "3.6 or later"
   - Copy the connection string (looks like: mongodb+srv://username:password@cluster.mongodb.net/myFirstDatabase?retryWrites=true&w=majority)

## Alternative: I can set up a temporary MongoDB for testing

If you want to get the app running quickly, I can set up a temporary MongoDB connection for testing purposes.
