#!/bin/bash

echo "📦 MONGODB DATA MIGRATION SCRIPT"
echo "================================="
echo ""
echo "This will dump your local MongoDB and restore it to Atlas"
echo ""

# Local MongoDB settings
LOCAL_URI="mongodb://localhost:27017"
LOCAL_DB="talent_match"

# Atlas MongoDB settings  
ATLAS_URI="mongodb+srv://a_db_user:ZhNMqELhwIphBte9@cluster0.ugkdlhn.mongodb.net/talent_match?retryWrites=true&w=majority&appName=Cluster0"
ATLAS_DB="talent_match"

# Create backup directory
BACKUP_DIR="./mongodb_backup"
mkdir -p $BACKUP_DIR

echo "🔄 Step 1: Dumping local MongoDB..."
mongodump --uri="$LOCAL_URI" --db="$LOCAL_DB" --out="$BACKUP_DIR"

if [ $? -eq 0 ]; then
    echo "✅ Local dump completed"
    echo ""
    echo "🚀 Step 2: Restoring to MongoDB Atlas..."
    mongorestore --uri="$ATLAS_URI" --db="$ATLAS_DB" "$BACKUP_DIR/$LOCAL_DB" --drop
    
    if [ $? -eq 0 ]; then
        echo "✅ Migration completed successfully!"
        echo ""
        echo "📊 Cleaning up..."
        rm -rf $BACKUP_DIR
        echo "✅ Backup files cleaned"
        echo ""
        echo "🎉 Your local data is now on MongoDB Atlas!"
    else
        echo "❌ Restore failed. Check Atlas connection."
    fi
else
    echo "❌ Local dump failed. Is MongoDB running locally?"
fi
