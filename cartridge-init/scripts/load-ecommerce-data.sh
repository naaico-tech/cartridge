#!/bin/bash

# Load E-Commerce Data into PostgreSQL
# This script loads the e-commerce schema and sample data into the database

set -e

# Database connection details
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="cartridge"
DB_USER="cartridge"
DB_PASSWORD="cartridge"

# Connection string
CONNECTION_STRING="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

echo "🚀 Loading E-Commerce Data into PostgreSQL..."
echo "📊 Database: ${DB_NAME}"
echo "🔗 Connection: ${CONNECTION_STRING}"
echo ""

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo "❌ Error: psql command not found. Please install PostgreSQL client tools."
    exit 1
fi

# Function to execute SQL file
execute_sql_file() {
    local file=$1
    local description=$2
    
    if [ -f "$file" ]; then
        echo "📝 Executing: $description"
        echo "   File: $file"
        
        # Execute the SQL file
        PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f "$file"
        
        if [ $? -eq 0 ]; then
            echo "✅ Successfully executed: $description"
        else
            echo "❌ Failed to execute: $description"
            exit 1
        fi
        echo ""
    else
        echo "❌ Error: File not found: $file"
        exit 1
    fi
}

# Execute schema creation
execute_sql_file "scripts/ecommerce-schema.sql" "E-Commerce Database Schema"

# Execute sample data insertion
execute_sql_file "scripts/ecommerce-sample-data.sql" "E-Commerce Sample Data"

echo "🎉 E-Commerce data loading completed successfully!"
echo ""
echo "📈 Data Summary:"
echo "   - Users: 5 sample users"
echo "   - Categories: 9 categories (including subcategories)"
echo "   - Products: 15 products across different categories"
echo "   - Orders: 5 sample orders"
echo "   - Reviews: 5 product reviews"
echo "   - Coupons: 4 discount coupons"
echo "   - Analytics: Page views and product views data"
echo ""
echo "🔍 You can now connect to the database using:"
echo "   psql $CONNECTION_STRING"
echo ""
echo "📊 Sample queries to test the data:"
echo "   SELECT COUNT(*) FROM ecommerce.users;"
echo "   SELECT COUNT(*) FROM ecommerce.products;"
echo "   SELECT COUNT(*) FROM ecommerce.orders;"
echo "   SELECT name, price, stock_quantity FROM ecommerce.products WHERE is_featured = true;" 