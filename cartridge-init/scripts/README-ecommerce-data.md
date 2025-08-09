# E-Commerce Database Data

This directory contains comprehensive e-commerce database schema and sample data for PostgreSQL, designed to work with the `cartridge-init` system.

## 📁 Files

- `ecommerce-schema.sql` - Complete e-commerce database schema
- `ecommerce-sample-data.sql` - Realistic sample data for testing
- `load-ecommerce-data.sh` - Automated script to load data into PostgreSQL
- `README-ecommerce-data.md` - This documentation file

## 🗄️ Database Schema

The e-commerce schema includes the following tables:

### Core Tables
- **`ecommerce.users`** - User accounts and profiles
- **`ecommerce.user_addresses`** - Shipping and billing addresses
- **`ecommerce.categories`** - Product categories with hierarchical structure
- **`ecommerce.products`** - Product catalog with pricing and inventory
- **`ecommerce.product_images`** - Product images and media
- **`ecommerce.product_variants`** - Product variants (size, color, etc.)

### Order Management
- **`ecommerce.orders`** - Customer orders with status tracking
- **`ecommerce.order_items`** - Individual items in orders
- **`ecommerce.cart_items`** - Shopping cart functionality

### Customer Experience
- **`ecommerce.product_reviews`** - Customer reviews and ratings
- **`ecommerce.coupons`** - Discount codes and promotions
- **`ecommerce.coupon_usage`** - Coupon usage tracking
- **`ecommerce.wishlist_items`** - Customer wishlists

### Analytics
- **`analytics.page_views`** - Website page view tracking
- **`analytics.product_views`** - Product view analytics

## 📊 Sample Data

The sample data includes:

### Users (5 records)
- John Doe, Jane Smith, Mike Johnson, Sarah Wilson, David Brown
- Realistic email addresses and contact information
- Addresses for shipping and billing

### Categories (9 records)
- Electronics (parent category)
  - Computers & Laptops
  - Smartphones
- Clothing (parent category)
  - Men's Clothing
  - Women's Clothing
- Home & Garden
- Books
- Sports & Outdoors

### Products (15 records)
- **Electronics**: MacBook Pro, Dell XPS 15, iPhone 15 Pro, Samsung Galaxy S24, Sony Headphones
- **Clothing**: T-shirts, Summer Dress, Jeans, Blouse
- **Home & Garden**: KitchenAid Mixer, Garden Tools
- **Books**: Programming Guide, Data Science Handbook
- **Sports**: Nike Running Shoes, Yoga Mat

### Orders (5 records)
- Various order statuses (pending, confirmed, processing, shipped, delivered)
- Different payment methods (credit card, PayPal)
- Realistic pricing and tax calculations

### Additional Data
- Product reviews and ratings
- Coupon codes and discounts
- Shopping cart items
- Wishlist items
- Analytics data (page views, product views)

## 🚀 Quick Start

### Prerequisites
- PostgreSQL running on localhost:5432
- Database `cartridge` created
- User `cartridge` with password `cartridge`

### Load Data Automatically
```bash
# Make script executable (if not already done)
chmod +x scripts/load-ecommerce-data.sh

# Run the data loading script
./scripts/load-ecommerce-data.sh
```

### Load Data Manually
```bash
# Load schema
psql postgresql://cartridge:cartridge@localhost:5432/cartridge -f scripts/ecommerce-schema.sql

# Load sample data
psql postgresql://cartridge:cartridge@localhost:5432/cartridge -f scripts/ecommerce-sample-data.sql
```

## 🔍 Sample Queries

### Basic Counts
```sql
-- Count users
SELECT COUNT(*) FROM ecommerce.users;

-- Count products
SELECT COUNT(*) FROM ecommerce.products;

-- Count orders
SELECT COUNT(*) FROM ecommerce.orders;
```

### Featured Products
```sql
SELECT name, price, stock_quantity 
FROM ecommerce.products 
WHERE is_featured = true;
```

### Product Categories
```sql
SELECT c.name as category, COUNT(p.product_id) as product_count
FROM ecommerce.categories c
LEFT JOIN ecommerce.products p ON c.category_id = p.category_id
GROUP BY c.name
ORDER BY product_count DESC;
```

### Order Analysis
```sql
SELECT 
    status,
    COUNT(*) as order_count,
    AVG(total_amount) as avg_order_value
FROM ecommerce.orders
GROUP BY status;
```

### User Activity
```sql
SELECT 
    u.first_name,
    u.last_name,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as total_spent
FROM ecommerce.users u
LEFT JOIN ecommerce.orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.first_name, u.last_name
ORDER BY total_spent DESC;
```

## 🏗️ Schema Features

### Indexes
- Performance indexes on frequently queried columns
- Full-text search indexes on product names and descriptions
- Composite indexes for complex queries

### Triggers
- Automatic `updated_at` timestamp updates
- Data integrity constraints

### Extensions
- `uuid-ossp` for UUID generation
- `pg_trgm` for trigram matching (full-text search)

## 🔗 Connection String

The data is designed to work with this connection string:
```
postgresql://cartridge:cartridge@localhost:5432/cartridge
```

## 📈 Data Sources

This sample data is inspired by real e-commerce patterns and includes references from:
- [E-Commerce PostgreSQL Schema](https://github.com/larbisahli/e-commerce-database-schema)
- [Dell Store Example](https://github.com/asotolongo/dell_store)
- [PostgreSQL Guide Example Database](https://www.postgresguide.com/setup/example/)

## 🧪 Testing

The data is designed to work with the `cartridge-init` scanner and AI model generation features. You can:

1. **Scan the database** using the scanner API endpoints
2. **Generate dbt models** based on the e-commerce schema
3. **Test AI model generation** with realistic data patterns
4. **Validate analytics** with the included analytics data

## 🔧 Customization

To customize the data for your needs:

1. **Modify `ecommerce-schema.sql`** to add/remove tables
2. **Update `ecommerce-sample-data.sql`** to change sample data
3. **Adjust connection details** in `load-ecommerce-data.sh`
4. **Add your own data** by extending the sample data file

## 📝 Notes

- All UUIDs are pre-generated for consistency
- Passwords are hashed (using bcrypt format)
- Dates are realistic and recent
- Prices are in USD and realistic for the products
- Stock quantities are varied to test inventory scenarios 