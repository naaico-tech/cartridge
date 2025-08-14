// MongoDB initialization script for cartridge-warp development

// Switch to ecommerce database
db = db.getSiblingDB('ecommerce');

// Create sample collections with data
db.products.insertMany([
    {
        _id: ObjectId(),
        name: "Laptop Pro",
        category: "Electronics",
        price: 1299.99,
        stock: 50,
        created_at: new Date(),
        updated_at: new Date()
    },
    {
        _id: ObjectId(),
        name: "Wireless Mouse",
        category: "Electronics", 
        price: 29.99,
        stock: 100,
        created_at: new Date(),
        updated_at: new Date()
    },
    {
        _id: ObjectId(),
        name: "Coffee Mug",
        category: "Home",
        price: 12.99,
        stock: 200,
        created_at: new Date(),
        updated_at: new Date()
    }
]);

db.orders.insertMany([
    {
        _id: ObjectId(),
        order_number: "ORD-001",
        customer_id: ObjectId(),
        items: [
            { product_id: ObjectId(), quantity: 1, price: 1299.99 }
        ],
        total_amount: 1299.99,
        status: "pending",
        created_at: new Date(),
        updated_at: new Date()
    },
    {
        _id: ObjectId(),
        order_number: "ORD-002", 
        customer_id: ObjectId(),
        items: [
            { product_id: ObjectId(), quantity: 2, price: 29.99 },
            { product_id: ObjectId(), quantity: 1, price: 12.99 }
        ],
        total_amount: 72.97,
        status: "completed",
        created_at: new Date(),
        updated_at: new Date()
    }
]);

db.customers.insertMany([
    {
        _id: ObjectId(),
        email: "john.doe@example.com",
        first_name: "John",
        last_name: "Doe",
        phone: "+1-555-0123",
        address: {
            street: "123 Main St",
            city: "Anytown",
            state: "CA",
            zip: "12345"
        },
        created_at: new Date(),
        updated_at: new Date()
    },
    {
        _id: ObjectId(),
        email: "jane.smith@example.com",
        first_name: "Jane",
        last_name: "Smith", 
        phone: "+1-555-0456",
        address: {
            street: "456 Oak Ave",
            city: "Somewhere",
            state: "NY",
            zip: "67890"
        },
        created_at: new Date(),
        updated_at: new Date()
    }
]);

// Create indexes for change detection
db.products.createIndex({ "updated_at": 1 });
db.orders.createIndex({ "updated_at": 1 });
db.customers.createIndex({ "updated_at": 1 });

print("MongoDB initialized with sample ecommerce data");
