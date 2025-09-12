#!/usr/bin/env python3
"""
Demo script for Database Swap functionality.
This script creates sample databases and demonstrates migration capabilities.
"""

import sqlite3
import os
import sys
import subprocess
import tempfile

def create_sample_database(db_path: str) -> None:
    """Create a sample SQLite database with test data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER,
        department TEXT,
        salary REAL,
        is_active BOOLEAN,
        created_at TEXT
    )
    ''')
    
    # Create products table
    cursor.execute('''
    CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        price REAL,
        category TEXT,
        stock_quantity INTEGER,
        is_available BOOLEAN
    )
    ''')
    
    # Insert sample users
    users_data = [
        (1, 'Alice Johnson', 'alice@company.com', 30, 'Engineering', 75000.0, True, '2023-01-15'),
        (2, 'Bob Smith', 'bob@company.com', 25, 'Marketing', 55000.0, True, '2023-02-20'),
        (3, 'Carol Davis', 'carol@company.com', 35, 'Engineering', 82000.0, True, '2023-03-10'),
        (4, 'David Wilson', 'david@company.com', 28, 'Sales', 60000.0, False, '2023-04-05'),
        (5, 'Eve Brown', 'eve@company.com', 32, 'HR', 65000.0, True, '2023-05-15'),
    ]
    
    # Insert sample products
    products_data = [
        (1, 'Laptop Pro', 'High-performance laptop', 1299.99, 'Electronics', 50, True),
        (2, 'Wireless Mouse', 'Ergonomic wireless mouse', 29.99, 'Electronics', 200, True),
        (3, 'Office Chair', 'Comfortable office chair', 249.99, 'Furniture', 30, True),
        (4, 'USB Monitor', '27-inch 4K monitor', 399.99, 'Electronics', 25, True),
        (5, 'Mechanical Keyboard', 'RGB mechanical keyboard', 149.99, 'Electronics', 75, True),
        (6, 'Desk Lamp', 'LED desk lamp', 79.99, 'Furniture', 100, True),
        (7, 'Webcam HD', '1080p webcam', 89.99, 'Electronics', 60, False),
    ]
    
    cursor.executemany('INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?)', users_data)
    cursor.executemany('INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?)', products_data)
    
    conn.commit()
    conn.close()
    
    print(f"✓ Created sample database: {db_path}")
    print(f"  - Users: {len(users_data)} records")
    print(f"  - Products: {len(products_data)} records")

def run_command(cmd: list) -> bool:
    """Run a command and return success status."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Command failed: {' '.join(cmd)}")
        print(f"  Error: {e.stderr}")
        return False

def main():
    """Main demo function."""
    print("=" * 60)
    print("DATABASE SWAP DEMONSTRATION")
    print("=" * 60)
    
    # Create temporary directory for demo
    with tempfile.TemporaryDirectory() as temp_dir:
        source_db = os.path.join(temp_dir, "demo_source.db")
        target_db = os.path.join(temp_dir, "demo_target.db")
        config_file = os.path.join(temp_dir, "demo_config.yaml")
        
        print("\n1. Creating sample source database...")
        create_sample_database(source_db)
        
        print("\n2. Testing database connection...")
        cmd = [
            sys.executable, "-m", "database_swap.main",
            "test-connection", "--db-type", "sqlite", "--database", source_db
        ]
        if not run_command(cmd):
            return 1
        
        print("\n3. Analyzing source database structure...")
        cmd = [
            sys.executable, "-m", "database_swap.main",
            "analyze", "--db-type", "sqlite", "--database", source_db
        ]
        if not run_command(cmd):
            return 1
        
        print("\n4. Performing dry run migration...")
        cmd = [
            sys.executable, "-m", "database_swap.main",
            "migrate", "--dry-run",
            "--source-type", "sqlite", "--source-database", source_db,
            "--target-type", "sqlite", "--target-database", target_db
        ]
        if not run_command(cmd):
            return 1
        
        print("\n5. Performing actual migration...")
        cmd = [
            sys.executable, "-m", "database_swap.main",
            "migrate",
            "--source-type", "sqlite", "--source-database", source_db,
            "--target-type", "sqlite", "--target-database", target_db,
            "--batch-size", "3", "--rate-limit-delay", "0.1"
        ]
        if not run_command(cmd):
            return 1
        
        print("\n6. Verifying target database...")
        cmd = [
            sys.executable, "-m", "database_swap.main",
            "analyze", "--db-type", "sqlite", "--database", target_db
        ]
        if not run_command(cmd):
            return 1
        
        print("\n7. Testing selective table migration...")
        selective_target = os.path.join(temp_dir, "selective_target.db")
        cmd = [
            sys.executable, "-m", "database_swap.main",
            "migrate",
            "--source-type", "sqlite", "--source-database", source_db,
            "--target-type", "sqlite", "--target-database", selective_target,
            "--tables", "users"
        ]
        if not run_command(cmd):
            return 1
        
        print("\n8. Verifying selective migration...")
        cmd = [
            sys.executable, "-m", "database_swap.main",
            "analyze", "--db-type", "sqlite", "--database", selective_target
        ]
        if not run_command(cmd):
            return 1
        
        print("\n" + "=" * 60)
        print("DEMONSTRATION COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("\nKey features demonstrated:")
        print("✓ Database connection testing")
        print("✓ Database structure analysis")
        print("✓ Dry run migration (validation)")
        print("✓ Full database migration")
        print("✓ Selective table migration")
        print("✓ Batch processing and rate limiting")
        print("✓ Progress tracking and statistics")
        
        return 0

if __name__ == "__main__":
    sys.exit(main())