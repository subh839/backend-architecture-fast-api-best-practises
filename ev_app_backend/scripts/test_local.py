import psycopg2
import os
from dotenv import load_dotenv

load_dotenv('.env')

def test_local_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT') or 5432),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        print(" Local database connection successful!")
        
        # Create tables if they don't exist
        cur = conn.cursor()
        
        # Create users table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR UNIQUE NOT NULL,
                hashed_password VARCHAR NOT NULL,
                full_name VARCHAR,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create a test user
        cur.execute("""
            INSERT INTO users (email, hashed_password, full_name) 
            VALUES ('test@example.com', 'hashed_password', 'Test User')
            ON CONFLICT (email) DO NOTHING
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        
        print("Local database setup completed!")
        
    except Exception as e:
        print(f" Local database connection failed: {e}")
        print("Make sure PostgreSQL is running on localhost:5432")

if __name__ == "__main__":
    test_local_db()