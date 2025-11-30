import os
import psycopg2
from dotenv import load_dotenv

def env_only_test():
    print("🔧 .ENV ONLY DATABASE TEST")
    print("=" * 40)
    
    # Step 1: Find and load .env file
    print("\n📁 STEP 1: Locating .env file...")
    
    # Try multiple possible locations
    possible_paths = [
        '../.env',                              # From scripts folder
        '.env',                                 # Current directory  
        os.path.join(os.path.dirname(__file__), '../.env'),  # Absolute path
        os.path.abspath('../.env'),            # Absolute path alternative
        os.path.join(os.getcwd(), '.env'),     # Current working directory
        os.path.join(os.getcwd(), '../.env'),  # Parent of current working directory
    ]
    
    env_loaded = False
    loaded_path = None
    
    for path in possible_paths:
        if os.path.exists(path):
            load_dotenv(path)
            env_loaded = True
            loaded_path = path
            print(f"   ✅ .env loaded from: {path}")
            break
        else:
            print(f"   ❌ Not found: {path}")
    
    if not env_loaded:
        print("\n❌ No .env file found! Creating template...")
        create_env_file()
        return
    
    # Step 2: Verify all required environment variables
    print("\n🔍 STEP 2: Checking environment variables...")
    
    required_vars = {
        'DB_HOST': 'Database host',
        'DB_PORT': 'Database port', 
        'DB_NAME': 'Database name',
        'DB_USER': 'Database user',
        'DB_PASSWORD': 'Database password'
    }
    
    missing_vars = []
    for var, description in required_vars.items():
        value = os.getenv(var)
        if value:
            if var == 'DB_PASSWORD':
                print(f"   ✅ {var}: {'*' * len(value)} ({description})")
            else:
                print(f"   ✅ {var}: {value} ({description})")
        else:
            print(f"   ❌ {var}: NOT SET ({description})")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n💡 Missing variables: {missing_vars}")
        print("   Edit your .env file and add these variables")
        return
    
    # Step 3: Test database connection using .env variables
    print("\n🔌 STEP 3: Testing database connection...")
    
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT')),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            sslmode='require',
            connect_timeout=10
        )
        print("   ✅ Database connection successful!")
        
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        print(f"\n💡 Check your .env file at: {loaded_path}")
        return
    
    # Step 4: Check database content
    print("\n📊 STEP 4: Checking database content...")
    
    cur = conn.cursor()
    
    # Get all tables
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    
    tables = [row[0] for row in cur.fetchall()]
    print(f"   Found {len(tables)} tables in database")
    
    # Show table counts
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        status = "✅" if count > 0 else "⚠️ "
        print(f"   {status} {table}: {count} rows")
    
    # Special check for chatplug_stations
    if 'chatplug_stations' in tables:
        print(f"\n🔌 CHATPLUG STATIONS DETAILS:")
        cur.execute("SELECT COUNT(*) FROM chatplug_stations")
        total = cur.fetchone()[0]
        print(f"   Total stations: {total}")
        
        # Show sample data
        cur.execute("SELECT name, city, country_code FROM chatplug_stations LIMIT 3")
        samples = cur.fetchall()
        print("   Sample stations:")
        for name, city, country in samples:
            print(f"      📍 {name} | {city}, {country}")
    
    conn.close()
    
    print(f"\n🎉 .ENV TEST COMPLETED SUCCESSFULLY!")
    print(f"   Config file: {loaded_path}")

def create_env_file():
    """Create a .env file template"""
    env_content = """# Database Configuration - REQUIRED
DB_HOST=db.igegifqtwssiqfkzwtdt.supabase.co
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_actual_password_here

# Supabase Configuration
SUPABASE_URL=https://igegifqtwssiqfkzwtdt.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlnZWdpZnF0d3NzaXFma3p3dGR0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTY4NDE5NTgsImV4cCI6MjA3MjQxNzk1OH0.iX1-hXuh6pMhZRpObHxf85b1l_FoZ-kmITT7fgD-NGE

# JWT Configuration
JWT_SECRET_KEY=supersecretkey12345
JWT_ALGORITHM=HS256
JWT_EXPIRATION_MINUTES=60

# Environment
ENV=development
"""
    
    # Create in the most likely location
    env_path = os.path.abspath('../.env')
    
    with open(env_path, 'w') as f:
        f.write(env_content)
    
    print(f"\n📝 Created .env template at: {env_path}")
    print("💡 INSTRUCTIONS:")
    print("   1. Open the .env file")
    print("   2. Change 'your_actual_password_here' to your real database password")
    print("   3. Save the file")
    print("   4. Run this script again")

def debug_env():
    """Debug function to see what's happening with .env"""
    print("\n🐛 DEBUG: Environment Status")
    print("=" * 30)
    
    # Check current working directory
    print(f"Current directory: {os.getcwd()}")
    
    # Check if .env exists in common locations
    check_paths = [
        '.env',
        '../.env',
        os.path.abspath('.env'),
        os.path.abspath('../.env')
    ]
    
    for path in check_paths:
        exists = os.path.exists(path)
        print(f"{'✅' if exists else '❌'} {path}")

if __name__ == "__main__":
    env_only_test()
    # Uncomment next line for debugging
    # debug_env()