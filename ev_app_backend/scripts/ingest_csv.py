import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
import numpy as np

# Load environment variables
load_dotenv('../.env')  # Load from root directory

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SupabaseCSVIngestor:
    def __init__(self):
        # Use the direct Supabase connection string from your .env
        self.database_url = os.getenv('DATABASE_URL')
        
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in environment variables")
        
        self.engine = create_engine(self.database_url)
        self.data_dir = '../data'
        
    def test_connection(self):
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()
                logger.info(f"✅ Connected to PostgreSQL: {version[0]}")
                return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def create_tables(self):
        """Create tables optimized for Supabase with proper data types"""
        create_tables_sql = """
        -- Enable required extensions for Supabase
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        CREATE EXTENSION IF NOT EXISTS "vector";
        
        -- Charging sessions table (from your schema)
        CREATE TABLE IF NOT EXISTS charging_sessions (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            vehicle_model TEXT,
            battery_bwh NUMERIC,
            station_id TEXT,
            energy_bwh NUMERIC,
            charging_duration_min NUMERIC,
            charging_rate_bw NUMERIC,
            charging_cost NUMERIC,
            time_of_day TEXT,
            day_of_week TEXT,
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            distance_km NUMERIC,
            temperature_c NUMERIC,
            vehicle_age_years NUMERIC,
            charger_type TEXT,
            user_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Charging stations table
        CREATE TABLE IF NOT EXISTS charging_stations (
            id TEXT PRIMARY KEY,
            source_id TEXT,
            name TEXT,
            city TEXT,
            state TEXT,
            connector_types JSONB,
            max_power_kw NUMERIC,
            status TEXT,
            raw_data JSONB,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            location GEOMETRY(Point, 4326),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Electric vehicles table
        CREATE TABLE IF NOT EXISTS electric_vehicles (
            id TEXT PRIMARY KEY,
            vehicle_model TEXT,
            battery_capacity_kwh NUMERIC,
            range_km NUMERIC,
            charging_speed_kw NUMERIC,
            price_usd NUMERIC,
            year INTEGER,
            manufacturer TEXT,
            vehicle_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Telemetry table
        CREATE TABLE IF NOT EXISTS telemetry (
            id TEXT PRIMARY KEY,
            vehicle_id TEXT,
            timestamp TIMESTAMP,
            battery_level NUMERIC,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            speed_kmh NUMERIC,
            temperature_c NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_charging_sessions_user_id ON charging_sessions(user_id);
        CREATE INDEX IF NOT EXISTS idx_charging_sessions_station_id ON charging_sessions(station_id);
        CREATE INDEX IF NOT EXISTS idx_charging_sessions_created_at ON charging_sessions(created_at);
        CREATE INDEX IF NOT EXISTS idx_charging_stations_location ON charging_stations USING GIST(location);
        CREATE INDEX IF NOT EXISTS idx_telemetry_vehicle_id ON telemetry(vehicle_id);
        CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry(timestamp);
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_tables_sql))
                conn.commit()
            logger.info("✅ Tables created successfully with indexes")
        except Exception as e:
            logger.error(f"❌ Error creating tables: {e}")
    
    def clean_column_names(self, df):
        """Clean column names to be PostgreSQL compatible"""
        df.columns = [col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '') 
                     for col in df.columns]
        return df
    
    def handle_nan_values(self, df):
        """Replace NaN values with None for proper PostgreSQL handling"""
        return df.replace({np.nan: None})
    
    def map_data_types(self, df, table_name):
        """Map pandas data types to appropriate PostgreSQL types"""
        type_mapping = {}
        
        for column in df.columns:
            dtype = str(df[column].dtype)
            
            if 'int' in dtype:
                type_mapping[column] = 'INTEGER'
            elif 'float' in dtype:
                type_mapping[column] = 'NUMERIC'
            elif 'bool' in dtype:
                type_mapping[column] = 'BOOLEAN'
            elif 'datetime' in dtype:
                type_mapping[column] = 'TIMESTAMP'
            else:
                type_mapping[column] = 'TEXT'
        
        return type_mapping
    
    def ingest_csv(self, csv_file, table_name):
        """Ingest a single CSV file into Supabase PostgreSQL"""
        csv_path = os.path.join(self.data_dir, csv_file)
        
        if not os.path.exists(csv_path):
            logger.warning(f"⚠️ CSV file not found: {csv_path}")
            return
        
        try:
            # Read CSV file with error handling
            logger.info(f"📖 Reading {csv_file}...")
            
            # Try different encodings if needed
            try:
                df = pd.read_csv(csv_path)
            except UnicodeDecodeError:
                df = pd.read_csv(csv_path, encoding='latin-1')
            
            # Clean data
            df = self.clean_column_names(df)
            df = self.handle_nan_values(df)
            
            # Log basic info
            logger.info(f"📊 File: {csv_file}, Rows: {len(df)}, Columns: {len(df.columns)}")
            logger.info(f"📋 Sample columns: {list(df.columns)[:5]}...")
            
            # Data type mapping
            type_mapping = self.map_data_types(df, table_name)
            logger.info(f"🔧 Data types: {dict(list(type_mapping.items())[:3])}...")
            
            # Ingest data in chunks for large files
            chunk_size = 1000
            total_rows = len(df)
            
            for i in range(0, total_rows, chunk_size):
                chunk = df[i:i + chunk_size]
                chunk.to_sql(
                    table_name, 
                    self.engine, 
                    if_exists='append' if i > 0 else 'replace',
                    index=False,
                    method='multi'
                )
                logger.info(f"✅ Chunk {i//chunk_size + 1} ingested ({len(chunk)} rows)")
            
            logger.info(f"🎯 Successfully ingested {total_rows} rows into {table_name}")
            
            # Verify data count
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.fetchone()[0]
                logger.info(f"🔍 Verification: {count} rows in {table_name} table")
            
        except Exception as e:
            logger.error(f"❌ Error ingesting {csv_file}: {e}")
            # Log first few rows for debugging
            try:
                logger.info(f"🐛 First 3 rows sample: {df.head(3).to_dict()}")
            except:
                pass
    
    def ingest_all_csvs(self):
        """Ingest all CSV files in the data directory"""
        if not self.test_connection():
            return
        
        # Create tables
        self.create_tables()
        
        # Map CSV files to table names
        csv_mapping = {
            'charging_patterns.csv': 'charging_sessions',
            'charging_stations.csv': 'charging_stations', 
            'electric_vehicles_spec_2025.csv': 'electric_vehicles',
            'telemetry.csv': 'telemetry'
        }
        
        for csv_file, table_name in csv_mapping.items():
            logger.info(f"\n{'='*50}")
            logger.info(f"🔄 Processing: {csv_file} -> {table_name}")
            logger.info(f"{'='*50}")
            self.ingest_csv(csv_file, table_name)
        
        logger.info("\n🎉 All CSV files processed successfully!")
        
        # Final summary
        self.generate_summary()
    
    def generate_summary(self):
        """Generate a summary of ingested data"""
        try:
            with self.engine.connect() as conn:
                tables = ['charging_sessions', 'charging_stations', 'electric_vehicles', 'telemetry']
                
                logger.info("\n📊 DATABASE SUMMARY:")
                logger.info("-" * 30)
                
                for table in tables:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    logger.info(f"📈 {table}: {count:,} rows")
                
        except Exception as e:
            logger.error(f"Error generating summary: {e}")

def main():
    """Main function"""
    try:
        ingestor = SupabaseCSVIngestor()
        ingestor.ingest_all_csvs()
    except Exception as e:
        logger.error(f"🚨 Fatal error: {e}")

if __name__ == "__main__":
    main()