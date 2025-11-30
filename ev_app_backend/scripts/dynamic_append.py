import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StationsDataAppender:
    def __init__(self):
        # Load environment variables
        load_dotenv('.env')
        
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT') or 5432),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            sslmode='require'
        )
        self.data_dir = '../data'
        self.MAX_ROWS = 100000  # Limit to 5,000 rows
    
    def find_csv_file(self, csv_file):
        """Find CSV file in multiple possible locations"""
        possible_paths = [
            f"../data/{csv_file}",    # From scripts folder
            f"./data/{csv_file}",     # Current directory data folder
            f"./{csv_file}",          # Current directory
            f"../{csv_file}",         # Parent directory
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"✅ Found CSV file: {path}")
                return path
        
        logger.error(f"❌ CSV file not found: {csv_file}")
        return None
    
    def map_to_stations_schema(self, df):
        """Map CSV data to stations table schema"""
        mapped_data = []
        
        # Limit the dataframe to MAX_ROWS
        limited_df = df.head(self.MAX_ROWS)
        
        for _, row in limited_df.iterrows():
            station_data = {
                'station_id': None,
                'name': None,
                'lat': None,
                'lon': None,
                'address': None,
                'city': None,
                'country': None,
                'power_class_kw': None,
                'connectors': None
            }
            
            # Map columns based on available data
            if 'id' in df.columns:
                station_data['station_id'] = str(row['id'])
            elif 'station_id' in df.columns:
                station_data['station_id'] = str(row['station_id'])
            
            if 'name' in df.columns:
                station_data['name'] = row['name']
            
            if 'latitude' in df.columns:
                station_data['lat'] = float(row['latitude']) if pd.notna(row['latitude']) else None
            elif 'lat' in df.columns:
                station_data['lat'] = float(row['lat']) if pd.notna(row['lat']) else None
            
            if 'longitude' in df.columns:
                station_data['lon'] = float(row['longitude']) if pd.notna(row['longitude']) else None
            elif 'lon' in df.columns:
                station_data['lon'] = float(row['lon']) if pd.notna(row['lon']) else None
            
            if 'city' in df.columns:
                station_data['city'] = row['city']
            
            if 'country_code' in df.columns:
                station_data['country'] = row['country_code']
            elif 'country' in df.columns:
                station_data['country'] = row['country']
            
            if 'power_kw' in df.columns:
                station_data['power_class_kw'] = float(row['power_kw']) if pd.notna(row['power_kw']) else None
            elif 'power_class_kw' in df.columns:
                station_data['power_class_kw'] = float(row['power_class_kw']) if pd.notna(row['power_class_kw']) else None
            
            if 'power_class' in df.columns:
                station_data['connectors'] = row['power_class']
            elif 'connector_types' in df.columns:
                station_data['connectors'] = row['connector_types']
            
            # Create address from available fields
            address_parts = []
            if 'name' in df.columns and pd.notna(row['name']):
                address_parts.append(str(row['name']))
            if 'city' in df.columns and pd.notna(row['city']):
                address_parts.append(str(row['city']))
            if 'state_province' in df.columns and pd.notna(row['state_province']):
                address_parts.append(str(row['state_province']))
            
            if address_parts:
                station_data['address'] = ', '.join(address_parts)
            
            mapped_data.append(station_data)
        
        return mapped_data
    
    def get_existing_station_ids(self):
        """Get existing station_ids to avoid duplicates"""
        cur = self.conn.cursor()
        cur.execute("SELECT station_id FROM stations WHERE station_id IS NOT NULL")
        existing_ids = [str(row[0]) for row in cur.fetchall()]
        cur.close()
        return existing_ids
    
    def append_to_stations(self, csv_file):
        """Append CSV data to stations table (limited to 5,000 rows)"""
        csv_path = self.find_csv_file(csv_file)
        
        if not csv_path:
            return False
        
        logger.info(f"\n🚀 PROCESSING: {csv_file} -> stations table (MAX: {self.MAX_ROWS:,} rows)")
        logger.info("=" * 60)
        
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            original_count = len(df)
            
            # Apply row limit
            if len(df) > self.MAX_ROWS:
                df = df.head(self.MAX_ROWS)
                logger.info(f"📊 ORIGINAL CSV: {original_count:,} rows -> LIMITED TO: {len(df):,} rows")
            else:
                logger.info(f"📊 CSV: {len(df):,} rows (within {self.MAX_ROWS:,} limit)")
            
            logger.info(f"📋 CSV columns: {list(df.columns)}")
            
            # Show sample data
            logger.info("📄 Sample data (first 3 rows):")
            for i, (_, row) in enumerate(df.head(3).iterrows()):
                logger.info(f"   Row {i+1}: {dict(row)}")
            
            # Map data to stations schema
            mapped_data = self.map_to_stations_schema(df)
            logger.info(f"🔧 Mapped {len(mapped_data):,} records to stations schema")
            
            # Get existing IDs to avoid duplicates
            existing_ids = self.get_existing_station_ids()
            if existing_ids:
                logger.info(f"⚠️ Found {len(existing_ids):,} existing station_ids in database")
            
            # Filter out duplicates
            new_data = []
            duplicate_count = 0
            
            for data in mapped_data:
                if data['station_id'] and data['station_id'] not in existing_ids:
                    new_data.append(data)
                else:
                    duplicate_count += 1
            
            if duplicate_count > 0:
                logger.info(f"🔄 Skipped {duplicate_count:,} duplicate station_ids")
            
            if not new_data:
                logger.info("✅ No new data to insert (all station_ids already exist)")
                return True
            
            logger.info(f"📥 Preparing to insert {len(new_data):,} new records")
            
            # Insert data with batch processing for better performance
            inserted_count = 0
            cur = self.conn.cursor()
            
            # Process in batches of 1000
            batch_size = 1000
            for i in range(0, len(new_data), batch_size):
                batch = new_data[i:i + batch_size]
                
                for record in batch:
                    try:
                        # Build the INSERT query
                        columns = [k for k, v in record.items() if v is not None]
                        placeholders = ', '.join(['%s'] * len(columns))
                        
                        query = f"INSERT INTO stations ({', '.join(columns)}) VALUES ({placeholders})"
                        values = [record[col] for col in columns]
                        
                        cur.execute(query, values)
                        inserted_count += 1
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to insert station_id {record.get('station_id')}: {e}")
                        continue
                
                # Commit after each batch
                self.conn.commit()
                logger.info(f"   ✅ Batch completed: {inserted_count:,} records inserted so far")
            
            cur.close()
            
            logger.info(f"🎯 SUCCESS: Inserted {inserted_count:,} new rows into stations table")
            
            # Show final stats
            self.show_stations_stats()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            self.conn.rollback()
            return False
    
    def append_to_stations_with_sampling(self, csv_file, sample_size=5000):
        """Alternative method: Append random sample of data to stations table"""
        csv_path = self.find_csv_file(csv_file)
        
        if not csv_path:
            return False
        
        logger.info(f"\n🎲 PROCESSING RANDOM SAMPLE: {csv_file} -> stations table")
        logger.info(f"📊 SAMPLE SIZE: {sample_size:,} random rows")
        logger.info("=" * 60)
        
        try:
            # Read CSV and take random sample
            df = pd.read_csv(csv_path)
            
            if len(df) > sample_size:
                df_sample = df.sample(n=sample_size, random_state=42)  # random_state for reproducibility
                logger.info(f"📊 ORIGINAL: {len(df):,} rows -> RANDOM SAMPLE: {len(df_sample):,} rows")
            else:
                df_sample = df
                logger.info(f"📊 Using all {len(df_sample):,} rows (within sample size limit)")
            
            # Map data to stations schema
            mapped_data = self.map_to_stations_schema(df_sample)
            
            # Get existing IDs to avoid duplicates
            existing_ids = self.get_existing_station_ids()
            
            # Filter out duplicates
            new_data = [data for data in mapped_data if data['station_id'] and data['station_id'] not in existing_ids]
            
            if not new_data:
                logger.info("✅ No new data to insert (all station_ids already exist)")
                return True
            
            logger.info(f"📥 Preparing to insert {len(new_data):,} new sampled records")
            
            # Insert data
            inserted_count = 0
            cur = self.conn.cursor()
            
            for record in new_data:
                try:
                    columns = [k for k, v in record.items() if v is not None]
                    placeholders = ', '.join(['%s'] * len(columns))
                    
                    query = f"INSERT INTO stations ({', '.join(columns)}) VALUES ({placeholders})"
                    values = [record[col] for col in columns]
                    
                    cur.execute(query, values)
                    inserted_count += 1
                    
                    if inserted_count % 1000 == 0:
                        logger.info(f"   ... inserted {inserted_count:,} records")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Failed to insert station_id {record.get('station_id')}: {e}")
                    continue
            
            self.conn.commit()
            cur.close()
            
            logger.info(f"🎯 SUCCESS: Inserted {inserted_count:,} sampled rows into stations table")
            self.show_stations_stats()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            self.conn.rollback()
            return False
    
    def show_stations_stats(self):
        """Show stations table statistics"""
        cur = self.conn.cursor()
        
        # Get total count
        cur.execute("SELECT COUNT(*) FROM stations")
        total_count = cur.fetchone()[0]
        
        # Get count of records with coordinates
        cur.execute("SELECT COUNT(*) FROM stations WHERE lat IS NOT NULL AND lon IS NOT NULL")
        with_coords = cur.fetchone()[0]
        
        # Get count by country
        cur.execute("SELECT country, COUNT(*) FROM stations WHERE country IS NOT NULL GROUP BY country ORDER BY COUNT(*) DESC LIMIT 5")
        top_countries = cur.fetchall()
        
        # Get power class distribution
        cur.execute("""
            SELECT 
                CASE 
                    WHEN power_class_kw < 50 THEN 'Slow (<50kW)'
                    WHEN power_class_kw BETWEEN 50 AND 150 THEN 'Fast (50-150kW)'
                    WHEN power_class_kw > 150 THEN 'Ultra-Fast (>150kW)'
                    ELSE 'Unknown'
                END as power_category,
                COUNT(*) as count
            FROM stations 
            WHERE power_class_kw IS NOT NULL
            GROUP BY power_category
            ORDER BY count DESC
        """)
        power_distribution = cur.fetchall()
        
        logger.info(f"📈 STATIONS TABLE STATISTICS:")
        logger.info(f"   Total records: {total_count:,}")
        logger.info(f"   Records with coordinates: {with_coords:,}")
        logger.info(f"   Top countries:")
        for country, count in top_countries:
            logger.info(f"      {country}: {count:,}")
        logger.info(f"   Power distribution:")
        for category, count in power_distribution:
            logger.info(f"      {category}: {count:,}")
        
        cur.close()
    
    def close(self):
        """Close connection"""
        if hasattr(self, 'conn'):
            self.conn.close()
            logger.info("✅ Database connection closed")

def main():
    appender = None
    try:
        appender = StationsDataAppender()
        
        # Option 1: Append first 5,000 rows
        logger.info("🔄 METHOD 1: Using first 5,000 rows")
        success = appender.append_to_stations('charging_stations.csv')
        
        # Option 2: Uncomment below to use random sampling instead
        # logger.info("🔄 METHOD 2: Using random sampling (5,000 rows)")
        # success = appender.append_to_stations_with_sampling('charging_stations.csv', sample_size=5000)
        
        if success:
            logger.info("✅ Successfully appended limited data to stations table!")
        else:
            logger.error("❌ Failed to append data to stations table")
        
    except Exception as e:
        logger.error(f"🚨 Fatal error: {e}")
    finally:
        if appender:
            appender.close()

if __name__ == "__main__":
    main()