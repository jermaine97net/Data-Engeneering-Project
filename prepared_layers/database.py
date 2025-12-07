import time
import psycopg2
from prepared_layers.utils import (
    get_logger, POSTGRES_HOST, POSTGRES_PORT, 
    POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB_ANALYTICS
)

logger = get_logger(__name__)

def get_db_connection(database: str):
    """Create and return a database connection."""
    for attempt in range(10):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=database
            )
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to {database} (attempt {attempt + 1}/10): {e}")
            time.sleep(5)
    raise Exception(f"Could not connect to {database} after 10 attempts")


def create_prepared_layer_tables():
    """Create all prepared layer tables if they don't exist."""
    conn = get_db_connection(POSTGRES_DB_ANALYTICS)
    cursor = conn.cursor()
    
    try:
        
        # CDR Data - 2: Tower sessions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.cdr_tower_sessions (
                id SERIAL PRIMARY KEY,
                msisdn VARCHAR(20) NOT NULL,
                tower_id INTEGER NOT NULL,
                session_start TIMESTAMP NOT NULL,
                session_end TIMESTAMP NOT NULL,
                interaction_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_tower_sessions_msisdn ON prepared_layers.cdr_tower_sessions(msisdn);
            CREATE INDEX IF NOT EXISTS idx_tower_sessions_tower_id ON prepared_layers.cdr_tower_sessions(tower_id);
            CREATE INDEX IF NOT EXISTS idx_tower_sessions_start ON prepared_layers.cdr_tower_sessions(session_start);
        """)
        
        
        # Forex Data - 1: OHLC summaries with technical indicators
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.forex_ohlc_m1 (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                pair_name VARCHAR(20) NOT NULL,
                open_price DECIMAL(10, 4) NOT NULL,
                high_price DECIMAL(10, 4) NOT NULL,
                low_price DECIMAL(10, 4) NOT NULL,
                close_price DECIMAL(10, 4) NOT NULL,
                ema_8 DECIMAL(10, 4),
                ema_21 DECIMAL(10, 4),
                atr_8 DECIMAL(10, 4),
                atr_21 DECIMAL(10, 4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(datetime, pair_name)
            );
            CREATE INDEX IF NOT EXISTS idx_forex_m1_datetime ON prepared_layers.forex_ohlc_m1(datetime);
            CREATE INDEX IF NOT EXISTS idx_forex_m1_pair ON prepared_layers.forex_ohlc_m1(pair_name);
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.forex_ohlc_m30 (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                pair_name VARCHAR(20) NOT NULL,
                open_price DECIMAL(10, 4) NOT NULL,
                high_price DECIMAL(10, 4) NOT NULL,
                low_price DECIMAL(10, 4) NOT NULL,
                close_price DECIMAL(10, 4) NOT NULL,
                ema_8 DECIMAL(10, 4),
                ema_21 DECIMAL(10, 4),
                atr_8 DECIMAL(10, 4),
                atr_21 DECIMAL(10, 4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(datetime, pair_name)
            );
            CREATE INDEX IF NOT EXISTS idx_forex_m30_datetime ON prepared_layers.forex_ohlc_m30(datetime);
            CREATE INDEX IF NOT EXISTS idx_forex_m30_pair ON prepared_layers.forex_ohlc_m30(pair_name);
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.forex_ohlc_h1 (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                pair_name VARCHAR(20) NOT NULL,
                open_price DECIMAL(10, 4) NOT NULL,
                high_price DECIMAL(10, 4) NOT NULL,
                low_price DECIMAL(10, 4) NOT NULL,
                close_price DECIMAL(10, 4) NOT NULL,
                ema_8 DECIMAL(10, 4),
                ema_21 DECIMAL(10, 4),
                atr_8 DECIMAL(10, 4),
                atr_21 DECIMAL(10, 4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(datetime, pair_name)
            );
            CREATE INDEX IF NOT EXISTS idx_forex_h1_datetime ON prepared_layers.forex_ohlc_h1(datetime);
            CREATE INDEX IF NOT EXISTS idx_forex_h1_pair ON prepared_layers.forex_ohlc_h1(pair_name);
        """)
        
        # Processing state tracking table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.processing_state (
                id SERIAL PRIMARY KEY,
                layer_name VARCHAR(100) UNIQUE NOT NULL,
                last_processed_datetime TIMESTAMP,
                last_run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        conn.commit()
        logger.info("Successfully created all prepared layer tables")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating tables: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
