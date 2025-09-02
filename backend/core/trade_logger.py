import asyncio
from datetime import datetime
from .database import today_engine, all_engine, sql_text

class TradeLogger:
    """Handles all database interactions for logging trades using a connection pool."""
    def __init__(self, db_lock):
        self.db_lock = db_lock
        self.engines = [today_engine, all_engine]

    async def log_trade(self, trade_info):
        """Asynchronously logs a completed trade to the databases using the pool."""
        def db_call():
            columns = ", ".join(trade_info.keys())
            placeholders = ", ".join(f":{key}" for key in trade_info.keys())
            sql = f"INSERT INTO trades ({columns}) VALUES ({placeholders})"
            
            for engine in self.engines:
                try:
                    with engine.begin() as conn:
                        conn.execute(sql_text(sql), trade_info)
                except Exception as e:
                    db_name = engine.url.database
                    print(f"CRITICAL DB ERROR writing to {db_name}: {e}")

        async with self.db_lock:
            await asyncio.to_thread(db_call)

    @staticmethod
    def setup_databases():
        """
        Creates/updates tables if they don't exist and clears the 'today'
        database if it's a new day.
        """
        # --- MODIFIED: Added charges and net_pnl columns to the schema ---
        create_table_sql = sql_text('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                trigger_reason TEXT NOT NULL,
                symbol TEXT,
                quantity INTEGER,
                pnl REAL,
                entry_price REAL,
                exit_price REAL,
                exit_reason TEXT,
                trend_state TEXT,
                atr REAL,
                charges REAL,
                net_pnl REAL
            )
        ''')
        
        # --- NEW: Logic to add new columns if they don't exist (for backward compatibility) ---
        def upgrade_schema(engine):
            with engine.connect() as conn:
                conn.execute(create_table_sql) # First, ensure table exists
                # Check for and add new columns individually
                cursor = conn.execute(sql_text("PRAGMA table_info(trades);"))
                columns = [row[1] for row in cursor]
                if 'charges' not in columns:
                    conn.execute(sql_text("ALTER TABLE trades ADD COLUMN charges REAL;"))
                if 'net_pnl' not in columns:
                    conn.execute(sql_text("ALTER TABLE trades ADD COLUMN net_pnl REAL;"))
                # Use .commit() because ALTER TABLE cannot run in a transaction block on some dbs
                if hasattr(conn, 'commit'): conn.commit() 

        # Upgrade schema for both databases
        upgrade_schema(today_engine)
        upgrade_schema(all_engine)
        
        try:
            with open("last_run_date.txt", "r") as f: last_run_date = f.read()
        except FileNotFoundError: last_run_date = ""

        today_date = datetime.now().strftime("%Y-%m-%d")
        if last_run_date != today_date:
            print(f"New day detected. Clearing today's trade log...")
            with today_engine.begin() as conn:
                conn.execute(sql_text("DELETE FROM trades"))
            with open("last_run_date.txt", "w") as f: f.write(today_date)
            print("Today's trade log cleared.")

        print("Databases setup complete.")