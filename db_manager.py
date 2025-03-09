import sqlite3
import datetime
import re

class DatabaseManager:
    def __init__(self, db_name='funpay.db'):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.games_table_name = f"games_{self.timestamp}"  # Table name with timestamp
        self._setup_database()

    def _setup_database(self):
        """Initialize the database structure with timestamped games table, parser_runs table, and orders table."""

        # Create a table to store parser run details
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS parser_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT UNIQUE NOT NULL
            )
        ''')

        # Create the timestamped games table
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.games_table_name} (
                game_id INTEGER PRIMARY KEY,
                game_url TEXT NOT NULL,
                game_title TEXT NOT NULL
            )
        ''')

        # Create the orders table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                user_name TEXT NOT NULL,
                description TEXT,
                price REAL,
                link TEXT,
                timestamp TEXT
            )
        ''')

        # Inserting time into parser_runs
        try:
            self.cursor.execute(f"INSERT INTO parser_runs (timestamp) VALUES (?)", (self.timestamp,))
            self.conn.commit()
        except sqlite3.IntegrityError:
            print(f"Timestamp {self.timestamp} already exists in parser_runs. Skipping insertion.")
            self.timestamp = self.get_last_timestamp()
            self.games_table_name = f"games_{self.timestamp}"

    def save_order(self, user_id, user_name, description, price, link):
        """Save order details into the orders table."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.cursor.execute('''
            INSERT INTO orders (user_id, user_name, description, price, link, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, user_name, description, price, link, timestamp))
        self.conn.commit()

    def get_all_games(self):
        """Fetch all game URLs from the current timestamped games table"""
        self.cursor.execute(f"SELECT game_id, game_url FROM {self.games_table_name}")
        return self.cursor.fetchall()

    def get_existing_columns(self):
        """Get all current column names in the current timestamped games table"""
        self.cursor.execute(f"PRAGMA table_info({self.games_table_name})")
        return {row[1] for row in self.cursor.fetchall()}

    def add_column(self, column_name):
        column_name_clear = re.sub(r'[^a-zA-Z0-9_]', '', column_name)
        self.cursor.execute(f"ALTER TABLE {self.games_table_name} ADD COLUMN {column_name_clear} INTEGER DEFAULT 0")
        self.conn.commit()

    def update_game(self, game_id, game_data):
        """Update game record in the current timestamped games table with new data"""
        columns = ", ".join(f"{re.sub(r'[^a-zA-Z0-9_]', '', col)} = ?" for col in game_data.keys())
        values = list(game_data.values()) + [game_id]
        query = f"UPDATE {self.games_table_name} SET {columns} WHERE game_id = ?"
        self.cursor.execute(query, values)
        self.conn.commit()

    def get_parser_runs(self):
        """Fetch all recorded parser runs."""
        self.cursor.execute("SELECT run_id, timestamp FROM parser_runs ORDER BY timestamp DESC")
        return self.cursor.fetchall()

    def get_games_table_name(self):
        return self.games_table_name

    def get_last_timestamp(self):
        self.cursor.execute("SELECT timestamp FROM parser_runs ORDER BY timestamp DESC LIMIT 1")
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            return None

    def close(self):
        """Close the database connection"""
        self.conn.close()

    def insert_games(self, games_data):
        for game_id, game_url, game_title, _ in games_data:
            self.cursor.execute(f'INSERT OR IGNORE INTO {self.games_table_name} (game_id, game_url, game_title) VALUES (?, ?, ?)', (game_id, game_url, game_title))
        self.conn.commit()

    def create_lots_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS lots (
                lot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                lot_name TEXT NOT NULL,
                lot_url TEXT NOT NULL,
                game_id INTEGER,
                table_name TEXT,
                FOREIGN KEY (game_id) REFERENCES games (game_id)
            )
        ''')
        self.conn.commit()

    def insert_lots(self, games_data):
        for game_id, _, _, lots in games_data:
            for lot_name, lot_url in lots:
                self.cursor.execute('INSERT INTO lots (lot_name, lot_url, game_id, table_name) VALUES (?, ?, ?, ?)', (lot_name, lot_url, game_id, self.games_table_name))
        self.conn.commit()