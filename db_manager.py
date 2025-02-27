# db_manager.py
import sqlite3
import re
class DatabaseManager:
    def __init__(self, db_name='games.db'):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self._setup_database()

    def _setup_database(self):
        """Initialize the basic database structure"""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS games (
                game_id INTEGER PRIMARY KEY,
                game_url TEXT NOT NULL,
                game_title TEXT NOT NULL
            )
        ''')
        self.conn.commit()

    def get_all_games(self):
        """Fetch all game URLs from the database"""
        self.cursor.execute("SELECT game_id, game_url FROM games")
        return self.cursor.fetchall()

    def get_existing_columns(self):
        """Get all current column names in the games table"""
        self.cursor.execute("PRAGMA table_info(games)")
        return {row[1] for row in self.cursor.fetchall()}

    def add_column(self, column_name):

        self.cursor.execute(f"ALTER TABLE games ADD COLUMN {column_name} INTEGER DEFAULT 0")
        self.conn.commit()

    def update_game(self, game_id, game_data):
        """Update game record with new data"""
        columns = ", ".join(f"{col} = ?" for col in game_data.keys())
        values = list(game_data.values()) + [game_id]
        query = f"UPDATE games SET {columns} WHERE game_id = ?"
        self.cursor.execute(query, values)
        self.conn.commit()

    def close(self):
        """Close the database connection"""
        self.conn.close()


