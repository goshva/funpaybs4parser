import sqlite3
from bs4 import BeautifulSoup

# Read the HTML content from the file
with open('index.html', 'r', encoding='utf-8') as file:
    html_content = file.read()

# Parse the HTML content
soup = BeautifulSoup(html_content, 'html.parser')

# Extract game information and related lots
games_data = []

for game_item in soup.find_all('div', class_='promo-game-item'):
    game_title_tag = game_item.find('div', class_='game-title')
    game_title = game_title_tag.get_text(strip=True)
    game_id = game_title_tag.get('data-id')
    game_url = game_title_tag.find('a').get('href')
    lots = []
    lots_list = game_item.find('ul', class_='list-inline')
    if lots_list:
        for lot_item in lots_list.find_all('li'):
            lot_name = lot_item.get_text(strip=True)
            lot_url = lot_item.find('a').get('href')
            lots.append((lot_name, lot_url))

    games_data.append((game_id, game_url, game_title, lots))

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('games.db')
cursor = conn.cursor()

# Create the games table
cursor.execute('''
CREATE TABLE IF NOT EXISTS games (
    game_id INTEGER PRIMARY KEY,
    game_url TEXT NOT NULL,
    game_title TEXT NOT NULL,
    Accounts INTEGER,
    Keys INTEGER,
    Items INTEGER,
    Services INTEGER,
    Pass INTEGER
)
''')

# Create the lots table
cursor.execute('''
CREATE TABLE IF NOT EXISTS lots (
    lot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    lot_name TEXT NOT NULL,
    lot_url TEXT NOT NULL,
    game_id INTEGER,
    FOREIGN KEY (game_id) REFERENCES games (game_id)
)
''')

# Insert data into the games table
for game_id, game_url, game_title, _ in games_data:
    cursor.execute('INSERT OR IGNORE INTO games (game_id, game_url, game_title) VALUES (?, ?, ?)', (game_id, game_url, game_title))

# Insert data into the lots table
for game_id, _, _, lots in games_data:
    for lot_name, lot_url in lots:
        cursor.execute('INSERT INTO lots (lot_name, lot_url, game_id) VALUES (?, ?, ?)', (lot_name, lot_url, game_id))

# Commit the changes and close the connection
conn.commit()
conn.close()
