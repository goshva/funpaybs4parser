import sqlite3
from bs4 import BeautifulSoup

# Load the HTML content
with open('index.html', 'r', encoding='utf-8') as file:
    html_content = file.read()

# Parse the HTML content
soup = BeautifulSoup(html_content, 'html.parser')

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('games.db')
cursor = conn.cursor()

# Create the games table
cursor.execute('''
CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    link TEXT
)
''')

# Create the lots table with a foreign key relation to games
cursor.execute('''
CREATE TABLE IF NOT EXISTS lots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER,
    item TEXT NOT NULL,
    FOREIGN KEY (game_id) REFERENCES games (id)
)
''')

# Parse the HTML and insert data into the database
game_data = []

# Iterate over each list item that contains a game title
for li in soup.find_all('li'):
    # Extract the game title
    game_title = li.text.strip()

    # Find the associated link, if available
    link = li.find('a')
    game_link = link['href'] if link else None

    # Find the nearest list-inline element
    list_inline = li.find_next_sibling('ul', class_='list-inline')
    list_inline_items = [item.text.strip() for item in list_inline.find_all('li')] if list_inline else []

    # Insert the game data into the games table
    cursor.execute('INSERT INTO games (title, link) VALUES (?, ?)', (game_title, game_link))
    game_id = cursor.lastrowid

    # Insert the list-inline items into the lots table
    for item in list_inline_items:
        cursor.execute('INSERT INTO lots (game_id, item) VALUES (?, ?)', (game_id, item))

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Data has been successfully inserted into the database.")