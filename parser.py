import sqlite3
import requests
from bs4 import BeautifulSoup

# Connect to SQLite database
conn = sqlite3.connect('games.db')
cursor = conn.cursor()

# Ensure the table has the basic structure
cursor.execute('''
CREATE TABLE IF NOT EXISTS games (
    game_id INTEGER PRIMARY KEY,
    game_url TEXT NOT NULL,
    game_title TEXT NOT NULL
)
''')

# Fetch all game URLs from the database
cursor.execute("SELECT game_id, game_url FROM games")
games = cursor.fetchall()

# Function to get current table columns
def get_existing_columns():
    cursor.execute("PRAGMA table_info(games)")
    return {row[1] for row in cursor.fetchall()}

# Function to scrape game details from game_url
def scrape_game_data(game_url):
    try:
        # Make an HTTP request to the game_url
        response = requests.get(game_url)
        response.raise_for_status()  # Raise error for bad response
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the counter list container
        counter_list = soup.find("div", class_="counter-list counter-list-circles")

        # Extract category values
        game_data = {}
        if counter_list:
            for a_tag in counter_list.find_all("a", class_="counter-item"):
                param_div = a_tag.find("div", class_="counter-param")
                value_div = a_tag.find("div", class_="counter-value")

                if param_div and value_div:
                    category = param_div.text.strip().replace(" ", "_")  # Convert to column-safe format
                    value = int(value_div.text.strip())

                    game_data[category] = value

        return game_data

    except requests.RequestException as e:
        print(f"Failed to fetch {game_url}: {e}")
        return None

# Process each game entry
for game_id, game_url in games:
    print(f"Fetching data for {game_url}...")

    # Scrape game details
    game_details = scrape_game_data(game_url)

    if game_details:
        # Get existing columns in the games table
        existing_columns = get_existing_columns()

        category = category.replace('-','_')
        for category in game_details.keys():
            
            if category not in existing_columns:
                print(f"Adding new column: {category}")
                cursor.execute(f"ALTER TABLE games ADD COLUMN {category} INTEGER DEFAULT 0")
                existing_columns.add(category)  # Update cached columns list

        # Build the UPDATE query dynamically
        update_columns = ", ".join(f"{col} = ?" for col in game_details.keys())
        update_values = list(game_details.values()) + [game_id]

        query = f"UPDATE games SET {update_columns} WHERE game_id = ?"
        cursor.execute(query, update_values)

        # Commit the update immediately
        conn.commit()

        print(f"Updated game_id {game_id} with new values.")

# Close the database connection
conn.close()
print("Database update complete.")
