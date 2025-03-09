import time
import schedule
import requests
from bs4 import BeautifulSoup
import re
from db_manager import DatabaseManager
from scraper import GameScraper

def get_games_data():
    """
    Fetches the HTML from funpay.com/en/, parses it, and extracts
    game data (title, ID, URL, and lots).

    Returns:
        list: A list of tuples, where each tuple contains (game_id, game_url, game_title, lots).
    """
    url = "https://funpay.com/en/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')

    games_data = []

    for game_item in soup.find_all('div', class_='promo-game-item'):
        game_title_tag = game_item.find('div', class_='game-title')
        if game_title_tag:
            game_title = game_title_tag.get_text(strip=True)
            game_id = game_title_tag.get('data-id')
            game_url = game_title_tag.find('a').get('href')

            if not game_url.startswith('http'):
                game_url = "https://funpay.com" + game_url

            lots = []
            lots_list = game_item.find('ul', class_='list-inline')
            if lots_list:
                for lot_item in lots_list.find_all('li'):
                    lot_name = lot_item.get_text(strip=True)
                    lot_url = lot_item.find('a').get('href')
                    if not lot_url.startswith('http'):
                        lot_url = "https://funpay.com" + lot_url

                    lots.append((lot_name, lot_url))

            games_data.append((game_id, game_url, game_title, lots))

    return games_data


def main():
    # Initialize components
    db = DatabaseManager()
    scraper = GameScraper()

    games_data = get_games_data()

    db.insert_games(games_data)
    db.create_lots_table()
    db.insert_lots(games_data)

    # Get all games from the database
    games = db.get_all_games()
    print(f"Processing {len(games)} games...")

    # Process each game
    for game_id, game_url in games:
        print(f"Fetching data for {game_url}...")

        # Scrape game details
        game_details = scraper.scrape_game_data(game_url)

        if game_details:
            # Update database structure if needed
            existing_columns = db.get_existing_columns()
            for category in game_details.keys():
                if category not in existing_columns:
                    print(f"Adding new column: {category}")
                    db.add_column(category)

            # Update game data
            db.update_game(game_id, game_details)
            print(f"Updated game_id {game_id} with new values.")

    # Cleanup
    db.close()
    print("Database update complete.")


def run_hourly():
    print("Starting hourly run...")
    main()
    print("Hourly run completed.")


if __name__ == "__main__":
    # Run the script immediately
    run_hourly()

    # Schedule the script to run every hour
    schedule.every().hour.do(run_hourly)

    while True:
        schedule.run_pending()
        time.sleep(1)