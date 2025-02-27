from db_manager import DatabaseManager
from scraper import GameScraper
from init import games_data

def main():
    # Initialize components
    db = DatabaseManager()
    scraper = GameScraper()
    db.insert_games(games_data) #inserting data from html parser
    db.create_lots_table()
    db.insert_lots(games_data)

    # Get all games
    games = db.get_all_games()

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

if __name__ == "__main__":
    main()