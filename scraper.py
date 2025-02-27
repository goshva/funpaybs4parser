import re
import requests
from bs4 import BeautifulSoup
import threading
import time
import random

class GameScraper:
    def scrape_game_data(self, game_url):
        """Scrape game details from the provided URL with retry mechanism"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = requests.get(game_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")

                counter_list = soup.find("div", class_="counter-list")
                if not counter_list:
                    return None

                game_data = {}
                for a_tag in counter_list.find_all("a", class_="counter-item"):
                    inside_div = a_tag.find("div", class_="inside")
                    param_div = inside_div.find("div", class_="counter-param")
                    value_div = inside_div.find("div", class_="counter-value")

                    if param_div and value_div:
                        category = (param_div.text.strip())
                        category = re.sub(r'[^a-zA-Z0-9_]', '', category)
                        value = int(value_div.text.strip())
                        game_data[category] = value

                return game_data

            except requests.HTTPError as http_err:
                if http_err.response.status_code == 429:
                    retry_after = int(http_err.response.headers.get("Retry-After", 2))
                    print(f"Rate limited. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                else:
                    print(f"HTTP error occurred: {http_err}")
                    return None
            except requests.RequestException as e:
                print(f"Failed to fetch {game_url}: {e}")
                return None

        print(f"Failed to fetch {game_url} after {max_retries} attempts.")
        return None

    def scrape_multiple_games(self, game_urls):
        """Scrape multiple game URLs with a delay of 2 seconds between each request"""
        def scrape_with_delay(url):
            game_data = self.scrape_game_data(url)
            print(f"Scraped data: {game_data}")
            # Add a random delay between requests to avoid rate limiting
            time.sleep(random.uniform(2, 5))

        threads = []
        for url in game_urls:
            thread = threading.Thread(target=scrape_with_delay, args=(url,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

# Example usage
if __name__ == "__main__":
    scraper = GameScraper()
    game_urls = [
        "https://funpay.com/en/lots/2866/",
        "https://funpay.com/en/lots/2867/",
        "https://funpay.com/en/lots/2868/"
    ]
    scraper.scrape_multiple_games(game_urls)
    print("Scraping completed.")