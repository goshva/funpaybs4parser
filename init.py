import sqlite3
from bs4 import BeautifulSoup
from db_manager import DatabaseManager

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

