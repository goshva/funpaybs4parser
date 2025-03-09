import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('funpay.db')
cursor = conn.cursor()

# Create the users table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT NOT NULL,
        status_timestamp DATETIME,
        registration_timestamp DATETIME,
        seller_rating REAL,
        total_reviews INTEGER,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL
    )
''')

# Create the offers table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS offers (
        offer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        category TEXT,
        description TEXT,
        price TEXT,
        server_or_platform TEXT,
        in_stock TEXT,
        link TEXT,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
''')

# URL of the page to scrape
url = "https://funpay.com/en/lots/81/"

# Send a GET request to the URL
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all the order elements
    orders = soup.find_all('a', class_='tc-item')

    # Loop through each order and extract the required information
    for order in orders:
        # Extract the user ID and user name
        user_info = order.find('div', class_='media-user-name')
        user_id = int(order.find('div', class_='avatar-photo')['data-href'].split('/')[-2])
        user_name = user_info.text.strip() if user_info else 'N/A'

        # Extract the description
        description = order.find('div', class_='tc-desc-text').text.strip()

        # Extract the price
        price = order.find('div', class_='tc-price').text.strip()

        # Extract the link
        link = order['href']

        # Insert user data into the users table
        cursor.execute('''
            INSERT OR IGNORE INTO users (user_id, username, created_at, updated_at)
            VALUES (?, ?, ?, ?)
        ''', (user_id, user_name, datetime.now(), datetime.now()))

        # Insert offer data into the offers table
        cursor.execute('''
            INSERT INTO offers (user_id, description, price, link)
            VALUES (?, ?, ?, ?)
        ''', (user_id, description, price, link))

    # Commit the changes and close the connection
    conn.commit()
else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")

# Close the database connection
conn.close()
