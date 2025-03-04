import requests
from bs4 import BeautifulSoup
import random
import time
from tqdm import tqdm
import sqlite3
import datetime
import logging
import re

# Import the provided DatabaseManager class
from db_manager import DatabaseManager

# Constants
MAX_USER_ID = 14112521
BASE_URL = "https://funpay.com/en/users/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
ONE_WEEK = datetime.timedelta(days=7)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_database(db):
    """Set up the users and offers tables with DATETIME columns."""
    db.cursor.execute('''
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
    
    db.cursor.execute('''
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
    db.conn.commit()

def parse_date_to_datetime(date_str):
    """Convert a date string like '16 September 2023, 8:59 1 year ago' to SQLite DATETIME format."""
    try:
        # Use regex to extract date and time, allowing single-digit hours
        match = re.match(r'(\d{1,2} \w+ \d{4}, \d{1,2}:\d{2})', date_str)
        if match:
            date_part = match.group(1)
            # Parse with flexible hour format (1 or 2 digits)
            dt = datetime.datetime.strptime(date_part, "%d %B %Y, %H:%M")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            logging.warning(f"Date string '{date_str}' does not match expected pattern")
            return None
    except (ValueError, TypeError) as e:
        logging.warning(f"Failed to parse date '{date_str}': {e}")
        return None

def is_valid_float(value):
    """Check if a string can be converted to a float."""
    try:
        float(value)
        return True
    except ValueError:
        return False

def parse_user_page(user_id, db):
    """Parse a FunPay user profile and save to database."""
    url = f"{BASE_URL}{user_id}/"
    current_time = datetime.datetime.now()
    current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Check if user was updated recently
    db.cursor.execute("SELECT updated_at FROM users WHERE user_id = ?", (user_id,))
    result = db.cursor.fetchone()
    if result:
        last_updated = datetime.datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S")
        if (current_time - last_updated) < ONE_WEEK:
            logging.info(f"User {user_id}: Skipped (updated within last week)")
            return False
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            logging.error(f"User {user_id}: Failed with status code {response.status_code}")
            return False
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Check if user exists
        if "User not found" in response.text or soup.find("h1") is None:
            logging.info(f"User {user_id}: Not found")
            return False
        
        # Extract username
        h1_tag = soup.find("h1", class_="mb40")
        username = h1_tag.find("span", class_="mr4").get_text(strip=True) if h1_tag and h1_tag.find("span", class_="mr4") else None
        if not username:
            username_tag = soup.find("h1")
            username = username_tag.get_text(strip=True).split()[0] if username_tag else f"User_{user_id}"
            logging.warning(f"User {user_id}: Username fallback used - {username}")
        
        # Extract status
        status = soup.find("span", class_="media-user-status")
        status_timestamp = current_time_str if status and "Online" in status.get_text(strip=True) else None
        if not status:
            logging.warning(f"User {user_id}: Status not found")
        
        # Extract registration date
        reg_date = soup.find("div", class_="param-item")
        reg_date_str = reg_date.find("div", class_="text-nowrap").get_text(strip=True) if reg_date and reg_date.find("div", class_="text-nowrap") else None
        registration_timestamp = parse_date_to_datetime(reg_date_str) if reg_date_str else None
        if not reg_date_str:
            logging.warning(f"User {user_id}: Registration date not found")
        
        # Extract seller rating
        rating = soup.find("div", class_="rating-value")
        seller_rating_str = rating.find("span", class_="big").get_text(strip=True) if rating and rating.find("span", class_="big") else None
        seller_rating = float(seller_rating_str) if seller_rating_str and is_valid_float(seller_rating_str) else None
        if not seller_rating_str:
            rating_div = soup.find("div", class_="rating-value")
            logging.warning(f"User {user_id}: Seller rating not found. Rating div: {rating_div}")
        
        # Extract total reviews
        reviews = soup.find("div", class_="rating-full-count")
        total_reviews_str = reviews.get_text(strip=True).split()[0] if reviews else None
        total_reviews = int(total_reviews_str) if total_reviews_str and total_reviews_str.isdigit() else None
        if not total_reviews_str:
            reviews_div = soup.find("div", class_="rating-full-count")
            logging.warning(f"User {user_id}: Total reviews not found. Reviews div: {reviews_div}")
        
        # Insert or update user in database
        db.cursor.execute('''
            INSERT INTO users (user_id, username, status_timestamp, registration_timestamp, seller_rating, total_reviews, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                status_timestamp = excluded.status_timestamp,
                registration_timestamp = excluded.registration_timestamp,
                seller_rating = excluded.seller_rating,
                total_reviews = excluded.total_reviews,
                updated_at = excluded.updated_at
        ''', (user_id, username, status_timestamp, registration_timestamp, seller_rating, total_reviews, current_time_str, current_time_str))
        
        # Delete existing offers for this user
        db.cursor.execute("DELETE FROM offers WHERE user_id = ?", (user_id,))
        
        # Extract and insert offers
        offer_sections = soup.find_all("div", class_="offer")
        for section in offer_sections:
            category_tag = section.find("div", class_="offer-list-title")
            category = category_tag.find("h3").get_text(strip=True) if category_tag and category_tag.find("h3") else "Unknown category"
            
            offer_items = section.find_all("a", class_="tc-item")
            for item in offer_items:
                desc = item.find("div", class_="tc-desc-text")
                description = desc.get_text(strip=True) if desc else "No description"
                
                price_div = item.find("div", class_="tc-price")
                price = price_div.find("div").get_text(strip=True) if price_div and price_div.find("div") else "No price"
                
                server = item.find("div", class_="tc-server")
                server_or_platform = server.get_text(strip=True) if server else None
                
                amount = item.find("div", class_="tc-amount")
                in_stock = amount.get_text(strip=True) if amount else None
                
                link = item["href"] if "href" in item.attrs else None
                
                db.cursor.execute('''
                    INSERT INTO offers (user_id, category, description, price, server_or_platform, in_stock, link)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, category, description, price, server_or_platform, in_stock, link))
        
        db.conn.commit()
        logging.info(f"User {user_id}: Successfully parsed - {username}")
        return True
    
    except requests.RequestException as e:
        logging.error(f"User {user_id}: Request failed - {e}")
        return False

def main():
    # Initialize database
    db = DatabaseManager("funpay.db")
    setup_database(db)
    
    total_users = MAX_USER_ID
    print(f"Starting to parse {total_users} users randomly without repeats...")
    
    # Load visited IDs from database
    db.cursor.execute("SELECT user_id FROM users")
    visited_ids = {row[0] for row in db.cursor.fetchall()}
    
    # Use tqdm for progress tracking
    with tqdm(total=total_users, desc="Parsing Users") as pbar:
        pbar.update(len(visited_ids))  # Set initial progress
        
        while len(visited_ids) < total_users:
            user_id = random.randint(1, MAX_USER_ID)
            
            if user_id in visited_ids:
                if not parse_user_page(user_id, db):
                    continue
            else:
                if parse_user_page(user_id, db):
                    visited_ids.add(user_id)
                    pbar.update(1)
            
            # Random delay to avoid rate limiting (1-5 seconds)
            time.sleep(random.uniform(1, 5))
    
    print(f"Completed parsing all {total_users} users.")
    db.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user. Progress saved in funpay.db")