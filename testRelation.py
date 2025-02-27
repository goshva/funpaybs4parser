import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('funpay.db')
cursor = conn.cursor()

# Query the games and their related lots
cursor.execute('''
SELECT games.game_title, lots.lot_name, lots.lot_url
FROM games
JOIN lots ON games.game_id = lots.game_id
''')

# Fetch all results
results = cursor.fetchall()

# Close the connection
conn.close()

# Generate HTML content
html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Games and Lots</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #333; }
        .game { margin-bottom: 20px; }
        .game h2 { color: #555; }
        .lots { list-style-type: none; padding: 0; }
        .lots li { margin-bottom: 5px; }
        .lots li a { color: #007BFF; text-decoration: none; }
        .lots li a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <h1>Games and Lots</h1>
"""

# Organize data by game
games_lots = {}
for game_title, lot_name, lot_url in results:
    if game_title not in games_lots:
        games_lots[game_title] = []
    games_lots[game_title].append((lot_name, lot_url))

# Add games and lots to HTML content
for game_title, lots in games_lots.items():
    html_content += f'<div class="game"><h2>{game_title}</h2>'
    html_content += '<ul class="lots">'
    for lot_name, lot_url in lots:
        html_content += f'<li><a href="{lot_url}" target="_blank">{lot_name}</a></li>'
    html_content += '</ul></div>'

html_content += """
</body>
</html>
"""

# Save the HTML content to a file
with open('games_lots_app.html', 'w', encoding='utf-8') as file:
    file.write(html_content)

'games_lots_app.html'
