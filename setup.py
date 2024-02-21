import os
import psycopg2
from bs4 import BeautifulSoup
import requests
import pandas as pd

# Set user-agent headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

# URL base to scrape
base_url = "https://www.forebet.com/en/football-predictions/under-over-25-goals/"

# Define start and end dates for the date range
start_date = "2024-02-01"
end_date = "2024-02-19"

# List to store the data
data = []

# Iterate over the date range
current_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)
while current_date <= end_date:
    # Construct the URL for the current date
    url = base_url + current_date.strftime("%Y-%m-%d")

    # Send a GET request to the URL
    response = requests.get(url, headers=headers)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all rows with class "rcnt"
        rows = soup.find_all('div', class_='rcnt')

        # Loop through each row
        for row in rows:
            # Extract league_country value
            league_country_element = row.find('div', class_='shortagDiv')
            league_country = league_country_element.find('span', class_='shortTag').text.strip() if league_country_element else 'N/A'

            # Extract home team value
            home_team_element = row.find('span', class_='homeTeam')
            home_team = home_team_element.text.strip() if home_team_element else 'N/A'

            # Extract away team value
            away_team_element = row.find('span', class_='awayTeam')
            away_team = away_team_element.text.strip() if away_team_element else 'N/A'

            # Extract date value
            date_element = row.find('time')
            date = date_element.text.strip() if date_element else 'N/A'

            # Extract avg_goals value
            avg_goals_element = row.find('div', class_='avg_sc')
            avg_goals = avg_goals_element.text.strip() if avg_goals_element else 'N/A'

            # Extract coef_value value
            coef_value_element = row.find('div', class_='bigOnly prmod')
            coef_value_span = coef_value_element.find('span', class_='lscrsp') if coef_value_element else None
            coef_value = coef_value_span.text.strip() if coef_value_span else 'N/A'

            # Extract score value
            score_element = row.find('div', class_='ex_sc tabonly')
            score = score_element.text.strip() if score_element else 'N/A'

            # Extract ht_score value
            ht_score_element = row.find('div', class_='lscr_td')
            ht_score_span = ht_score_element.find('span', class_='lscrsp') if ht_score_element else None
            ht_score = ht_score_span.text.strip() if ht_score_span else 'N/A'

            # Append the extracted data to the list
            data.append([league_country, home_team, away_team, date, avg_goals, coef_value, score, ht_score])

    # Increment the current date by one day
    current_date += pd.Timedelta(days=1)

# Create a DataFrame from the collected data
df = pd.DataFrame(data, columns=['League/Country', 'Home Team', 'Away Team', 'Date', 'Average Goals', 'Coefficient Value', 'Score', 'HT Score'])

# Export the DataFrame to an Excel file
df.to_excel('football_predictions.xlsx', index=False)

# Insert data into PostgreSQL database
try:
    # Connect to the database using environment variables
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

    # Create a cursor object
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS football_predictions (
            league_country TEXT,
            home_team TEXT,
            away_team TEXT,
            date TEXT,
            average_goals TEXT,
            coefficient_value TEXT,
            score TEXT,
            ht_score TEXT
        )
    ''')

    # Insert data into the table
    for index, row in df.iterrows():
        cursor.execute('''
            INSERT INTO football_predictions (league_country, home_team, away_team, date, average_goals, coefficient_value, score, ht_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''', (row['League/Country'], row['Home Team'], row['Away Team'], row['Date'], row['Average Goals'], row['Coefficient Value'], row['Score'], row['HT Score']))

    # Commit the transaction
    conn.commit()
    print("Data has been successfully inserted into the PostgreSQL database.")

except psycopg2.Error as e:
    print("Error while connecting to PostgreSQL:", e)

finally:
    # Close the cursor and connection
    if 'conn' in locals():
        cursor.close()
        conn.close()
