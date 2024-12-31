from flask import Flask, render_template, request, jsonify
import mysql.connector
import pandas as pd
import os 

app = Flask(__name__)

# MySQL Configuration
db_config = {
    'host': 'localhost',
    'user': 'admin',       # Replace with your MySQL username
    'password': 'admin123',   # Replace with your MySQL password
    'database': 'airpollutiondata'    # Replace with your database name
}


def get_cities():
    """Fetch distinct cities from the database."""
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    cursor.execute("SELECT DISTINCT city FROM air_quality")
    cities = [row[0] for row in cursor.fetchall()]
    connection.close()
    return cities

def get_air_quality_by_city(city):
    """Fetch air quality data for a selected city."""
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)
    query = """
        SELECT city, min_value, max_value 
        FROM air_quality 
        WHERE city = %s 
        ORDER BY last_update DESC 
        LIMIT 1
    """
    cursor.execute(query, (city,))
    result = cursor.fetchone()
    connection.close()
    return result

@app.route('/', methods=['GET', 'POST'])
def index():
    cities = get_cities()
    air_quality = None

    if request.method == 'POST':
        selected_city = request.form.get('city')
        air_quality = get_air_quality_by_city(selected_city)

    return render_template('index.html', cities=cities, air_quality=air_quality)

if __name__ == '__main__':
    app.run(debug=True)