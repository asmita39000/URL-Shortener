import hashlib
import sqlite3
import datetime
from flask import Flask, request, jsonify, redirect

# Initialize Flask app
app = Flask(__name__)

# Database setup
DB_NAME = "url_shortener.db"
def initialize_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS urls (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        original_url TEXT NOT NULL,
                        short_url TEXT NOT NULL UNIQUE,
                        creation_time TIMESTAMP NOT NULL,
                        expiration_time TIMESTAMP NOT NULL
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS analytics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        short_url TEXT NOT NULL,
                        access_time TIMESTAMP NOT NULL,
                        ip_address TEXT NOT NULL
                    )''')
    conn.commit()
    conn.close()

initialize_db()

# Helper functions
def generate_short_url(original_url):
    return hashlib.md5(original_url.encode()).hexdigest()[:6]

def get_current_time():
    return datetime.datetime.now()

def add_hours_to_time(time, hours):
    return time + datetime.timedelta(hours=hours)

# API endpoints
@app.route('/shorten', methods=['POST'])
def shorten_url():
    data = request.json
    original_url = data.get('original_url')
    expiry_hours = data.get('expiry_hours', 24)

    if not original_url:
        return jsonify({"error": "Original URL is required."}), 400

    try:
        short_url = generate_short_url(original_url)
        creation_time = get_current_time()
        expiration_time = add_hours_to_time(creation_time, expiry_hours)

        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''INSERT OR IGNORE INTO urls (original_url, short_url, creation_time, expiration_time)
                          VALUES (?, ?, ?, ?)''', (original_url, short_url, creation_time, expiration_time))
        conn.commit()
        conn.close()

        return jsonify({"original_url": original_url, "short_url": f"https://short.ly/{short_url}"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/<short_url>', methods=['GET'])
def redirect_url(short_url):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('SELECT original_url, expiration_time FROM urls WHERE short_url = ?', (short_url,))
    result = cursor.fetchone()

    if not result:
        return jsonify({"error": "URL not found."}), 404

    original_url, expiration_time = result
    if get_current_time() > datetime.datetime.fromisoformat(expiration_time):
        return jsonify({"error": "URL has expired."}), 410

    # Log analytics
    cursor.execute('INSERT INTO analytics (short_url, access_time, ip_address) VALUES (?, ?, ?)',
                   (short_url, get_current_time(), request.remote_addr))
    conn.commit()
    conn.close()

    return redirect(original_url)

@app.route('/analytics/<short_url>', methods=['GET'])
def get_analytics(short_url):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM analytics WHERE short_url = ?', (short_url,))
    logs = cursor.fetchall()

    cursor.execute('SELECT original_url FROM urls WHERE short_url = ?', (short_url,))
    url_result = cursor.fetchone()
    
    conn.close()

    if not url_result:
        return jsonify({"error": "Short URL not found."}), 404

    analytics_data = [{"access_time": log[2], "ip_address": log[3]} for log in logs]
    return jsonify({"original_url": url_result[0], "analytics": analytics_data}), 200

if __name__ == '__main__':
    app.run(debug=True)
