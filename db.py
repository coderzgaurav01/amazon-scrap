import psycopg2
from psycopg2 import sql
import pandas as pd

# Connect to the PostgreSQL database
def get_connection():
    try:
        conn = psycopg2.connect(
            dbname="amazon",
            user="postgres",
            password="1234ajay@",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        print("Database connection error:", e)
        return None

# Create the products table
def create_table():
    conn = get_connection()
    if conn:
        try:
            cur = conn.cursor()
            create_query = '''
            CREATE TABLE IF NOT EXISTS amazon_products (
                id SERIAL PRIMARY KEY,
                title TEXT,
                price TEXT,
                rating TEXT,
                reviews TEXT,
                availability TEXT
            );
            '''
            cur.execute(create_query)
            conn.commit()
            cur.close()
        except Exception as e:
            print("Table creation failed:", e)
        finally:
            conn.close()

# Insert scraped data into the database
def insert_data(df):
    conn = get_connection()
    if conn:
        try:
            cur = conn.cursor()
            for _, row in df.iterrows():
                insert_query = '''
                INSERT INTO amazon_products (title, price, rating, reviews, availability)
                VALUES (%s, %s, %s, %s, %s);
                '''
                cur.execute(insert_query, (
                    row['title'],
                    row['price'],
                    row['rating'],
                    row['reviews'],
                    row['availability']
                ))
            conn.commit()
            cur.close()
        except Exception as e:
            print("Data insertion error:", e)
        finally:
            conn.close()
