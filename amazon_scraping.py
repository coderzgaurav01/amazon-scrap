from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from db import create_table, insert_data

# Function to extract Product Title
def get_title(soup):
    try:
        title = soup.find("span", attrs={"id": 'productTitle'})
        return title.text.strip()
    except AttributeError:
        return ""

# Function to extract Product Price
def get_price(soup):
    try:
        return soup.find("span", attrs={'id': 'priceblock_ourprice'}).string.strip()
    except AttributeError:
        try:
            return soup.find("span", attrs={'id': 'priceblock_dealprice'}).string.strip()
        except:
            return ""

# Function to extract Product Rating
def get_rating(soup):
    try:
        return soup.find("i", attrs={'class': 'a-icon a-icon-star a-star-4-5'}).string.strip()
    except AttributeError:
        try:
            return soup.find("span", attrs={'class': 'a-icon-alt'}).string.strip()
        except:
            return ""

# Function to extract Number of User Reviews
def get_review_count(soup):
    try:
        return soup.find("span", attrs={'id': 'acrCustomerReviewText'}).string.strip()
    except AttributeError:
        return ""

# Function to extract Availability Status
def get_availability(soup):
    try:
        available = soup.find("div", attrs={'id': 'availability'})
        return available.find("span").string.strip()
    except AttributeError:
        return "Not Available"

# Main execution
if __name__ == '__main__':
    HEADERS = ({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept-Language': 'en-US, en;q=0.5'
    })

    URL = "https://www.amazon.com/s?k=playstation+4&ref=nb_sb_noss_2"
    webpage = requests.get(URL, headers=HEADERS)
    print(webpage.status_code)
    soup = BeautifulSoup(webpage.content, "html.parser")

    links = soup.find_all("a", attrs={'class': 'a-link-normal s-no-outline'})
    links_list = [link.get('href') for link in links]

    data = {"title": [], "price": [], "rating": [], "reviews": [], "availability": []}

    for link in links_list:
        product_url = "https://www.amazon.com" + link
        new_webpage = requests.get(product_url, headers=HEADERS)
        new_soup = BeautifulSoup(new_webpage.content, "html.parser")

        data['title'].append(get_title(new_soup))
        data['price'].append(get_price(new_soup))
        data['rating'].append(get_rating(new_soup))
        data['reviews'].append(get_review_count(new_soup))
        data['availability'].append(get_availability(new_soup))

    amazon_df = pd.DataFrame.from_dict(data)
    amazon_df['title'].replace('', np.nan, inplace=True)
    amazon_df.dropna(subset=['title'], inplace=True)
    amazon_df.to_csv("amazon_data.csv", index=False)

    # Store into PostgreSQL
    create_table()
    insert_data(amazon_df)
