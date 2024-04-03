import streamlit as st
import requests
import json
from lxml import html
import pandas as pd
import time
import re
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
import logging
import sqlite3
from datetime import datetime, timedelta
import threading
import random

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up cache database
def get_db_connection():
    conn = sqlite3.connect('cache.db', check_same_thread=False)
    conn.execute('''CREATE TABLE IF NOT EXISTS cache
                 (url TEXT PRIMARY KEY, data TEXT, timestamp TIMESTAMP)''')
    return conn

# Streamlit app title
st.title("Amazon Product Data Scraper - UK")

# Sidebar inputs
st.sidebar.header("Input Options")
api_key = st.sidebar.text_input("Enter your API Key:", value="", max_chars=None, key=None, type='default')
uploaded_file = st.sidebar.file_uploader("Upload a CSV file with URLs:", type=['csv'])

if not uploaded_file:
    urls_text = st.sidebar.text_area("Or paste URLs here (one per line):")
    urls = [url.strip() for url in urls_text.split('\n') if url.strip()]
else:
    urls = pd.read_csv(uploaded_file).squeeze().tolist()

# Set the number of concurrent scrapes
MAX_CONCURRENT_SCRAPES = 10
concurrent_scrapes = st.sidebar.number_input("Number of Concurrent Scrapes", min_value=1, max_value=MAX_CONCURRENT_SCRAPES, value=1, step=1)

# Add checkbox for initial delay
use_initial_delay = st.sidebar.checkbox("Use Initial Delay", value=True)

# Function to scrape data
def scrape_data(url, api_key, max_retries=5, initial_delay=2):
    # Get a new connection and cursor for this thread
    conn = get_db_connection()
    c = conn.cursor()

    # Check cache
    c.execute("SELECT data FROM cache WHERE url=? AND timestamp >= datetime('now', '-1 day')", (url,))
    cached_data = c.fetchone()
    if cached_data:
        data_dict = json.loads(cached_data[0])
        if "Not Found" not in data_dict.values():
            logging.info(f"Using cached data for {url}")
            conn.close()
            return data_dict

    # Validate URL format
    try:
        parsed_url = urlparse(url)
        if not all([parsed_url.scheme, parsed_url.netloc]):
            conn.close()
            return {"Error": "Invalid URL format."}
    except ValueError:
        conn.close()
        return {"Error": "Invalid URL format."}

    data_dict = {"Product URL": url, "Product Title": "Not found", "Brand Store": "Not found", "Brand Store URL": "Not found",
                 "Item model number": "Not found", "Manufacturer": "Not found"}

    retries = 0
    delay = initial_delay

    while retries < max_retries:
        scrapeowl_url = "https://api.scrapeowl.com/v1/scrape"
        object_of_data = {
            "api_key": api_key,
            "url": url,
            "premium_proxies": True,
            "country": "gb",
            "elements": [
                {
                    "type": "xpath",
                    "selector": "//span[@id='productTitle']"
                },
                {
                    "type": "xpath",
                    "selector": "//a[@id='bylineInfo']",
                },
                {
                    "type": "xpath",
                    "selector": "//div[@id='detailBullets_feature_div']"
                },
                {
                    "type": "xpath",
                    "selector": "//table[@id='productDetails_techSpec_section_1']"
                }
            ],
            "json_response": True
        }
        data = json.dumps(object_of_data)
        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(scrapeowl_url, data, headers=headers)
            response_content = response.content.decode('unicode_escape')  # Decode the content and remove backslashes
            tree = html.fromstring(response_content)
            byline_info = tree.xpath('//a[@id="bylineInfo"]')
            if byline_info:
                data_dict["Brand Store URL"] = byline_info[0].get('href')
            response_json = response.json()
            print(response_json)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {url}: {e}")
            conn.close()
            return {"Error": "Request failed."}

        if response.status_code == 200:
            tree = html.fromstring(response_content)
            
            # Extract data from the API response
            for element in response_json['data']:
                if element['selector'] == "//span[@id='productTitle']":
                    if element['results']:
                        data_dict["Product Title"] = element['results'][0].get('text', 'Not found')
                elif element['selector'] == "//a[@id='bylineInfo']":
                    if element['results']:
                        data_dict["Brand Store"] = element['results'][0].get('text', 'Not found')
                elif element['selector'] == "//div[@id='detailBullets_feature_div']":
                    if element['results']:
                        bullet_points = element['results'][0].get('text', '')
                        if "Manufacturer" in bullet_points:
                            manufacturer = bullet_points.split("Manufacturer")[1].strip().split("\n")[0].strip()
                            data_dict["Manufacturer"] = manufacturer.split(":")[1].strip() if ":" in manufacturer else manufacturer.strip()
                        if "Item model number" in bullet_points:
                            model_number = bullet_points.split("Item model number")[1].strip().split("\n")[0].strip()
                            data_dict["Item model number"] = model_number.split(":")[1].strip() if ":" in model_number else model_number.strip()
                elif element['selector'] == "//table[@id='productDetails_techSpec_section_1']":
                    if element['results']:
                        table_data = element['results'][0].get('text', '')
                        if "Manufacturer" in table_data:
                            manufacturer = table_data.split("Manufacturer")[1].strip().split("\n")[0].strip()
                            data_dict["Manufacturer"] = manufacturer.split("\t")[1].strip() if "\t" in manufacturer else manufacturer.strip()
                        if "Item model number" in table_data:
                            model_number = table_data.split("Item model number")[1].strip().split("\n")[0].strip()
                            data_dict["Item model number"] = model_number.split("\t")[1].strip() if "\t" in model_number else model_number.strip()

            # Cache the data only if "Not Found" is not present in any of the fields
            if "Not Found" not in data_dict.values():
                data_json = json.dumps(data_dict)
                c.execute("INSERT OR REPLACE INTO cache VALUES (?, ?, datetime('now'))", (url, data_json))
                conn.commit()

            conn.close()
            return data_dict

        elif response.status_code == 429:
            retries += 1
            exponential_delay = delay * (2 ** retries) + random.uniform(0, 1)
            logging.warning(f"Rate limit exceeded for {url}. Retrying in {exponential_delay:.2f} seconds...")
            time.sleep(exponential_delay)
        else:
            logging.error(f"Failed to fetch data for {url}. Status code: {response.status_code}")
            conn.close()
            return {"Error": f"Failed to fetch data. Status code: {response.status_code}"}

    # If all retries failed
    conn.close()
    return {"Error": "Maximum retries exceeded."}

# Main app
if st.button("Scrape Data"):
    if urls and api_key:
        MAX_LINKS = 1000
        if len(urls) > MAX_LINKS:
            st.warning(f"You have entered {len(urls)} links. Scraping more than {MAX_LINKS} links at once may take a long time and exceed rate limits.")

        progress_bar = st.progress(0)
        
        with st.spinner("Scraping data..."):
            all_data = []
            
            with ThreadPoolExecutor(max_workers=concurrent_scrapes) as executor:
                futures = [executor.submit(scrape_data, url, api_key, initial_delay=2 if use_initial_delay else 0) for url in urls]
                for i, future in enumerate(futures):
                    result = future.result()
                    all_data.append(result)
                    
                    progress_bar.progress((i + 1) / len(urls))
                    
                    if "Error" in result:
                        st.error(result["Error"])
                    
                    time.sleep(1)  # Add a delay of 1 second between requests
            
            df = pd.DataFrame(all_data)
            df['Scrape Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Store the DataFrame in session state
            st.session_state.df = df
            
            st.dataframe(df)
            
    else:
        st.warning("Please input an API key and at least one URL.")

# Download button in the sidebar
if 'df' in st.session_state:
    csv = st.session_state.df.to_csv(index=False)
    st.sidebar.download_button(
        label="Download Data as CSV",
        data=csv,
        file_name='scraped_data.csv',
        mime='text/csv',
    )

if st.button("Clear Data"):
    st.session_state.clear()

# Suggesting next steps for the user
st.sidebar.header("Next Steps")
st.sidebar.markdown("""
- Review the scraped data
- Enhance the scraper for more details
""")
