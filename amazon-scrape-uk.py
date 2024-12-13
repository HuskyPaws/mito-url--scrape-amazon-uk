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

# Set up logging with more detail
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s',
                   handlers=[
                       logging.StreamHandler()
                   ])

# Initialize session state for debug info if it doesn't exist
if 'debug_info' not in st.session_state:
    st.session_state.debug_info = []

# Set up cache database
def get_db_connection():
    conn = sqlite3.connect('cache.db', check_same_thread=False)
    conn.execute('''CREATE TABLE IF NOT EXISTS cache
                 (url TEXT PRIMARY KEY, data TEXT, timestamp TIMESTAMP)''')
    return conn

# Streamlit app title
st.title("Amazon Product Data Scraper - UK")

# Debug expander for raw data
debug_expander = st.expander("Debug Information", expanded=False)

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
        if "Not found" not in data_dict.values():
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

    data_dict = {
        "Product URL": url,
        "Product Title": "Not found",
        "Brand Store": "Not found",
        "Brand Store URL": "Not found",
        "Item Model Number": "Not found",
        "Manufacturer": "Not found",
        "Debug Info": {}
    }

    retries = 0
    delay = initial_delay

    while retries < max_retries:
        try:
            scrapeowl_url = "https://api.scrapeowl.com/v1/scrape"
            object_of_data = {
                "api_key": api_key,
                "url": url,
                "premium_proxies": True,
                "country": "gb",
                "render_js": True,
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

            response = requests.post(scrapeowl_url, data, headers=headers)
            response_content = response.content.decode('unicode_escape')
            tree = html.fromstring(response_content)
            byline_info = tree.xpath('//a[@id="bylineInfo"]')
            if byline_info:
                data_dict["Brand Store URL"] = "https://www.amazon.co.uk" + byline_info[0].get('href')
            response_json = response.json()

            # Store raw response for debugging
            data_dict["Debug Info"]["Raw Response"] = response_json
            logging.info(f"Raw ScrapeOwl Response for {url}:")
            logging.info(json.dumps(response_json, indent=2))

            if response.status_code == 200:
                # Process each element from the API response
                for element in response_json.get('data', []):
                    # Store element data for debugging
                    data_dict["Debug Info"][element['selector']] = element

                    # Handle product title
                    if element['selector'] == "//span[@id='productTitle']":
                        if element.get('results'):
                            data_dict["Product Title"] = element['results'][0].get('text', '').strip()

                    # Handle brand store
                    elif element['selector'] == "//a[@id='bylineInfo']":
                        if element.get('results'):
                            data_dict["Brand Store"] = element['results'][0].get('text', '').strip()

                    # Handle detail bullets
                    elif element['selector'] == "//div[@id='detailBullets_feature_div']":
                        if not element.get('error') and element.get('results'):
                            bullet_points = element['results'][0].get('text', '')
                            
                            # Check for manufacturer
                            if "Manufacturer" in bullet_points:
                                try:
                                    manufacturer = bullet_points.split("Manufacturer")[1].strip().split("\n")[0].strip()
                                    data_dict["Manufacturer"] = manufacturer.split(":")[1].strip() if ":" in manufacturer else manufacturer.strip()
                                except (IndexError, KeyError) as e:
                                    logging.error(f"Error processing Manufacturer: {str(e)}")

                            # Check for model number
                            if "Item model number" in bullet_points:
                                try:
                                    model_number = bullet_points.split("Item model number")[1].strip().split("\n")[0].strip()
                                    data_dict["Item Model Number"] = model_number.split(":")[1].strip() if ":" in model_number else model_number.strip()
                                except (IndexError, KeyError) as e:
                                    logging.error(f"Error processing Model Number: {str(e)}")

                    # Handle tech specs table
                    elif element['selector'] == "//table[@id='productDetails_techSpec_section_1']":
                        if element.get('results'):
                            table_data = element['results'][0].get('text', '')
                            
                            # Check manufacturer in table
                            if "Manufacturer" in table_data and data_dict["Manufacturer"] == "Not found":
                                try:
                                    manufacturer = table_data.split("Manufacturer")[1].strip().split("\n")[0].strip()
                                    data_dict["Manufacturer"] = manufacturer.split("\t")[1].strip() if "\t" in manufacturer else manufacturer.strip()
                                except (IndexError, KeyError) as e:
                                    logging.error(f"Error processing table Manufacturer: {str(e)}")

                            # Check for model number in table
                            if "Item model number" in table_data and data_dict["Item Model Number"] == "Not found":
                                try:
                                    model_number = table_data.split("Item model number")[1].strip().split("\n")[0].strip()
                                    data_dict["Item Model Number"] = model_number.split("\t")[1].strip() if "\t" in model_number else model_number.strip()
                                except (IndexError, KeyError) as e:
                                    logging.error(f"Error processing table Model Number: {str(e)}")

                # Cache the data only if we found some valid information
                cacheable_data = {k: v for k, v in data_dict.items() if k != "Debug Info"}
                if any(value != "Not found" for value in cacheable_data.values()):
                    data_json = json.dumps(cacheable_data)
                    c.execute("INSERT OR REPLACE INTO cache VALUES (?, ?, datetime('now'))", (url, data_json))
                    conn.commit()

                conn.close()
                return data_dict

            elif response.status_code == 429:
                retries += 1
                exponential_delay = delay * (2 ** retries) + random.uniform(0, 1)
                logging.warning(f"Rate limit exceeded for {url}. Retrying in {exponential_delay:.2f} seconds...")
                time.sleep(exponential_delay)
                continue
            else:
                logging.error(f"Failed to fetch data for {url}. Status code: {response.status_code}")
                conn.close()
                return {"Error": f"Failed to fetch data. Status code: {response.status_code}"}

        except Exception as e:
            logging.error(f"Exception occurred while processing {url}: {str(e)}")
            retries += 1
            if retries >= max_retries:
                conn.close()
                return {"Error": f"Exception occurred: {str(e)}"}
            time.sleep(delay * (2 ** retries) + random.uniform(0, 1))

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
        status_container = st.empty()
        debug_info = []
        
        with st.spinner("Scraping data..."):
            all_data = []
            
            with ThreadPoolExecutor(max_workers=concurrent_scrapes) as executor:
                futures = [executor.submit(scrape_data, url, api_key, initial_delay=2 if use_initial_delay else 0) 
                          for url in urls]
                
                for i, future in enumerate(futures):
                    try:
                        result = future.result(timeout=60)  # 60 second timeout per URL
                        
                        # Store debug info
                        if "Debug Info" in result:
                            debug_info.append({
                                "URL": urls[i],
                                "Debug Data": result["Debug Info"]
                            })
                            # Remove debug info from main results
                            result_without_debug = {k: v for k, v in result.items() if k != "Debug Info"}
                            all_data.append(result_without_debug)
                        else:
                            all_data.append(result)
                        
                        # Update progress
                        progress = (i + 1) / len(urls)
                        progress_bar.progress(progress)
                        status_container.text(f"Processed {i + 1} of {len(urls)} URLs")
                        
                        if "Error" in result:
                            st.error(f"Error processing URL {urls[i]}: {result['Error']}")
                        
                        # Add a small delay between requests
                        if i < len(urls) - 1:
                            time.sleep(1)
                            
                    except Exception as e:
                        st.error(f"Failed to process URL {urls[i]}: {str(e)}")
                        all_data.append({"Error": str(e)})
            
            # Create DataFrame and add timestamp
            df = pd.DataFrame(all_data)
            df['Scrape Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Store the DataFrame and debug info in session state
            st.session_state.df = df
            st.session_state.debug_info = debug_info
            
            # Display the results
            st.success("Scraping completed!")
            st.dataframe(df)
            
            # Display debug information in the expander
            with debug_expander:
                st.write("Debug Information for Each URL:")
                for debug_item in debug_info:
                    st.write(f"\nURL: {debug_item['URL']}")
                    st.json(debug_item['Debug Data'])
            
    else:
        st.warning("Please input an API key and at least one URL.")

# Download buttons in the sidebar
if 'df' in st.session_state:
    # Download scraped data
    csv = st.session_state.df.to_csv(index=False)
    st.sidebar.download_button(
        label="Download Data as CSV",
        data=csv,
        file_name='scraped_data.csv',
        mime='text/csv',
    )
    
    # Download debug data if available
    if st.session_state.debug_info:
        debug_json = json.dumps(st.session_state.debug_info, indent=2)
        st.sidebar.download_button(
            label="Download Debug Data",
            data=debug_json,
            file_name='debug_data.json',
            mime='application/json',
            key='debug_download'
        )

# Clear data button
if st.button("Clear Data"):
    st.session_state.clear()
    st.success("Data cleared successfully!")

# Suggesting next steps for the user
st.sidebar.header("Next Steps")
st.sidebar.markdown("""
- Review the scraped data
- Check debug information for any issues
- Download the results as CSV
- Download debug data for troubleshooting
- Clear data and start a new scraping session
""")
