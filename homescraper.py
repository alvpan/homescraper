import re
import pprint
from collections import defaultdict
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2
from datetime import datetime
import threading
import time

# PostgreSQL setup
config = {
    # 'dbname': 'test_verceldb', #This writes to the TEST database
    'dbname': 'verceldb', # !!! This writes to the REAL database
    'user': 'default',
    'password': '1SVvFXplu2hT',
    'host': 'ep-dawn-lab-a4lkz8oy.us-east-1.aws.neon.tech',
    'port': '5432',
    'sslmode': 'require'
}

# Proxy Setup
HOSTNAME = 'gr.smartproxy.com'
PORT = '30000'
proxy = '{hostname}:{port}'.format(hostname=HOSTNAME, port=PORT)

# Dictionary mapping table names to URLs
dict_table_url = {
    'Action_City1_Province': 'https://www.dummy.url',
    # e.t.c
}

# Lock for database operations
lock = threading.Lock()

def get_or_create_id(cursor, table, name, related_table=None, related_id=None):
    if related_table and related_id:
        cursor.execute(f'SELECT id FROM "{table}" WHERE name=%s AND {related_table}=%s', (name, related_id))
    else:
        cursor.execute(f'SELECT id FROM "{table}" WHERE name=%s', (name,))
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    if related_table and related_id:
        cursor.execute(f'INSERT INTO "{table}" (name, {related_table}) VALUES (%s, %s) RETURNING id', (name, related_id))
    else:
        cursor.execute(f'INSERT INTO "{table}" (name) VALUES (%s) RETURNING id', (name,))
    
    return cursor.fetchone()[0]

def Scraper(table_name):
    sqmeters = []
    price = []

    conn = None
    cursor = None

    try:
        with lock:
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            conn.autocommit = False
            print(f"\nProceeding with scraping for {table_name}...")

    except psycopg2.Error as error:
        print(f"\nDatabase error: {error}\n")
        return False

    try:
        action = 'No Action'
        url = dict_table_url.get(table_name, 'No Url')

        if table_name.startswith('Rent'):
            action = 'Rent'
        if table_name.startswith("Buy"):
            action = 'Buy'

        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
        options.add_argument('--proxy-server={}'.format(proxy))

        def generate_dynamic_chrome_user_agent():
            base_string = "Mozilla/5.0 ({os}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36"
            os_to_chrome_versions = {
                "Windows NT 10.0; Win64; x64": ["121.0.6167.140", "121.0.6167.139", "121.0.6167.85", "121.0.6167.86"],
                "Macintosh; Intel Mac OS X 10_15_7": ["121.0.6167.139", "121.0.6167.85"],
                "Macintosh; Intel Mac OS X 11_7_10": ["121.0.6167.139", "121.0.6167.85"],
                "X11; Linux x86_64": ["121.0.6167.139", "121.0.6167.85"]
            }

            os_keys = list(os_to_chrome_versions.keys())
            random.shuffle(os_keys)
            os = random.choice(os_keys)
            chrome_versions = os_to_chrome_versions[os]
            random.shuffle(chrome_versions)
            version = random.choice(chrome_versions)
            user_agent = base_string.format(os=os, chrome_version=version)
            return user_agent

        user_agent = generate_dynamic_chrome_user_agent()
        print("Agent:", user_agent)

        options.add_argument(f'user-agent={user_agent}')

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.get(url)
        driver.implicitly_wait(10)
        time.sleep(10)

        text = driver.find_element(By.TAG_NAME, 'body').text
        # print("Page body:\n", text)

        captcha_indicators = ["I am not a robot", "CAPTCHA", "captcha"]
        captcha_found = False
        for indicator in captcha_indicators:
            if driver.find_elements(By.XPATH, f"//*[contains(text(), '{indicator}')]"):
                captcha_found = True
                break

        if not captcha_found and driver.find_elements(By.CSS_SELECTOR, "iframe[src*='recaptcha']"):
            captcha_found = True

        driver.quit()

        def remove_lines_by_length(text):
            lines = text.split('\n')
            filtered_lines = [line for line in lines if 4 <= len(line) <= 30]
            return '\n'.join(filtered_lines)

        text = remove_lines_by_length(text)

        def delete_listings_without_price(text):
            lines = text.split('\n')
            lines_to_remove = set()
            for i in range(len(lines)-2):
                if lines[i].endswith('τ.μ.') and lines[i+2].endswith('τ.μ.'):
                    lines_to_remove.add(i)
                    if i > 0:
                        lines_to_remove.add(i-1)
            return '\n'.join([line for i, line in enumerate(lines) if i not in lines_to_remove])

        text = delete_listings_without_price(text)

        def remove_lines_not_followed_by_euro_two_lines_ahead(text):
            lines = text.split('\n')
            lines_to_remove = []
            for i in range(len(lines) - 2):
                current_line = lines[i]
                line_after_next = lines[i + 2]
                if current_line.endswith('τ.μ.') and not line_after_next.startswith('€'):
                    lines_to_remove.append(i)
            for index in sorted(lines_to_remove, reverse=True):
                del lines[index]
            return '\n'.join(lines)

        text = remove_lines_not_followed_by_euro_two_lines_ahead(text)

        def delete_lines_after_pattern_until_euro(text, pattern):
            lines = text.splitlines()
            new_lines = []
            skip_lines = False
            for line in lines:
                if not skip_lines and re.search(pattern, line):
                    new_lines.append(line)
                    skip_lines = True
                elif skip_lines and line.startswith('€'):
                    new_lines.append(line)
                    skip_lines = False
            return "\n".join(new_lines)

        text = delete_lines_after_pattern_until_euro(text, r'τ\.μ\.$|m²$')

        lines = text.strip().split('\n')
        if lines and lines[-1].endswith('τ.μ.'):
            lines = lines[:-1]
        text = '\n'.join(lines)

        if action == 'Buy':
            def delete_second_euro_line(text):
                lines = text.split('\n')
                processed_lines = []
                prev_line_starts_with_euro = False
                for line in lines:
                    current_line_starts_with_euro = line.startswith('€')
                    if not (current_line_starts_with_euro and prev_line_starts_with_euro):
                        processed_lines.append(line)
                    prev_line_starts_with_euro = current_line_starts_with_euro
                return '\n'.join(processed_lines)
            text = delete_second_euro_line(text)

        sqmeters = re.findall(r',.*(?=τ\.μ\.)|,.*(?=m²)', text)
        if action == 'Buy':
            price = re.findall(r'€(.*)', text)
        if action == 'Rent':
            price = re.findall(r'€(.*)/', text)

        def extract_numbers(my_list):
            processed_list = []
            for s in my_list:
                numbers_only = re.sub(r'\D', '', s)
                processed_list.append(numbers_only)
            return processed_list

        print('\n')
        print('///////////////////////////////////////////////////////////////////////////////////')
        sqmeters = extract_numbers(sqmeters)
        print(f'{table_name} Sq.Meters:', sqmeters, 'size:', len(sqmeters))
        price = extract_numbers(price)
        print(f'{table_name} Price:', price, 'size:', (len(price)))
        print('\n')

        if len(sqmeters) > 0 and len(price) > 0:
            if len(sqmeters) == len(price):
                sqmeters_price_dict = defaultdict(list)
                for sqm, p in zip(sqmeters, price):
                    sqmeters_price_dict[int(sqm)].append(int(p))
                sqmeters_price_dict = dict(sqmeters_price_dict)
                pprint.pprint(sqmeters_price_dict)
            else:
                print(f'\n!ERROR: NOT EQUAL LIST LENGTHS! for {table_name}.\n')
        else:
            if len(sqmeters) == 0 or len(price) == 0:
                print(f'\n!ERROR: sqmeters AND price list are EMPTY! for {table_name}.\n')

        def get_dict_average(dictionary):
            average = []
            dict_values = list(dictionary.values())
            for i in range(len(dict_values)):
                average.append(int(sum(dict_values[i])/len(dict_values[i])))
            average_dictionary = {key: value for key, value in zip(dictionary.keys(), average)}
            return(average_dictionary)

        average_sqmeters_price_dict = get_dict_average(sqmeters_price_dict)
        print(f"Scraping for {table_name} was successful.\n")

        # Data to SQL
        try:
            with lock:
                conn = psycopg2.connect(**config)
                cursor = conn.cursor()
                country_id = get_or_create_id(cursor, "Country", "Greece")
                province_id = get_or_create_id(cursor, "Province", "No Province", "country", country_id)
                city = table_name.split("_")[1]
                city_id = get_or_create_id(cursor, "City", city, "province", province_id)
                area = table_name.split("_")[2] if len(table_name.split("_")) == 3 else city
                area_id = get_or_create_id(cursor, "Area", area, "city", city_id)

                price_type = 1 if action == "Rent" else 2

                custom_date = datetime.now().date()

                for size, price in average_sqmeters_price_dict.items():
                    cursor.execute(f"""
                        INSERT INTO "PriceEntry" (
                            area, entry_date, price, price_type, surface
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (area_id, custom_date, price, price_type, size))

                conn.commit()
                print("Data successfully inserted into PriceEntry.")
        except Exception as e:
            print(f"Error in database operation: {str(e)}")
            conn.rollback()

        print("\nAction:", action)
        print("\nUrl:", url)
        print("\n")
        return True

    except Exception as e:
        print(f"Scraping failed for {table_name}. Error: {str(e)}\n")
        return False

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def scraping_task(table_names_subset):

    successful_scrapes = 0
    total_scrapes = len(table_names_subset)
    
    for table_name in table_names_subset:
        retries = 3  # Retry Limit
        scraping_successful = False

        while not scraping_successful and retries > 0:
            scraping_successful = Scraper(table_name)

            if not scraping_successful:
                print(f"Scraping for {table_name} failed. Trying again...")
                retries -= 1
                time.sleep(random.uniform(5, 10))
            else:
                successful_scrapes += 1

            print(f"Current success rate: {successful_scrapes}/{total_scrapes} ({successful_scrapes/total_scrapes*100:.2f}%)")
        
        if retries == 0:
            print(f"Skipping {table_name} after multiple failures.")
        
    print(f"Final success rate: {successful_scrapes}/{total_scrapes} ({successful_scrapes/total_scrapes*100:.2f}%)")
    print("Scraping task completed.")

all_table_names = list(dict_table_url.keys())

# Divide the list of names in 4 quarters
quarter_length = len(all_table_names) // 4
first_quarter = all_table_names[:quarter_length]
second_quarter = all_table_names[quarter_length:2 * quarter_length]
third_quarter = all_table_names[2 * quarter_length:3 * quarter_length]
fourth_quarter = all_table_names[3 * quarter_length:]

# Creating threads
thread1 = threading.Thread(target=scraping_task, args=(first_quarter,))
thread2 = threading.Thread(target=scraping_task, args=(second_quarter,))
thread3 = threading.Thread(target=scraping_task, args=(third_quarter,))
thread4 = threading.Thread(target=scraping_task, args=(fourth_quarter,))

start_time = time.time()

thread1.start()
thread2.start()
thread3.start()
thread4.start()

# Waiting for all threads to finish
thread1.join()
thread2.join()
thread3.join()
thread4.join()

print("\nAll table names have been successfully scraped.\n")
end_time = time.time()
elapsed_time_seconds = end_time - start_time
minutes = elapsed_time_seconds // 60
seconds = elapsed_time_seconds % 60
print(f"Total elapsed time: {minutes} minutes and {seconds} seconds.")
