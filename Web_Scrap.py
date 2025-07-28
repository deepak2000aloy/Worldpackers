import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
import time
import csv
import psycopg2
from psycopg2 import sql

max_pages = 150

# Setup Chrome options
options = uc.ChromeOptions()
options.headless = False
options.add_argument("--start-maximized")
options.add_argument("--disable-blink-features=AutomationControlled")

# Launch driver
driver = uc.Chrome(options=options)
driver.get("https://www.worldpackers.com/search/asia?q=asia")

# === Accept cookie popup if visible ===
try:
    cookie_button = WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
    )
    cookie_button.click()
    print(" Cookie accepted.")
    time.sleep(1)
except:
    print(" No cookie popup.")

all_data = []
page = 1

while page <= max_pages:
    print(f"\n Scraping page {page}...")

    # Wait for cards to load
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "a.main-thumb.vp-card.block"))
    )

    #  Scroll 
    scroll_pause_time = 1
    screen_height = driver.execute_script("return window.screen.height;")
    scroll_height = driver.execute_script("return document.body.scrollHeight;")

    for i in range(1, scroll_height // screen_height + 2):
        driver.execute_script(f"window.scrollTo(0, {i * screen_height});")
        time.sleep(scroll_pause_time)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    cards = soup.find_all('a', {'class': 'main-thumb vp-card block'})



    for card in cards:
        link = card['href'] if card.has_attr('href') else None
        listing_id = link.split('/')[-1] if link else "None"

        title_tag = card.find('h2', {'class': 'h5 mg_bot_5'})
        title = title_tag.text.strip() if title_tag else "None"

        img_tag = card.find('img', {'class': 'b-lazy vp_photo b-loaded'})
        img = img_tag['src'] if img_tag and img_tag.has_attr('src') else (
            img_tag['data-src'] if img_tag and img_tag.has_attr('data-src') else "No Image")


        host_type = location = volunteering_work = working_hours = minimum_day = accommodation = food = "None"

        div_1 = card.find('p', {'class': 'with-separators mg_bot_5'})
        if div_1:
            span1 = div_1.find_all('span', {'class': 'text small'})
            if len(span1) > 0:
                host_type = span1[0].text.strip()
            if len(span1) > 1:
                location = span1[1].text.strip()

        div_2 = card.find('div', {'class': 'with-separators'})
        if div_2:
            span2 = div_2.find_all('span', {'class': 'text small'})
            if len(span2) > 0:
                working_hours = span2[0].text.strip()
            if len(span2) > 1:
                volunteering_work = span2[1].text.strip()

        div_3 = card.find('div', {'class': 'with-separators mg_bot_10'})
        if div_3:
            span3 = div_3.find_all('span', {'class': 'text small'})
            if len(span3) > 0:
                minimum_day = span3[0].text.strip()
            if len(span3) > 1:
                accommodation = span3[1].text.strip()
            if len(span3) > 2:
                food = span3[2].text.strip()

        rating_tag = card.find('li', {'class': 'orange'})
        rating = rating_tag.text.strip() if rating_tag else "None"

        print(f"{listing_id} {title} | {host_type} | {location} | {volunteering_work} | {working_hours} | {minimum_day} | {accommodation} | {food} | {rating} | {img}")
        all_data.append([listing_id,title, host_type, location, volunteering_work, working_hours, minimum_day, accommodation, food, rating, img])

    # Move to next page if not on last
    if page < max_pages:
        try:
            next_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '//a[@rel="next"]'))
            )
            # Scroll into view in case of obstruction
            driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            time.sleep(1)
            try:
                next_button.click()
            except ElementClickInterceptedException:
                print("Element blocked! Trying JavaScript click...")
                driver.execute_script("arguments[0].click();", next_button)
            page += 1
            time.sleep(5)
        except TimeoutException:
            print(" Next button not found")
            break
    else:
        break

driver.quit()
print(f"\n Done scraping {len(all_data)} listings across {page} pages.")

csv_file = "worldpackers_data.csv"
with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["listing id"
        "Title", "Host Type", "Location", "Volunteering Work",
        "Working Hours", "Minimum Stay", "Accommodation", "Food", "Rating" , "Image"
    ])
    writer.writerows(all_data)

print(f"\n Done scraping {len(all_data)} listings across {page} pages.")
print(f" Data exported to {csv_file}")

PG_HOST = 'localhost'
PG_PORT = 5432
PG_DB = 'worldpackers_db'
PG_USER = 'your_pg_user'
PG_PASS = 'your_pg_password'
