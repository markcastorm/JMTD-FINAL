import time
from urllib.parse import urljoin
import requests
from pathlib import Path
import json
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import sys
import os

# --- WINDOWS UNICODE FIX ---
# Set console encoding to UTF-8 for Windows compatibility
if sys.platform.startswith('win'):
    try:
        # Try to set console to UTF-8
        os.system('chcp 65001 >nul 2>&1')
        # Reconfigure stdout for UTF-8
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

# Safe print function for Windows compatibility
def safe_print(message):
    """Print function that handles Unicode characters safely on Windows"""
    try:
        print(message)
    except UnicodeEncodeError:
        # Fallback: replace problematic Unicode characters
        safe_message = message.encode('ascii', 'replace').decode('ascii')
        print(safe_message)

# --- 1. CENTRALIZED CONFIGURATION ---
YEARS_TO_PROCESS = 1
BASE_OUTPUT_DIRECTORY = Path("JPX_Monthly_Reports")
LOG_FILE_PATH = Path("jpx_master_download_log.json")

MONTH_MAP = {
    "Jan.": "01_January", "Feb.": "02_February", "Mar.": "03_March",
    "Apr.": "04_April",   "May": "05_May",      "Jun.": "06_June",
    "Jul.": "07_July",    "Aug.": "08_August",   "Sep.": "09_September",
    "Oct.": "10_October", "Nov.": "11_November", "Dec.": "12_December"
}

def load_download_log(log_file_path):
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    except FileNotFoundError:
        safe_print("Log file not found. A new one will be created. Scraping all found files.")
        return set()

def save_download_log(log_file_path, downloaded_files_set):
    safe_print(f"\nSaving updated log to '{log_file_path.name}'...")
    with open(log_file_path, 'w', encoding='utf-8') as f:
        json.dump(sorted(list(downloaded_files_set)), f, indent=4)
    safe_print("Log saved.")

def download_file(session, url, file_path):
    """Uses a requests session to efficiently download a file."""
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        with open(file_path, 'wb') as f:
            f.write(response.content)
        return True
    except requests.RequestException as e:
        safe_print(f"    -> [ERROR] Network error downloading {url}. Error: {e}")
        return False

def process_category(category_name, file_filter, session, master_log, base_dir):
    """
    Handles the entire process for a single category using one Selenium instance
    to ensure all JavaScript is rendered correctly before parsing.
    """
    safe_print(f"\n==================================================")
    safe_print(f"      PROCESSING CATEGORY: {category_name.upper()}")
    safe_print(f"==================================================")
    
    driver = None
    try:
        # --- 1. SETUP AND NAVIGATION ---
        options = uc.ChromeOptions()
        # Uncomment the next line for headless mode
        # options.add_argument('--headless=new')
        
        # Add Windows-specific Chrome options
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--remote-debugging-port=9222')
        
        driver = uc.Chrome(options=options)
        wait = WebDriverWait(driver, 20)
        start_url = "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html"
        driver.get(start_url)
        
        try:
            wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Agree')]"))).click()
            safe_print("-> Clicked 'Agree' on cookie banner.")
        except (TimeoutException, NoSuchElementException):
            safe_print("-> Cookie banner not found or already accepted.")
        
        wait.until(EC.element_to_be_clickable((By.LINK_TEXT, category_name))).click()
        wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Monthly"))).click()
        safe_print(f"-> Navigated to '{category_name}' > 'Monthly' tab.")
        time.sleep(1.5)

        # --- 2. GET LIST OF ARCHIVE PAGES ---
        archive_container = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "number-select-box")))
        archive_dropdown = archive_container.find_element(By.CLASS_NAME, "backnumber")
        select = Select(archive_dropdown)
        archive_pages = {opt.text.strip(): urljoin(driver.current_url, opt.get_attribute('value')) for opt in select.options}
        years_to_scrape = list(archive_pages.items())[:YEARS_TO_PROCESS]
        safe_print(f"-> Found {len(years_to_scrape)} year(s) to process: {[y[0] for y in years_to_scrape]}")

        # --- 3. VISIT EACH ARCHIVE PAGE AND SCRAPE ---
        for year, url in years_to_scrape:
            safe_print(f"\n-=-=-=-=-= Processing Year: {year} =-=-=-=-=-")
            driver.get(url)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
            
            soup = BeautifulSoup(driver.page_source, 'lxml')
            
            all_tables = soup.find_all('table')
            monthly_data_table = next((t for t in all_tables if t.find('thead') and 'Jan.' in t.find('thead').text), None)

            if not monthly_data_table:
                safe_print(f"  -> [ERROR] Could not identify the main monthly data table for {year}.")
                continue
            
            safe_print(f"  -> [SUCCESS] Identified data table for {year}.")
            
            table_body = monthly_data_table.find('tbody')
            data_rows = table_body.find_all('tr') if table_body else []
            if len(data_rows) < 2:
                safe_print(f"  -> Table for {year} has fewer than 2 rows. Cannot find Excel files.")
                continue

            excel_row = data_rows[1]
            data_cells = excel_row.find_all('td')
            month_headers = [th.text.strip() for th in monthly_data_table.find('thead').find_all('th')]
            
            new_files_this_year = 0
            for month_header, cell in zip(month_headers, data_cells):
                link = cell.find('a', href=lambda h: h and (h.endswith('.xls') or h.endswith('.xlsx')))
                if not (link and file_filter in link['href']):
                    continue

                file_name = urljoin(driver.current_url, link['href']).split('/')[-1]
                if file_name in master_log:
                    continue
                
                new_files_this_year += 1
                month_folder_name = MONTH_MAP.get(month_header, "Unknown_Month")
                
                # --- Build the new path structure ---
                # The path will now be BASE_DIRECTORY / YEAR / MONTH
                final_output_path = base_dir / year / month_folder_name
                final_output_path.mkdir(parents=True, exist_ok=True)
                
                safe_print(f"  -> New file for month '{month_header}': '{file_name}'.")
                report_url = urljoin(driver.current_url, link['href'])
                
                file_path = final_output_path / file_name
                
                if download_file(session, report_url, file_path):
                    safe_print(f"    -> [SUCCESS] Successfully saved to: {year}/{month_folder_name}/{file_name}")
                    master_log.add(file_name)
                else:
                    safe_print(f"    -> [ERROR] Download failed for {file_name}")

            if new_files_this_year == 0:
                safe_print(f"  -> No new files to download for {year} (all files are up-to-date).")
    
    except Exception as e:
        safe_print(f"[ERROR] An unexpected error occurred while processing {category_name}. Error: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass  # Ignore cleanup errors
        safe_print(f"\n-=-=-=-=-= Finished processing {category_name}! =-=-=-=-=-")

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    categories_to_scrape = {
        "ETFs": "etf_m",
        "REITs": "reit_m"
    }
    
    try:
        BASE_OUTPUT_DIRECTORY.mkdir(exist_ok=True)
        master_download_log = load_download_log(LOG_FILE_PATH)
        initial_log_size = len(master_download_log)
        
        http_session = requests.Session()
        http_session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'})

        for category, file_id in categories_to_scrape.items():
            process_category(
                category_name=category, 
                file_filter=file_id, 
                session=http_session, 
                master_log=master_download_log, 
                base_dir=BASE_OUTPUT_DIRECTORY
            )

        if len(master_download_log) > initial_log_size:
            save_download_log(LOG_FILE_PATH, master_download_log)
        else:
            safe_print("\nNo new files were downloaded across all categories. Log file is up to date.")

        safe_print("\n\n==================================================")
        safe_print("      ALL CATEGORIES PROCESSED. SCRIPT FINISHED.")
        safe_print("==================================================")
        
        # Exit with success code
        sys.exit(0)
        
    except KeyboardInterrupt:
        safe_print("\n[INTERRUPTED] Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        safe_print(f"\n[CRITICAL ERROR] {str(e)}")
        sys.exit(1)