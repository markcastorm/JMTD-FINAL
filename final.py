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

# HEADLESS MODE CONFIGURATION
HEADLESS_MODE = True  # Set to False for debugging with visible browser

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

def setup_chrome_options(headless=True):
    """Setup Chrome options for optimal performance and compatibility"""
    options = uc.ChromeOptions()
    
    # Headless mode configuration
    if headless:
        safe_print("-> Running in HEADLESS mode (no browser window)")
        options.add_argument('--headless=new')  # Use new headless mode
    else:
        safe_print("-> Running in VISIBLE mode (browser window will appear)")
    
    # Essential Chrome arguments for stability and performance
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-images')  # Don't load images for faster performance
    options.add_argument('--disable-javascript-debugger')
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-renderer-backgrounding')
    
    # Memory and performance optimizations
    options.add_argument('--memory-pressure-off')
    options.add_argument('--max_old_space_size=4096')
    options.add_argument('--single-process')  # Use single process mode
    
    # Window size (important even in headless mode)
    options.add_argument('--window-size=1920,1080')
    
    # User agent to avoid detection
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Disable logging for cleaner output
    options.add_argument('--log-level=3')
    options.add_argument('--silent')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Prefs for better performance
    prefs = {
        "profile.default_content_setting_values": {
            "images": 2,  # Block images
            "plugins": 2,  # Block plugins
            "popups": 2,  # Block popups
            "geolocation": 2,  # Block location sharing
            "notifications": 2,  # Block notifications
            "media_stream": 2,  # Block media stream
        },
        "profile.managed_default_content_settings": {
            "images": 2
        }
    }
    options.add_experimental_option("prefs", prefs)
    
    return options

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
        safe_print("-> Setting up Chrome driver...")
        options = setup_chrome_options(headless=HEADLESS_MODE)
        
        # Create driver with options
        driver = uc.Chrome(options=options)
        wait = WebDriverWait(driver, 20)
        
        safe_print("-> Navigating to JPX website...")
        start_url = "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html"
        driver.get(start_url)
        
        # Handle cookie banner
        try:
            safe_print("-> Checking for cookie banner...")
            cookie_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Agree')]")))
            cookie_button.click()
            safe_print("-> Clicked 'Agree' on cookie banner.")
            time.sleep(1)
        except (TimeoutException, NoSuchElementException):
            safe_print("-> Cookie banner not found or already accepted.")
        
        # Navigate to category and monthly data
        safe_print(f"-> Navigating to '{category_name}' section...")
        category_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, category_name)))
        category_link.click()
        time.sleep(1)
        
        safe_print("-> Clicking 'Monthly' tab...")
        monthly_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Monthly")))
        monthly_link.click()
        safe_print(f"-> Successfully navigated to '{category_name}' > 'Monthly' tab.")
        time.sleep(2)  # Allow page to fully load

        # --- 2. GET LIST OF ARCHIVE PAGES ---
        safe_print("-> Finding archive dropdown...")
        archive_container = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "number-select-box")))
        archive_dropdown = archive_container.find_element(By.CLASS_NAME, "backnumber")
        select = Select(archive_dropdown)
        
        # Get available years
        archive_pages = {}
        for option in select.options:
            year_text = option.text.strip()
            year_url = urljoin(driver.current_url, option.get_attribute('value'))
            archive_pages[year_text] = year_url
        
        years_to_scrape = list(archive_pages.items())[:YEARS_TO_PROCESS]
        safe_print(f"-> Found {len(years_to_scrape)} year(s) to process: {[y[0] for y in years_to_scrape]}")

        # --- 3. VISIT EACH ARCHIVE PAGE AND SCRAPE ---
        for year, url in years_to_scrape:
            safe_print(f"\n-=-=-=-=-= Processing Year: {year} =-=-=-=-=-")
            
            # Navigate to year page
            safe_print(f"-> Loading data for {year}...")
            driver.get(url)
            
            # Wait for table to load
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
            time.sleep(2)  # Allow full page load
            
            # Parse the page
            soup = BeautifulSoup(driver.page_source, 'lxml')
            
            # Find the main data table
            all_tables = soup.find_all('table')
            monthly_data_table = None
            
            for table in all_tables:
                thead = table.find('thead')
                if thead and 'Jan.' in thead.text:
                    monthly_data_table = table
                    break

            if not monthly_data_table:
                safe_print(f"  -> [ERROR] Could not identify the main monthly data table for {year}.")
                continue
            
            safe_print(f"  -> [SUCCESS] Identified data table for {year}.")
            
            # Process table data
            table_body = monthly_data_table.find('tbody')
            data_rows = table_body.find_all('tr') if table_body else []
            
            if len(data_rows) < 2:
                safe_print(f"  -> [WARNING] Table for {year} has fewer than 2 rows. Cannot find Excel files.")
                continue

            # Get the Excel file row (usually second row)
            excel_row = data_rows[1]
            data_cells = excel_row.find_all('td')
            month_headers = [th.text.strip() for th in monthly_data_table.find('thead').find_all('th')]
            
            # Process each month
            new_files_this_year = 0
            for month_header, cell in zip(month_headers, data_cells):
                # Look for Excel file links
                link = cell.find('a', href=lambda h: h and (h.endswith('.xls') or h.endswith('.xlsx')))
                if not (link and file_filter in link['href']):
                    continue

                # Get filename and check if already downloaded
                file_name = urljoin(driver.current_url, link['href']).split('/')[-1]
                if file_name in master_log:
                    safe_print(f"  -> [SKIP] Already downloaded: {file_name}")
                    continue
                
                new_files_this_year += 1
                month_folder_name = MONTH_MAP.get(month_header, "Unknown_Month")
                
                # Create output directory structure
                final_output_path = base_dir / year / month_folder_name
                final_output_path.mkdir(parents=True, exist_ok=True)
                
                safe_print(f"  -> [NEW] Downloading '{file_name}' for {month_header}...")
                report_url = urljoin(driver.current_url, link['href'])
                file_path = final_output_path / file_name
                
                # Download the file
                if download_file(session, report_url, file_path):
                    safe_print(f"    -> [SUCCESS] Saved to: {year}/{month_folder_name}/{file_name}")
                    master_log.add(file_name)
                else:
                    safe_print(f"    -> [ERROR] Download failed for {file_name}")

            if new_files_this_year == 0:
                safe_print(f"  -> [INFO] No new files to download for {year} (all files are up-to-date).")
            else:
                safe_print(f"  -> [SUMMARY] Downloaded {new_files_this_year} new files for {year}")
    
    except Exception as e:
        safe_print(f"[ERROR] An unexpected error occurred while processing {category_name}. Error: {e}")
        import traceback
        safe_print(f"[DEBUG] Error details: {traceback.format_exc()}")
    finally:
        # Clean up driver
        if driver:
            try:
                safe_print("-> Closing browser...")
                driver.quit()
            except Exception as cleanup_error:
                safe_print(f"[WARNING] Error during cleanup: {cleanup_error}")
        safe_print(f"\n-=-=-=-=-= Finished processing {category_name}! =-=-=-=-=-")

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    # Configuration summary
    mode = "HEADLESS" if HEADLESS_MODE else "VISIBLE"
    safe_print(f"\n🚀 JPX Data Downloader Starting in {mode} Mode")
    safe_print("="*60)
    safe_print(f"Years to process: {YEARS_TO_PROCESS}")
    safe_print(f"Output directory: {BASE_OUTPUT_DIRECTORY}")
    safe_print(f"Headless mode: {HEADLESS_MODE}")
    safe_print("="*60)
    
    categories_to_scrape = {
        "ETFs": "etf_m",
        "REITs": "reit_m"
    }
    
    try:
        # Initialize
        BASE_OUTPUT_DIRECTORY.mkdir(exist_ok=True)
        master_download_log = load_download_log(LOG_FILE_PATH)
        initial_log_size = len(master_download_log)
        
        # Setup HTTP session
        http_session = requests.Session()
        http_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        # Process each category
        start_time = time.time()
        for category, file_id in categories_to_scrape.items():
            safe_print(f"\n⏱️ Starting {category} processing...")
            category_start = time.time()
            
            process_category(
                category_name=category, 
                file_filter=file_id, 
                session=http_session, 
                master_log=master_download_log, 
                base_dir=BASE_OUTPUT_DIRECTORY
            )
            
            category_time = time.time() - category_start
            safe_print(f"⏱️ {category} completed in {category_time:.1f} seconds")

        # Save updated log
        if len(master_download_log) > initial_log_size:
            save_download_log(LOG_FILE_PATH, master_download_log)
            new_downloads = len(master_download_log) - initial_log_size
            safe_print(f"\n✅ Downloaded {new_downloads} new files")
        else:
            safe_print("\n📋 No new files were downloaded (all files are up-to-date)")

        # Final summary
        total_time = time.time() - start_time
        safe_print("\n" + "="*60)
        safe_print("🎉 ALL CATEGORIES PROCESSED SUCCESSFULLY!")
        safe_print("="*60)
        safe_print(f"⏱️ Total processing time: {total_time:.1f} seconds")
        safe_print(f"📁 Output directory: {BASE_OUTPUT_DIRECTORY}")
        safe_print(f"📊 Total files tracked: {len(master_download_log)}")
        safe_print("="*60)
        
        # Exit with success code
        sys.exit(0)
        
    except KeyboardInterrupt:
        safe_print("\n[INTERRUPTED] Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        safe_print(f"\n[CRITICAL ERROR] {str(e)}")
        import traceback
        safe_print(f"[DEBUG] Error details: {traceback.format_exc()}")
        sys.exit(1)