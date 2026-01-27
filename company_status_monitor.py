import os
import asyncio
import aiohttp
import json
import logging
from bs4 import BeautifulSoup
import re
from datetime import datetime

# --- Configuration ---
# Base URL for company list (pagination will be appended)
BASE_URL_TEMPLATE = "https://prisync.me/admin/company/admin/Company_page/{}/Company_sort/id.desc" 

# Local file to store state
STATE_FILE = 'company_state.json'

# Concurrency settings
CONCURRENT_REQUESTS = 10
START_PAGE = 1
MAX_PAGES_TO_SCRAPE = 200 

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FileStateManager:
    """Manages company state using a local JSON file."""
    def __init__(self, filename):
        self.filename = filename
        self.company_state = {} 

    def load_state(self):
        try:
            if os.path.exists(self.filename):
                with open(self.filename, 'r', encoding='utf-8') as f:
                    self.company_state = json.load(f)
                logger.info(f"✅ Loaded state for {len(self.company_state)} companies from {self.filename}.")
            else:
                logger.info(f"ℹ️ State file {self.filename} not found. Starting fresh.")
                self.company_state = {}
        except Exception as e:
            logger.error(f"Error loading state from {self.filename}: {e}")
            self.company_state = {}

    def save_state(self):
        try:
            with open(self.filename, 'w', encoding='utf-8') as f:
                json.dump(self.company_state, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Saved state to {self.filename}.")
        except Exception as e:
            logger.error(f"Error saving state to {self.filename}: {e}")

    def update_company(self, company_data):
        """Updates or adds a company to the state."""
        c_id = str(company_data['id'])
        self.company_state[c_id] = {
            'name': company_data['name'],
            'email': company_data['email'],
            'status': company_data['status'],
            'last_updated': str(datetime.now())
        }

class AsyncScraper:
    def __init__(self):
        raw_cookie = os.environ.get("PRISYNC_COOKIE", "")
        self.cookies = {}
        if raw_cookie:
            for item in raw_cookie.split(";"):
                if "=" in item:
                    k, v = item.strip().split("=", 1)
                    self.cookies[k.strip()] = v.strip()
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        }
        self.results = []
        self.lock = asyncio.Lock()
        self.stop_signal = False

    async def fetch_page(self, session, page_num):
        if self.stop_signal: return None
        url = BASE_URL_TEMPLATE.format(page_num)
        try:
            async with session.get(url, allow_redirects=False) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 302:
                    logger.warning(f"Page {page_num} redirected. COOKIE MIGHT BE EXPIRED!")
                    self.stop_signal = True # Stop other workers if cookie invalid
                elif response.status == 404:
                    logger.info(f"Page {page_num} not found. Reached end?")
                else:
                    logger.warning(f"Page {page_num} failed: {response.status}")
        except Exception as e:
            logger.error(f"Error fetching page {page_num}: {e}")
        return None

    def parse_html(self, html):
        data = []
        try:
            soup = BeautifulSoup(html, 'lxml')
            # Selector from user providing: #yw1 table.items tbody tr
            rows = soup.select("#yw1 table.items tbody tr")
            
            for row in rows:
                cols = row.find_all("td")
                # Based on user HTML:
                # 0: ID, 1: Name, 2: Email, 7: Company Status
                if len(cols) >= 8:
                    c_id = cols[0].get_text(strip=True)
                    name = cols[1].get_text(strip=True)
                    email = cols[2].get_text(strip=True)
                    status = cols[7].get_text(strip=True)
                    
                    if c_id and c_id.isdigit():
                        data.append({
                            "id": c_id,
                            "name": name,
                            "email": email,
                            "status": status
                        })
        except Exception:
            pass
        return data

    async def worker(self, queue, session):
        while True:
            try:
                page_num = await queue.get()
            except asyncio.QueueEmpty:
                break
                
            if self.stop_signal:
                queue.task_done()
                continue
                
            html = await self.fetch_page(session, page_num)
            if html:
                page_data = self.parse_html(html)
                if page_data:
                    async with self.lock:
                        self.results.extend(page_data)
            
            queue.task_done()

    async def run(self):
        queue = asyncio.Queue()
        for i in range(START_PAGE, MAX_PAGES_TO_SCRAPE + 1):
            queue.put_nowait(i)
        
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(cookies=self.cookies, headers=self.headers, connector=connector) as session:
            tasks = []
            for i in range(CONCURRENT_REQUESTS):
                task = asyncio.create_task(self.worker(queue, session))
                tasks.append(task)
            await queue.join()
            for task in tasks: task.cancel()
        
        return self.results

class SlackNotifier:
    def __init__(self):
        self.webhook_url = os.environ.get("SLACK_WEBHOOK")

    async def send_notification(self, changes):
        """
        changes: list of dicts {'type': 'NEW'|'CHANGE', 'data': ...}
        """
        if not self.webhook_url or not changes: return
        
        try:
            # Chunk messages to avoid Slack limit
            chunk_size = 10
            for i in range(0, len(changes), chunk_size):
                batch = changes[i:i+chunk_size]
                
                message_text = "📢 *Prisync Company Updates*\n\n"
                for item in batch:
                    if item['type'] == 'CHANGE':
                        c = item['data']
                        new_status = c['new'].lower() # Küçük harfe çevirip kontrol edelim
                        
                        # --- Emoji Mantığı ---
                        if 'paid' in new_status:
                            status_icon = "🎉 :partying_face:" # Kutlama
                        elif 'churned' in new_status or 'uninstalled' in new_status:
                            status_icon = "📉 :cry:" # Üzgün/Kayup
                        else:
                            status_icon = "🔄" # Standart değişim
                        # ---------------------

                        message_text += f"{status_icon} *Status Change*: {c['name']} (ID: {c['id']})\n"
                        message_text += f"   ❌ Old: {c['old']}  ➡  ✅ New: *{c['new']}*\n\n"

                    elif item['type'] == 'NEW':
                        c = item['data']
                        # İstersen yeni gelen 'Paid' müşteriler için de buraya benzer mantık ekleyebilirsin
                        message_text += f"✨ *New Company*: {c['name']} (ID: {c['id']})\n"
                        message_text += f"   Status: {c['status']}\n\n"
                
                connector = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=connector) as session:
                    await session.post(self.webhook_url, json={"text": message_text})
                    logger.info("Slack notification batch sent.")
                    await asyncio.sleep(1) # Rate limit niceness

        except Exception as e:
            logger.error(f"Slack error: {e}")

async def main():
    state_manager = FileStateManager(STATE_FILE)
    state_manager.load_state()

    logger.info("Starting scrape cycle...")
    scraper = AsyncScraper()
    scraped_data = await scraper.run()
    logger.info(f"Scrape finished. Found {len(scraped_data)} companies.")
    
    # Process Changes
    changes_to_notify = []
    
    # sort scraped data by ID
    scraped_data.sort(key=lambda x: int(x['id']))

    for company in scraped_data:
        c_id = str(company['id'])
        c_name = company['name']
        c_status = company['status']
        
        # Check against local state
        if c_id in state_manager.company_state:
            # Existing company
            old_status = state_manager.company_state[c_id]['status']
            if old_status != c_status:
                logger.info(f"Status Change Detected for {c_id}: {old_status} -> {c_status}")
                changes_to_notify.append({
                    'type': 'CHANGE',
                    'data': {
                        'id': c_id, 'name': c_name, 
                        'old': old_status, 'new': c_status
                    }
                })
                # Update state immediately (in memory)
                state_manager.update_company(company)
        else:
            # New company
            # Update state
            state_manager.update_company(company)
            # Notify
            changes_to_notify.append({
                'type': 'NEW',
                'data': company
            })

    # Save state to file
    state_manager.save_state()

    if changes_to_notify:
        slack = SlackNotifier()
        await slack.send_notification(changes_to_notify)
    else:
        logger.info("Checking complete. No changes detected.")

if __name__ == "__main__":
    asyncio.run(main())
