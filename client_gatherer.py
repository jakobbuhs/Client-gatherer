from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import time
import pandas as pd
from typing import List, Dict, Any, Optional
import asyncio
import aiohttp
import ssl
import certifi
from bs4 import BeautifulSoup
import logging
from urllib.parse import urlparse
import re
from dotenv import load_dotenv
import os
load_dotenv()
class GoogleShopifyFinder:
    def __init__(self, api_key: str, cse_id: str, store_limit: int = 100, max_checks: int = 1000):
        self.api_key = api_key
        self.cse_id = cse_id
        self.store_limit = store_limit
        self.max_checks = max_checks
        self.service = build("customsearch", "v1", developerKey=api_key)
        self.found_stores = []
        self.stores_count = 0
        self.checked_count = 0
        self.setup_logging()
        
        # Create SSL context using system certificates
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        # Add email pattern
        self.email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    async def find_emails(self, soup: BeautifulSoup, html: str) -> List[str]:
        """Find email addresses in HTML content"""
        emails = set()
        
        # Look for mailto links
        for link in soup.find_all('a', href=re.compile(r'mailto:')):
            email = link['href'].replace('mailto:', '').split('?')[0].strip()
            if self.email_pattern.match(email):
                emails.add(email.lower())
        
        # Look for emails in text
        text_emails = self.email_pattern.findall(html)
        emails.update([email.lower() for email in text_emails])
        
        return list(emails)
        
    async def verify_store(self, url: str) -> Optional[Dict[str, Any]]:
        """Verify if a URL is a Norwegian Shopify store with SSL handling"""
        self.checked_count += 1
        self.logger.info(f"\nVerifying URL {self.checked_count}: {url}")
        
        # Configure SSL context for aiohttp
        conn = aiohttp.TCPConnector(ssl=self.ssl_context)
        
        async with aiohttp.ClientSession(connector=conn) as session:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                # Try both https and http if needed
                try:
                    async with session.get(url, headers=headers, timeout=15) as response:
                        if response.status != 200:
                            # If https fails with 403/401, try http
                            if response.status in [403, 401]:
                                http_url = url.replace('https://', 'http://')
                                async with session.get(http_url, headers=headers, timeout=15) as http_response:
                                    if http_response.status != 200:
                                        return None
                                    html = await http_response.text()
                            else:
                                return None
                        else:
                            html = await response.text()
                except aiohttp.ClientSSLError:
                    # If SSL fails, try http
                    http_url = url.replace('https://', 'http://')
                    async with session.get(http_url, headers=headers, timeout=15) as response:
                        if response.status != 200:
                            return None
                        html = await response.text()
                
                self.logger.info(f"Successfully fetched HTML from {url}")
                
                # Check for Shopify indicators
                shopify_patterns = [
                    r'cdn\.shopify\.com',
                    r'shopify\.com/checkout',
                    r'Shopify\.theme',
                    r'shopify-payment-button',
                    r'powered by shopify',
                    r'_shopify_'
                ]
                
                is_shopify = any(re.search(pattern, html, re.I) for pattern in shopify_patterns)
                if not is_shopify:
                    self.logger.info(f"No Shopify indicators found in {url}")
                    return None
                
                # Parse HTML
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract metadata
                title = soup.title.string.strip() if soup.title else "Unknown"
                description = soup.find('meta', {'name': 'description'})
                description = description['content'] if description else "No description"
                
                # Find emails
                emails = await self.find_emails(soup, html)
                
                store_info = {
                    'url': url,
                    'title': title,
                    'description': description,
                    'emails': emails,
                    'verified': True,
                    'discovery_date': time.strftime('%Y-%m-%d')
                }
                
                if emails:
                    self.logger.info(f"Found emails: {emails}")
                
                self.logger.info(f"Successfully verified Shopify store: {url}")
                return store_info
                
            except Exception as e:
                self.logger.error(f"Error verifying store {url}: {str(e)}")
                return None

    async def search_norwegian_shopify(self) -> List[Dict[str, Any]]:
        """Search for Norwegian Shopify stores using Google API"""
        search_queries = [
            'site:.no "Powered by Shopify"',
            'site:.no inurl:products shopify.com',
            'site:.no inurl:collections cdn.shopify.com',
            'site:.no "Shopify online store"',
            'site:.no nettbutikk shopify',
            'site:.no vipps shopify',
            'site:.no klarna shopify'
        ]
        
        seen_urls = set()
        all_results = []
        
        for query in search_queries:
            if self.stores_count >= self.store_limit:
                self.logger.info(f"Store limit of {self.store_limit} reached")
                break
            
            self.logger.info(f"\nSearching with query: {query}")
            
            try:
                start_index = 1
                while start_index < 100:  # Google's max results per query
                    if self.stores_count >= self.store_limit:
                        break
                        
                    try:
                        results = self.service.cse().list(
                            q=query,
                            cx=self.cse_id,
                            start=start_index
                        ).execute()
                        
                        if 'items' not in results:
                            self.logger.info(f"No more results for query: {query}")
                            break
                        
                        new_urls = 0
                        for item in results['items']:
                            url = item.get('link')
                            if url and url not in seen_urls:
                                seen_urls.add(url)
                                all_results.append(item)
                                new_urls += 1
                                
                        self.logger.info(f"Found {new_urls} new URLs from page {start_index // 10 + 1}")
                        
                        if len(results['items']) < 10:  # Less than max results per page
                            break
                            
                    except HttpError as e:
                        self.logger.error(f"Error in Google API search: {str(e)}")
                        break
                        
                    start_index += 10
                    await asyncio.sleep(1)  # Respect API rate limits
                    
            except Exception as e:
                self.logger.error(f"Unexpected error in search: {str(e)}")
                continue
        
        self.logger.info(f"\nTotal URLs to verify: {len(all_results)}")
        return await self.verify_stores(all_results)
    
    async def verify_stores(self, search_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Verify multiple stores concurrently"""
        if not search_results:
            self.logger.warning("No search results to verify")
            return []
            
        urls = [result['link'] for result in search_results]
        self.logger.info(f"\nStarting verification of {len(urls)} URLs")
        
        # Process in smaller chunks to avoid overwhelming resources
        chunk_size = 5
        verified_stores = []
        
        for i in range(0, len(urls), chunk_size):
            chunk = urls[i:i + chunk_size]
            tasks = [self.verify_store(url) for url in chunk]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out None results and exceptions
            chunk_stores = [
                store for store in results 
                if store is not None and not isinstance(store, Exception)
            ]
            verified_stores.extend(chunk_stores)
            
            self.logger.info(f"Verified {len(chunk_stores)} stores from current chunk. "
                           f"Total verified: {len(verified_stores)}")
            
            if len(verified_stores) >= self.store_limit:
                return verified_stores[:self.store_limit]
            
            # Small delay between chunks
            await asyncio.sleep(1)
        
        return verified_stores

    def export_results(self, filename: str = 'norwegian_shopify_stores.csv'):
        """Export found stores to CSV"""
        if self.found_stores:
            df = pd.DataFrame(self.found_stores)
            df.to_csv(filename, index=False)
            self.logger.info(f"Results exported to {filename}")
        else:
            self.logger.warning("No stores found to export")
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate a summary report"""
        report = {
            'total_stores_found': len(self.found_stores),
            'total_urls_checked': self.checked_count,
            'store_limit': self.store_limit,
            'max_checks_limit': self.max_checks,
            'store_limit_reached': len(self.found_stores) >= self.store_limit,
            'max_checks_reached': self.checked_count >= self.max_checks,
            'scan_date': time.strftime('%Y-%m-%d'),
            'stores': self.found_stores
        }
        
        report_filename = 'shopify_report.json'
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=4, ensure_ascii=False)
        
        self.logger.info(f"Generated report with {len(self.found_stores)} stores")
        return report

async def main():
    API_KEY = os.getenv("API_KEY") # Replace with your actual API key
    CSE_ID = os.getenv("CSE_ID") # Replace with your actual CSE ID
    
    print("\nStarting Shopify store finder with SSL handling...")
    
    try:
        finder = GoogleShopifyFinder(API_KEY, CSE_ID, store_limit=100, max_checks=1000)
        stores = await finder.search_norwegian_shopify()
        finder.found_stores = stores
        
        print(f"\nSearch completed:")
        print(f"- URLs checked: {finder.checked_count}")
        print(f"- Stores found: {len(stores)}")
        
        if stores:
            finder.export_results()
            report = finder.generate_report()
            print("\nStores found:")
            for store in stores:
                print(f"- {store['url']}: {store['title']}")
                if store.get('emails'):
                    print(f"  Emails: {', '.join(store['emails'])}")
        
    except Exception as e:
        print(f"\nFatal error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())