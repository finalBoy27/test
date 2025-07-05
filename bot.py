import os
import requests
from bs4 import BeautifulSoup
import re
import logging
from urllib.parse import urljoin
from flask import Flask, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import telebot
from dotenv import load_dotenv
from datetime import datetime, timedelta
import traceback
import time
import cloudscraper
from telebot.apihelper import ApiTelegramException
import fcntl
import sys

load_dotenv()

# Initialize logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
MAX_RETRIES = 3
MAX_WORKERS = 3
MAX_PAGES_PER_SEARCH = 10
BASE_URL = "https://desifakes.com"
MAX_FILE_SIZE_MB = 10  # Max file size for upload (in MB)
POLLING_TIMEOUT = 30  # Timeout for polling
POLLING_INTERVAL = 1  # Interval between polls
CONFLICT_RETRY_DELAY = 10  # Increased delay for 409 conflict retries
GROUP_CHAT_ID = os.getenv('GROUP_CHAT_ID')  # Group chat ID from .env
MAX_IMAGE_RETRIES = 3  # Max retries for downloading images
BATCH_SIZE = 5  # Number of images per batch
LOCK_FILE = "/tmp/bot.lock"  # File-based lock to prevent multiple instances

# Global task tracking
active_tasks = {}

class ScraperError(Exception):
    pass

def acquire_lock():
    """Acquire a file-based lock to ensure single instance."""
    lock_file = open(LOCK_FILE, 'w')
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        logger.info("Lock acquired successfully")
        return lock_file
    except IOError:
        logger.error("Another instance is already running. Exiting.")
        sys.exit(1)

def make_request(url, method='get', **kwargs):
    """Make a request using cloudscraper to bypass Cloudflare."""
    scraper = cloudscraper.create_scraper()
    for attempt in range(MAX_RETRIES):
        try:
            response = scraper.request(
                method,
                url,
                timeout=8 + (attempt * 5),
                **kwargs
            )
            response.raise_for_status()
            logger.info(f"Success for {url} on attempt {attempt + 1}")
            return response
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            if '503 Service Unavailable' in str(e):
                logger.warning(f"503 detected, waiting 10 seconds before retry")
                time.sleep(10)
            if attempt == MAX_RETRIES - 1:
                raise ScraperError(f"Failed for {url} after {MAX_RETRIES} attempts: {str(e)}")
            time.sleep(1 * (attempt + 1))

def download_image(image_url):
    """Download an image with retries, returning None if it fails."""
    for attempt in range(MAX_IMAGE_RETRIES):
        try:
            response = make_request(image_url)
            if response.status_code == 200:
                return BytesIO(response.content)
            else:
                logger.warning(f"Failed to download {image_url} on attempt {attempt + 1}: Status {response.status_code}")
        except ScraperError as e:
            logger.warning(f"Download attempt {attempt + 1} failed for {image_url}: {str(e)}")
        except Exception as e:
            logger.warning(f"Unexpected error downloading {image_url} on attempt {attempt + 1}: {str(e)}")
        time.sleep(1 * (attempt + 1))
    logger.error(f"Failed to download {image_url} after {MAX_IMAGE_RETRIES} attempts")
    return None

def upload_file(file_buffer, filename):
    """Upload file to hosting services and return the URL."""
    file_buffer.seek(0, os.SEEK_END)
    file_size_mb = file_buffer.tell() / (1024 * 1024)
    file_buffer.seek(0)

    if file_size_mb > MAX_FILE_SIZE_MB:
        raise ScraperError(f"File {filename} is {file_size_mb:.2f} MB, exceeds {MAX_FILE_SIZE_MB} MB limit")

    try:
        files = {'fileToUpload': (filename, file_buffer, 'text/html')}
        data = {'reqtype': 'fileupload'}
        response = requests.post("https://catbox.moe/user/api.php", files=files, data=data)
        if response.status_code == 200 and response.text.startswith("https://"):
            logger.info(f"Uploaded to Catbox: {response.text.strip()}")
            return response.text.strip()
    except Exception as e:
        logger.error(f"Catbox upload failed: {str(e)}")

    raise ScraperError("All upload attempts failed")

def generate_links(start_year, end_year, username, title_only=False):
    """Generate search URLs for a username and date range efficiently."""
    if not username or not isinstance(username, str):
        raise ValueError("Invalid username")

    now = datetime.now()
    current_year = now.year
    start_year = max(2010, start_year)
    end_year = min(end_year, current_year)

    encoded_username = username.replace(' ', '+')
    search_id = "40169483"
    base_url = f"{BASE_URL}/search/{search_id}/"
    title_flag = 1 if title_only else 0

    months = [
        ("11-28", "01-03"), ("10-29", "12-03"),
        ("09-28", "11-03"), ("08-29", "10-03"),
        ("07-29", "09-03"), ("06-28", "08-03"),
        ("05-29", "07-03"), ("04-28", "06-03"),
        ("03-29", "05-03"), ("02-26", "04-03"),
        ("01-29", "03-03"), ("12-29", "02-03")
    ]

    links = []
    for year in range(end_year, start_year - 1, -1):
        for start_month, end_month in months:
            start_date = f"{year}-{start_month}"
            end_year_adjusted = year + 1 if start_month > end_month else year
            end_date = f"{end_year_adjusted}-{end_month}"

            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                continue

            if start_dt > now:
                continue

            if end_dt > now:
                end_date = now.strftime("%Y-%m-%d")

            url = (
                f"{base_url}?q={encoded_username}"
                f"&c[newer_than]={start_date}"
                f"&c[older_than]={end_date}"
                f"&c[title_only]={title_flag}&o=date"
            )

            links.append((year, url, start_date, end_date))

    logger.info(f"Generated {len(links)} search URLs for {username}")
    return links

def split_url(url, start_date, end_date, max_pages=MAX_PAGES_PER_SEARCH):
    """Split search URL into smaller date ranges if too many pages."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        response = make_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        pagination = soup.find('div', class_='pageNav')
        total_pages = max(
            [int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()]
        ) if pagination else 1

        if total_pages < max_pages:
            return [(url, start_date, end_date)]

        total_days = (end_dt - start_dt).days
        mid_dt = start_dt + timedelta(days=total_days // 2)

        first_end = (mid_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        second_start = (mid_dt + timedelta(days=1)).strftime("%Y-%m-%d")

        first_url = re.sub(r"c\[newer_than\]=[^&]+", f"c[newer_than]={start_date}", url)
        first_url = re.sub(r"c\[older_than\]=[^&]+", f"c[older_than]={first_end}", first_url)

        second_url = re.sub(r"c\[newer_than\]=[^&]+", f"c[newer_than]={second_start}", url)
        second_url = re.sub(r"c\[older_than\]=[^&]+", f"c[older_than]={end_date}", second_url)

        return split_url(first_url, start_date, first_end) + \
               split_url(second_url, second_start, end_date)

    except Exception as e:
        logger.error(f"Split error for {url}: {str(e)}")
        return [(url, start_date, end_date)]

def fetch_page_data(url, page=None):
    """Fetch post links from a search page."""
    try:
        full_url = f"{url}&page={page}" if page else url
        response = make_request(full_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = list(dict.fromkeys(
            urljoin(BASE_URL, link['href']) for link in soup.find_all('a', href=True)
            if 'threads/' in link['href'] and not link['href'].startswith('#') and 'page-' not in link['href']
        ))
        pagination = soup.find('div', class_='pageNav')
        total_pages = max(
            [int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()]
        ) if pagination else 1
        logger.debug(f"Fetched {len(links)} post links from {full_url}, total pages: {total_pages}")
        return links, total_pages
    except Exception as e:
        logger.error(f"Failed to fetch page data for {full_url}: {str(e)}")
        return [], 1

def extract_post_date(article):
    """Extract the actual post date from the article."""
    try:
        time_tag = article.find('time', class_='u-dt')
        if time_tag and 'datetime' in time_tag.attrs:
            post_date = datetime.strptime(time_tag['datetime'], "%Y-%m-%dT%H:%M:%S%z")
            return post_date.strftime("%Y-%m-%d")
        return None
    except Exception as e:
        logger.debug(f"Failed to extract post date: {str(e)}")
        return None

def process_post(post_link, username, start_year, end_year, media_by_date, global_seen):
    """Process a post to extract media with exact date."""
    try:
        response = make_request(post_link)
        soup = BeautifulSoup(response.text, 'html.parser')

        year_match = re.search(r'c\[newer_than\]=(\d{4})-', post_link)
        year = int(year_match.group(1)) if year_match else end_year

        title_div = soup.find('div', class_='p-title')
        page_title = title_div.find('h1', class_='p-title-value').get_text(strip=True).lower() if title_div else ''
        username_lower = username.lower()
        username_parts = username_lower.split()
        title_matches = False
        if page_title:
            for part in username_parts:
                if page_title.startswith(part) or page_title.endswith(part):
                    title_matches = True
                    break
                if part in page_title.replace('-', ' '):
                    title_matches = True
                    break

        post_id = re.search(r'post-(\d+)', post_link)
        articles = [soup.find('article', {'data-content': f'post-{post_id.group(1)}', 'id': f'js-post-{post_id.group(1)}'})] if post_id else soup.find_all('article')
        if not articles:
            logger.warning(f"No articles found in {post_link}")
            return

        if title_matches:
            logger.info(f"Page title '{page_title}' matches username '{username}', processing all media in {post_link}")
            filtered_articles = articles
        else:
            filtered_articles = [
                a for a in articles if a and (
                    username_lower in a.get_text(separator=" ").lower() or
                    username_lower in a.get('data-author', '').lower()
                )
            ]
            if not filtered_articles:
                logger.info(f"No articles matched username '{username}' in {post_link}")
                return

        for article in filtered_articles:
            post_date = extract_post_date(article) or f"{year}-01-01"
            try:
                post_year = int(post_date.split('-')[0])
                if post_year < start_year or post_year > end_year:
                    logger.debug(f"Skipping post outside year range: {post_date}")
                    continue
            except ValueError:
                post_year = year

            for img in article.find_all('img', src=True):
                src = urljoin(BASE_URL, img['src']) if img['src'].startswith("/") else img['src']
                media_type = "gifs" if src.lower().endswith(".gif") else "images"
                if (src.startswith("data:image") or "addonflare/awardsystem/icons/" in src or
                    any(keyword in src.lower() for keyword in ["avatars", "ozzmodz_badges_badge", "premium", "likes"])):
                    logger.debug(f"Skipping unwanted media in {post_link}: {src}")
                    continue
                if src not in global_seen[media_type]:
                    global_seen[media_type].add(src)
                    if post_date not in media_by_date[media_type]:
                        media_by_date[media_type][post_date] = []
                    media_by_date[media_type][post_date].append(src)
                    logger.debug(f"Added {media_type} from {post_link}: {src}")
                else:
                    logger.debug(f"Duplicate {media_type} skipped in {post_link}: {src}")

            for video in article.find_all('video', src=True):
                src = urljoin(BASE_URL, video['src']) if video['src'].startswith("/") else video['src']
                media_type = "videos"
                if (src.startswith("data:") or "addonflare/awardsystem/icons/" in src or
                    any(keyword in src.lower() for keyword in ["avatars", "ozzmodz_badges_badge", "premium", "likes"])):
                    logger.debug(f"Skipping unwanted media in {post_link}: {src}")
                    continue
                if src not in global_seen[media_type]:
                    global_seen[media_type].add(src)
                    if post_date not in media_by_date[media_type]:
                        media_by_date[media_type][post_date] = []
                    media_by_date[media_type][post_date].append(src)
                    logger.debug(f"Added {media_type} from {post_link}: {src}")
                else:
                    logger.debug(f"Duplicate {media_type} skipped in {post_link}: {src}")

            for source in article.find_all('source', src=True):
                src = urljoin(BASE_URL, source['src']) if source['src'].startswith("/") else source['src']
                media_type = "videos"
                if (src.startswith("data:") or "addonflare/awardsystem/icons/" in src or
                    any(keyword in src.lower() for keyword in ["avatars", "ozzmodz_badges_badge", "premium", "likes"])):
                    logger.debug(f"Skipping unwanted media in {post_link}: {src}")
                    continue
                if src not in global_seen[media_type]:
                    global_seen[media_type].add(src)
                    if post_date not in media_by_date[media_type]:
                        media_by_date[media_type][post_date] = []
                    media_by_date[media_type][post_date].append(src)
                    logger.debug(f"Added {media_type} from {post_link}: {src}")
                else:
                    logger.debug(f"Duplicate {media_type} skipped in {post_link}: {src}")

    except Exception as e:
        logger.error(f"Failed to process post {post_link}: {str(e)}")

def create_html(media_by_date_per_username, usernames, start_year, end_year):
    """Generate HTML with lazy loading for all media (images, GIFs, videos) in chronological order."""
    usernames_str = ", ".join(usernames)
    title = f"{usernames_str} - Media Gallery"
    logger.debug(f"Generating HTML for usernames: {usernames_str}")

    html_fragments = [f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{title}</title>
  <style>
    body {{ background-color: #000; font-family: Arial, sans-serif; margin: 0; padding: 20px; color: white; }}
    h1 {{ text-align: center; margin-bottom: 20px; }}
    .button-container {{ text-align: center; margin-bottom: 20px; display: flex; flex-wrap: wrap; justify-content: center; gap: 10px; }}
    .filter-button {{
      padding: 10px 20px;
      margin: 5px;
      font-size: 16px;
      border-radius: 5px;
      border: none;
      background-color: #333;
      color: white;
      cursor: pointer;
      transition: background-color 0.3s;
    }}
    .filter-button:hover {{ background-color: #555; }}
    .filter-button.active {{ background-color: #007bff; }}
    .number-input {{
      padding: 8px;
      font-size: 16px;
      width: 60px;
      border-radius: 5px;
      border: none;
      background-color: #333;
      color: white;
    }}
    .media-type-select {{
      padding: 8px;
      font-size: 16px;
      border-radius: 5px;
      border: none;
      background-color: #333;
      color: white;
    }}
    .masonry {{ display: flex; justify-content: center; gap: 15px; min-height: 100px; }}
    .column {{ flex: 1; display: flex; flex-direction: column; gap: 15px; }}
    .column img, .column video {{ width: 100%; border-radius: 5px; display: block; }}
    img[loading="lazy"], video[loading="lazy"] {{ opacity: 0; transition: opacity 0.3s; }}
    img[loading="lazy"].loaded, video[loading="lazy"].loaded {{ opacity: 1; }}
    @media (max-width: 768px) {{
      .masonry {{ flex-direction: column; }}
      .filter-button {{ padding: 8px 15px; font-size: 14px; }}
      .number-input, .media-type-select {{ width: 100px; font-size: 14px; }}
    }}
  </style>
</head>
<body>
  <h1>{usernames_str} - Media Gallery ({start_year}-{end_year})</h1>
  <div class="button-container">
    <select id="mediaType" class="media-type-select">
      <option value="all" selected>All</option>
      <option value="images">Images</option>
      <option value="videos">Videos</option>
      <option value="gifs">Gifs</option>
    </select>
    <div id="itemsPerUserContainer">
      <input type="number" id="itemsPerUser" class="number-input" min="1" value="2" placeholder="Items per user">
    </div>
    <button class="filter-button active" data-usernames="" data-original-text="All">All</button>
    {"".join(f'<button class="filter-button" data-usernames="{username.replace(" ", "_")}" data-original-text="{username}">{username}</button>' for username in usernames)}
  </div>
  <div class="masonry" id="masonry"></div>
  <script>
    const mediaData = {{
"""]

    total_items = 0
    media_counts = {}
    all_media = []
    for username in usernames:
        media_by_date = media_by_date_per_username.get(username, {'images': {}, 'videos': {}, 'gifs': {}})
        count = sum(
            len(media_by_date[media_type][date])
            for media_type in media_by_date
            for date in media_by_date[media_type]
        )
        media_counts[username] = count
        total_items += count

        # Collect all media for this username
        for media_type in ['images', 'videos', 'gifs']:
            for date in sorted(media_by_date[media_type].keys(), reverse=True):
                for item in media_by_date[media_type][date]:
                    if not item.startswith(('http://', 'https://')):
                        logger.warning(f"Skipping invalid URL for {username}: {item}")
                        continue
                    safe_src = item.replace('"', '\\"').replace('\n', '')
                    all_media.append({'username': username, 'type': media_type, 'src': safe_src, 'date': date})

    if total_items == 0:
        logger.warning(f"No media items found for {usernames_str}")
        return None

    # Sort all media by date (newest to oldest)
    all_media = sorted(all_media, key=lambda x: x['date'], reverse=True)

    # Group media by username for JavaScript
    for username in usernames:
        user_media = [item for item in all_media if item['username'] == username]
        html_fragments.append(f"      {username.replace(' ', '_')}: [\n")
        for item in user_media:
            html_fragments.append(f'        {{type: "{item["type"]}", src: "{item["src"]}", date: "{item["date"]}"}},\n')
        html_fragments.append("      ],\n")
        logger.info(f"Username {username}: {media_counts.get(username, 0)} media items")
    html_fragments.append("    };\n")

    html_fragments.append(f"""    const usernames = {str(list(map(lambda x: x.replace(' ', '_'), usernames)))};
    const masonry = document.getElementById("masonry");
    const buttons = document.querySelectorAll('.filter-button');
    const mediaTypeSelect = document.getElementById('mediaType');
    const itemsPerUserInput = document.getElementById('itemsPerUser');
    let selectedUsername = '';

    function updateButtonLabels() {{
      buttons.forEach(button => {{
        const originalText = button.getAttribute('data-original-text');
        button.textContent = originalText;
      }});
    }}

    function getOrderedMedia(mediaType, itemsPerUser) {{
      let allMedia = [];
      try {{
        if (selectedUsername === '') {{
          let maxRounds = 0;
          const mediaByUser = {{}};
          usernames.forEach(username => {{
            let userMedia = mediaData[username] || [];
            if (mediaType !== 'all') {{
              userMedia = userMedia.filter(item => item.type === mediaType);
            }}
            userMedia = userMedia.sort((a, b) => new Date(b.date) - new Date(a.date));
            mediaByUser[username] = userMedia;
            maxRounds = Math.max(maxRounds, Math.ceil(userMedia.length / itemsPerUser));
          }});
          for (let round = 0; round < maxRounds; round++) {{
            usernames.forEach(username => {{
              const start = round * itemsPerUser;
              const end = start + itemsPerUser;
              allMedia = allMedia.concat(mediaByUser[username].slice(start, end));
            }});
          }}
          allMedia = allMedia.filter(item => item);
        }} else {{
          let userMedia = mediaData[selectedUsername] || [];
          if (mediaType !== 'all') {{
            userMedia = userMedia.filter(item => item.type === mediaType);
          }}
          allMedia = userMedia.sort((a, b) => new Date(b.date) - new Date(a.date));
        }}
      }} catch (e) {{
        console.error('Error in getOrderedMedia:', e);
        return [];
      }}
      return allMedia;
    }}

    function renderMedia() {{
      try {{
        masonry.innerHTML = '';
        const mediaType = mediaTypeSelect.value;
        const itemsPerUser = parseInt(itemsPerUserInput.value) || 2;
        const columnsCount = 3;
        const columns = [];

        for (let i = 0; i < columnsCount; i++) {{
          const col = document.createElement("div");
          col.className = "column";
          masonry.appendChild(col);
          columns.push(col);
        }}

        const allMedia = getOrderedMedia(mediaType, itemsPerUser);
        const totalRows = Math.ceil(allMedia.length / columnsCount);

        for (let row = 0; row < totalRows; row++) {{
          for (let col = 0; col < columnsCount; col++) {{
            const actualCol = row % 2 === 0 ? col : columnsCount - 1 - col;
            const index = row * columnsCount + col;
            if (index < allMedia.length) {{
              let element;
              if (allMedia[index].type === "videos") {{
                element = document.createElement("video");
                element.setAttribute("data-src", allMedia[index].src);
                element.controls = true;
                element.alt = "Video";
                element.loading = "lazy";
                element.classList.add("lazy");
                element.onerror = () => console.error('Failed to load video:', allMedia[index].src);
              }} else {{
                element = document.createElement("img");
                element.setAttribute("data-src", allMedia[index].src);
                element.alt = allMedia[index].type.charAt(0).toUpperCase() + allMedia[index].type.slice(1);
                element.loading = "lazy";
                element.classList.add("lazy");
                element.onerror = () => console.error('Failed to load image:', allMedia[index].src);
              }}
              columns[actualCol].appendChild(element);
            }}
          }}
        }}

        const lazyElements = document.querySelectorAll('.lazy');
        const observer = new IntersectionObserver((entries, observer) => {{
          entries.forEach(entry => {{
            if (entry.isIntersecting) {{
              const element = entry.target;
              element.src = element.getAttribute('data-src');
              element.classList.add('loaded');
              observer.unobserve(element);
            }}
          }});
        }}, {{ rootMargin: '0px 0px 200px 0px' }});

        lazyElements.forEach(element => observer.observe(element));
      }} catch (e) {{
        console.error('Error in renderMedia:', e);
        masonry.innerHTML = '<p style="color: red;">Error loading media. Please check console for details.</p>';
      }}
    }}

    buttons.forEach(button => {{
      button.addEventListener('click', () => {{
        try {{
          const username = button.getAttribute('data-usernames');
          if (button.classList.contains('active')) {{
            return;
          }}
          buttons.forEach(btn => btn.classList.remove('active'));
          button.classList.add('active');
          selectedUsername = username;
          updateButtonLabels();
          renderMedia();
        }} catch (e) {{
          console.error('Error in button click handler:', e);
        }}
      }});
    }});

    mediaTypeSelect.addEventListener('change', () => {{
      try {{
        renderMedia();
      }} catch (e) {{
        console.error('Error in mediaTypeSelect change handler:', e);
      }}
    }});

    itemsPerUserInput.addEventListener('input', () => {{
      try {{
        renderMedia();
      }} catch (e) {{
        console.error('Error in itemsPerUserInput input handler:', e);
      }}
    }});

    try {{
      updateButtonLabels();
      renderMedia();
    }} catch (e) {{
      console.error('Initial render failed:', e);
      masonry.innerHTML = '<p style="color: red;">Error loading media. Please check console for details.</p>';
    }}
  </script>
</body>
</html>""")

    html_content = "".join(html_fragments)
    logger.debug(f"Generated HTML with {total_items} items")
    return html_content

def send_telegram_message(chat_id, text, **kwargs):
    """Send a Telegram message with retries for rate limits."""
    for attempt in range(MAX_RETRIES):
        try:
            return bot.send_message(chat_id=chat_id, text=text, **kwargs)
        except ApiTelegramException as e:
            if e.error_code == 429:  # Too Many Requests
                retry_after = int(e.result_json.get('parameters', {}).get('retry_after', 5))
                logger.warning(f"429 Too Many Requests, waiting {retry_after} seconds")
                time.sleep(retry_after)
            elif e.error_code == 409:
                logger.warning(f"409 Conflict in send_message, attempt {attempt + 1}: {str(e)}")
                time.sleep(CONFLICT_RETRY_DELAY)
            else:
                logger.warning(f"Send message attempt {attempt + 1} failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise ScraperError(f"Failed to send message: {str(e)}")
            time.sleep(1 * (attempt + 1))
        except Exception as e:
            logger.warning(f"Send message attempt {attempt + 1} failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise ScraperError(f"Failed to send message: {str(e)}")
            time.sleep(1 * (attempt + 1))

def send_image_batch(chat_id, images):
    """Send a batch of up to 5 images to the Telegram group with a single username caption."""
    media_group = []

    first_username = None

    for i, (image_url, username, _) in enumerate(images):
        image_data = download_image(image_url)
        if image_data:
            media = telebot.types.InputMediaPhoto(media=image_data)

            # Set the first username as caption (only once)
            if first_username is None:
                first_username = username

            media_group.append(media)
        else:
            logger.warning(f"Skipping image {image_url} due to download failure")

    if media_group:
        # Set caption ONLY on first image
        media_group[0].caption = first_username
        media_group[0].parse_mode = 'Markdown'

        for attempt in range(MAX_RETRIES):
            try:
                bot.send_media_group(chat_id=GROUP_CHAT_ID, media=media_group)
                logger.info(f"Sent batch of {len(media_group)} images to group {GROUP_CHAT_ID}")
                return True
            except ApiTelegramException as e:
                if e.error_code == 429:
                    retry_after = int(e.result_json.get('parameters', {}).get('retry_after', 5))
                    logger.warning(f"429 Too Many Requests, waiting {retry_after} seconds")
                    time.sleep(retry_after)
                else:
                    logger.warning(f"Failed to send media group, attempt {attempt + 1}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Failed to send media group after {MAX_RETRIES} attempts")
                    return False
                time.sleep(1 * (attempt + 1))
            except Exception as e:
                logger.warning(f"Unexpected error sending media group, attempt {attempt + 1}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Failed to send media group after {MAX_RETRIES} attempts")
                    return False
                time.sleep(1 * (attempt + 1))
    return False

def process_and_send_images(chat_id, media_by_date_per_username, usernames, start_year, end_year):
    """Process and send images/GIFs in batches with username caption."""
    sent_images = 0
    all_media = []

    # Collect all media with username and date
    for username in usernames:
        media_by_date = media_by_date_per_username.get(username, {'images': {}, 'gifs': {}})
        all_dates = set(media_by_date['images'].keys()) | set(media_by_date['gifs'].keys())

        for date in sorted(all_dates, reverse=True):
            if not (str(start_year) <= date[:4] <= str(end_year)):
                continue

            images = media_by_date['images'].get(date, [])
            gifs = media_by_date['gifs'].get(date, [])
            for url in images + gifs:
                all_media.append((url, username, date))

    # Sort media by date (newest to oldest)
    all_media = sorted(all_media, key=lambda x: x[2], reverse=True)

    # Process in batches
    for i in range(0, len(all_media), BATCH_SIZE):
        batch = all_media[i:i + BATCH_SIZE]
        if not batch:
            continue

        if send_image_batch(chat_id, batch):
            sent_images += len(batch)
        else:
            logger.error(f"Failed to send batch {i // BATCH_SIZE + 1} for {usernames}")

    return sent_images

def cancel_task(chat_id):
    """Cancel an active scraping task."""
    if chat_id in active_tasks:
        executor, futures = active_tasks[chat_id]
        for future in futures:
            future.cancel()
        executor._threads.clear()
        executor.shutdown(wait=False)
        del active_tasks[chat_id]
        return True
    return False

def handle_message(message):
    """Handle incoming Telegram messages."""
    try:
        chat_id = message.chat.id
        message_id = message.message_id
        text = message.text.strip()

        if not text:
            send_telegram_message(chat_id, "Please send a search query", reply_to_message_id=message_id)
            return

        if text.lower() == '/stop':
            if cancel_task(chat_id):
                send_telegram_message(chat_id, "✅ Scraping stopped", reply_to_message_id=message_id)
            else:
                send_telegram_message(chat_id, "ℹ️ No active scraping to stop.", reply_to_message_id=message_id)
            return

        parts = text.split()
        if len(parts) < 1 or (parts[0] == '/start' and len(parts) < 2):
            send_telegram_message(
                chat_id,
                "Usage: username1,username2,... [title_only y/n] [start_year] [end_year]\n"
                "Example: 'Akshra Singh,Kareena Kapoor' n 2019 2025",
                reply_to_message_id=message_id
            )
            return

        if chat_id in active_tasks:
            send_telegram_message(chat_id, "⚠️ Scraping already running. Use /stop to cancel.", reply_to_message_id=message_id)
            return

        if parts[0] == '/start':
            usernames_str = ' '.join(parts[1:-3]) if len(parts) > 4 else parts[1]
            title_only_idx = 2 if len(parts) <= 4 else len(parts) - 3
        else:
            usernames_str = ' '.join(parts[:-3]) if len(parts) > 3 else parts[0]
            title_only_idx = 1 if len(parts) <= 3 else len(parts) - 3

        usernames = [username.strip() for username in usernames_str.split(',')]
        title_only = parts[title_only_idx].lower() == 'y' if len(parts) > title_only_idx else False
        start_year = int(parts[-2]) if len(parts) > 2 else 2019
        end_year = int(parts[-1]) if len(parts) > 1 else datetime.now().year

        usernames_display = ", ".join(usernames)
        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        active_tasks[chat_id] = (executor, [])

        try:
            media_by_date_per_username = {
                username: {'images': {}, 'videos': {}, 'gifs': {}}
                for username in usernames
            }
            global_seen = {'images': set(), 'videos': set(), 'gifs': set()}

            for username_idx, username in enumerate(usernames):
                all_post_links = []
                seen_links = set()
                search_links = generate_links(start_year, end_year, username, title_only)

                if not search_links:
                    logger.warning(f"No search URLs generated for {username}")
                    continue

                for year, search_link, start_date, end_date in search_links:
                    if chat_id not in active_tasks:
                        raise ScraperError("Task cancelled by user")

                    urls_to_process = split_url(search_link, start_date, end_date)
                    for url, s_date, e_date in urls_to_process:
                        total_pages = fetch_page_data(url)[1]
                        page_futures = []
                        for page in range(1, total_pages + 1):
                            page_futures.append(
                                executor.submit(fetch_page_data, url, page=page)
                            )
                        active_tasks[chat_id] = (executor, page_futures)
                        for future in as_completed(page_futures):
                            if chat_id not in active_tasks:
                                raise ScraperError("Task cancelled by user")
                            try:
                                links, _ = future.result()
                                all_post_links.extend(link for link in links if link not in seen_links)
                                seen_links.update(links)
                            except ScraperError as e:
                                logger.error(f"Page fetch failed for {username}: {str(e)}")
                                continue

                if not all_post_links:
                    logger.warning(f"No posts found for {username}")
                    continue

                logger.info(f"Processing {len(all_post_links)} unique post links for {username}")
                post_futures = []
                for link in all_post_links:
                    post_futures.append(
                        executor.submit(
                            process_post, link, username, start_year, end_year,
                            media_by_date_per_username[username], global_seen
                        )
                    )
                active_tasks[chat_id] = (executor, post_futures)

                for future in as_completed(post_futures):
                    if chat_id not in active_tasks:
                        raise ScraperError("Task cancelled by user")
                    try:
                        future.result()
                    except ScraperError as e:
                        logger.error(f"Post processing failed for {username}: {str(e)}")
                        continue
                    except Exception as e:
                        logger.error(f"Unexpected post processing error for {username}: {str(e)}")
                        continue

            # Process and send images in batches
            total_images = sum(
                sum(len(media_by_date_per_username[username]['images'].get(date, [])) +
                    len(media_by_date_per_username[username]['gifs'].get(date, []))
                    for date in set(media_by_date_per_username[username]['images'].keys()) | 
                               set(media_by_date_per_username[username]['gifs'].keys()))
                for username in usernames
            )
            if total_images > 0:
                sent_images = process_and_send_images(chat_id, media_by_date_per_username, usernames, start_year, end_year)
            else:
                sent_images = 0

            # Generate and upload HTML
            html_content = create_html(media_by_date_per_username, usernames, start_year, end_year)
            if html_content:
                html_file = BytesIO(html_content.encode('utf-8'))
                filename = f"{'_'.join(username.replace(' ', '_') for username in usernames)}_media.html"
                total_items = sum(
                    sum(len(media_by_date_per_username[username][media_type][date])
                        for media_type in media_by_date_per_username[username]
                        for date in media_by_date_per_username[username][media_type])
                    for username in usernames
                )
                try:
                    url = upload_file(html_file, filename)
                    send_telegram_message(
                        chat_id,
                        text=(
                            f"✅ Uploaded {filename}\n"
                            f"Found {total_items} media items (Sent {sent_images} images) for '{usernames_display}' ({start_year}-{end_year})\n"
                            f"Raw File: {url}\n"
                            f"Rendered: https://htmlpreview.github.io/?{url}"
                        )
                    )
                except ScraperError as e:
                    send_telegram_message(
                        chat_id,
                        text=f"❌ Upload failed for '{usernames_display}': {str(e)}"
                    )
            else:
                send_telegram_message(
                    chat_id,
                    text=f"⚠️ No media found for '{usernames_display}' ({start_year}-{end_year})"
                )

        except ScraperError as e:
            if str(e) == "Task cancelled by user":
                send_telegram_message(
                    chat_id,
                    text=f"🛑 Scraping stopped for '{usernames_display}'"
                )
            else:
                send_telegram_message(
                    chat_id,
                    text=f"❌ Error for '{usernames_display}': {str(e)}"
                )
                logger.error(f"Scraper error: {str(e)}\n{traceback.format_exc()}")
        except Exception as e:
            send_telegram_message(
                chat_id,
                text=f"❌ Error for '{usernames_display}': {str(e)}"
            )
            logger.error(f"Error processing {usernames_display}: {str(e)}\n{traceback.format_exc()}")
        finally:
            if chat_id in active_tasks:
                executor, _ = active_tasks[chat_id]
                executor._threads.clear()
                executor.shutdown(wait=False)
                del active_tasks[chat_id]

    except Exception as e:
        logger.critical(f"Unhandled error: {str(e)}\n{traceback.format_exc()}")
        if 'chat_id' in locals():
            try:
                send_telegram_message(
                    chat_id,
                    text=f"❌ Critical error: {str(e)}"
                )
            except:
                pass
        if 'chat_id' in locals() and chat_id in active_tasks:
            executor, _ = active_tasks[chat_id]
            executor._threads.clear()
            executor.shutdown(wait=False)
            del active_tasks[chat_id]

def start_bot():
    """Start the Telegram bot with polling and single-instance enforcement."""
    lock_file = acquire_lock()  # Ensure only one instance runs
    max_attempts = 10  # Increased retries for robustness
    for attempt in range(max_attempts):
        try:
            logger.info(f"Attempting to start bot polling (attempt {attempt + 1})...")
            bot.message_handler(commands=['start'])(handle_message)
            bot.message_handler(func=lambda message: True)(handle_message)
            bot.polling(none_stop=True, interval=POLLING_INTERVAL, timeout=POLLING_TIMEOUT)
            break
        except ApiTelegramException as e:
            if e.error_code == 429:  # Too Many Requests
                retry_after = int(e.result_json.get('parameters', {}).get('retry_after', 5))
                logger.warning(f"429 Too Many Requests, waiting {retry_after} seconds")
                time.sleep(retry_after)
            elif e.error_code == 409:
                logger.warning(f"409 Conflict detected on attempt {attempt + 1}: {str(e)}")
                if attempt < max_attempts - 1:
                    logger.info(f"Retrying after {CONFLICT_RETRY_DELAY} seconds...")
                    time.sleep(CONFLICT_RETRY_DELAY)
                else:
                    logger.error("Max retries reached for 409 conflict. Exiting.")
                    raise
            else:
                logger.error(f"Telegram API error: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Failed to start bot: {str(e)}\n{traceback.format_exc()}")
            if attempt < max_attempts - 1:
                logger.info(f"Retrying after {CONFLICT_RETRY_DELAY} seconds...")
                time.sleep(CONFLICT_RETRY_DELAY)
            else:
                logger.error("Max retries reached. Exiting.")
                raise
        finally:
            if 'lock_file' in locals():
                lock_file.close()

@app.route('/')
def home():
    return "🤖 Bot is Running!"

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "time": datetime.now().isoformat()})

def run_flask_app():
    app.run(host='0.0.0.0', port=5000)

if __name__ == "__main__":
    try:
        TELEGRAM_BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
        GROUP_CHAT_ID = os.environ['GROUP_CHAT_ID']
        bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

        from threading import Thread
        Thread(target=start_bot).start()
        run_flask_app()

    except KeyError as e:
        logger.error(f"Environment variable not set: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize bot: {str(e)}\n{traceback.format_exc()}")
        raise
