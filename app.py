import os
import requests
from bs4 import BeautifulSoup
import re
import logging
from urllib.parse import urljoin
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import telebot
from datetime import datetime, timedelta
import traceback
import time
import cloudscraper

# Initialize logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
MAX_RETRIES = 3
MAX_WORKERS = 18
MAX_PAGES_PER_SEARCH = 10
BASE_URL = "https://desifakes.com"
MAX_FILE_SIZE_MB = 50  # Max file size for upload (in MB)

# Global task tracking
active_tasks = {}

# Allowed chat IDs
ALLOWED_CHAT_IDS = {5809601894, 1285451259}

class ScraperError(Exception):
    pass

def make_request(url, method='get', **kwargs):
    """Make a request using cloudscraper to bypass Cloudflare protection."""
    scraper = cloudscraper.create_scraper()
    initial_timeout = 8
    timeout_increment = 5

    for attempt in range(MAX_RETRIES):
        current_timeout = initial_timeout + (attempt * timeout_increment)
        try:
            response = scraper.request(
                method,
                url,
                timeout=current_timeout,
                **kwargs
            )
            response.raise_for_status()
            logger.info(f"Success on attempt {attempt + 1} for {url}")
            return response
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            if '503 Service Unavailable' in str(e):
                logger.warning(f"503 detected, waiting 10 seconds before retry")
                time.sleep(10)
            if attempt == MAX_RETRIES - 1:
                raise ScraperError(f"Failed for {url} after {MAX_RETRIES} attempts: {str(e)}")
            time.sleep(1 * (attempt + 1))

def upload_file(file_buffer, filename):
    """Upload file to hosting services and return the URL."""
    file_buffer.seek(0, os.SEEK_END)
    file_size_mb = file_buffer.tell() / (1024 * 1024)
    file_buffer.seek(0)

    if file_size_mb > MAX_FILE_SIZE_MB:
        raise ScraperError(f"File {filename} is {file_size_mb:.2f} MB, exceeds {MAX_FILE_SIZE_MB} MB limit")

    # 1. Try Catbox
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
    """Generate search URLs for a username and date range efficiently with 3-day buffer."""
    if not username or not isinstance(username, str):
        raise ValueError("Invalid username")

    now = datetime.now()
    current_year = now.year
    current_month = now.month
    current_day = now.day

    start_year = max(2010, start_year)
    end_year = min(end_year, current_year)

    encoded_username = username.replace(' ', '+')
    search_id = "40169483"
    base_url = f"{BASE_URL}/search/{search_id}/"
    title_flag = 1 if title_only else 0

    # Extended date ranges with 3-day buffer before and after
    months = [
        ("12-13", "01-03"), ("11-28", "12-18"), ("11-13", "12-03"), ("10-29", "11-18"),
        ("10-13", "11-03"), ("09-28", "10-18"), ("09-13", "10-03"), ("08-29", "09-18"),
        ("08-13", "09-03"), ("07-29", "08-18"), ("07-13", "08-03"), ("06-28", "07-18"),
        ("06-13", "07-03"), ("05-29", "06-18"), ("05-13", "06-03"), ("04-28", "05-18"),
        ("04-13", "05-03"), ("03-29", "04-18"), ("03-13", "04-03"), ("02-26", "03-18"),
        ("02-13", "03-03"), ("01-29", "02-18"), ("01-13", "02-03"), ("12-29", "01-18")
    ]

    links = []

    for year in range(end_year, start_year - 1, -1):
        for start_month, end_month in months:
            # Adjust years for date ranges crossing year boundaries
            start_year_adj = year - 1 if start_month.startswith("12-") else year
            end_year_adj = year + 1 if end_month.startswith("01-") else year

            start_date = f"{start_year_adj}-{start_month}"
            end_date = f"{end_year_adj}-{end_month}"

            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError as e:
                logger.error(f"Invalid date format: {start_date} to {end_date} - {str(e)}")
                continue

            # Skip if start date is in the future
            if start_dt > now:
                continue

            # Adjust end date if it exceeds current date
            if end_dt > now:
                end_date = now.strftime("%Y-%m-%d")

            # Validate date range
            if start_dt >= end_dt:
                logger.warning(f"Invalid date range: {start_date} to {end_date}")
                continue

            url = (
                f"{base_url}?q={encoded_username}"
                f"&c[newer_than]={start_date}"
                f"&c[older_than]={end_date}"
                f"&c[title_only]={title_flag}&o=date"
            )

            links.append((year, url, start_date, end_date))

    if not links:
        logger.warning(f"No valid URLs generated for username: {username}")

    logger.info(f"Generated {len(links)} search URLs for {username}")
    return links

def split_url(url, start_date, end_date, max_pages=MAX_PAGES_PER_SEARCH):
    """Split search URL into smaller date ranges with 3-day buffer until pages < 10."""
    try:
        # Parse start and end dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Validate date range
        if start_dt >= end_dt:
            logger.warning(f"Invalid date range: {start_date} to {end_date}")
            return [(url, start_date, end_date)]

        # Add 3-day buffer before and after
        buffered_start_dt = start_dt - timedelta(days=3)
        buffered_end_dt = end_dt + timedelta(days=3)
        now = datetime.now()

        # Ensure buffered dates don't exceed current date or go before reasonable start
        if buffered_end_dt > now:
            buffered_end_dt = now
        if buffered_start_dt.year < 2010:  # Assuming 2010 as minimum year
            buffered_start_dt = datetime(2010, 1, 1)

        buffered_start_date = buffered_start_dt.strftime("%Y-%m-%d")
        buffered_end_date = buffered_end_dt.strftime("%Y-%m-%d")

        # Update URL with buffered dates
        buffered_url = re.sub(r"c\[newer_than\]=[^&]+", f"c[newer_than]={buffered_start_date}", url)
        buffered_url = re.sub(r"c\[older_than\]=[^&]+", f"c[older_than]={buffered_end_date}", buffered_url)

        # Make request to check pagination
        try:
            response = make_request(buffered_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            pagination = soup.find('div', class_='pageNav')
            total_pages = 1
            if pagination:
                page_numbers = [int(link.text.strip()) for link in pagination.find_all('a') if link.text.strip().isdigit()]
                total_pages = max(page_numbers) if page_numbers else 1
        except Exception as e:
            logger.error(f"Failed to fetch or parse pagination for {buffered_url}: {str(e)}")
            return [(buffered_url, buffered_start_date, buffered_end_date)]

        # If pages are less than 10, return the URL with buffered dates
        if total_pages < max_pages:
            return [(buffered_url, buffered_start_date, buffered_end_date)]

        # Calculate midpoint for splitting
        total_days = (buffered_end_dt - buffered_start_dt).days
        if total_days <= 1:  # Prevent infinite recursion for very small ranges
            logger.warning(f"Cannot split range further: {buffered_start_date} to {buffered_end_date}")
            return [(buffered_url, buffered_start_date, buffered_end_date)]

        mid_dt = buffered_start_dt + timedelta(days=total_days // 2)

        # Define two new date ranges with 3-day buffers
        first_range_start = buffered_start_dt
        first_range_end = mid_dt + timedelta(days=3)  # Add 3-day buffer to end
        second_range_start = mid_dt - timedelta(days=3)  # Add 3-day buffer to start
        second_range_end = buffered_end_dt

        # Ensure dates don't exceed boundaries
        if first_range_end > now:
            first_range_end = now
        if second_range_start > now:
            second_range_start = now

        first_range = (first_range_start.strftime("%Y-%m-%d"), first_range_end.strftime("%Y-%m-%d"))
        second_range = (second_range_start.strftime("%Y-%m-%d"), second_range_end.strftime("%Y-%m-%d"))

        # Update URLs for new ranges
        first_url = re.sub(r"c\[newer_than\]=[^&]+", f"c[newer_than]={first_range[0]}", buffered_url)
        first_url = re.sub(r"c\[older_than\]=[^&]+", f"c[older_than]={first_range[1]}", first_url)
        second_url = re.sub(r"c\[newer_than\]=[^&]+", f"c[newer_than]={second_range[0]}", buffered_url)
        second_url = re.sub(r"c\[older_than\]=[^&]+", f"c[older_than]={second_range[1]}", second_url)

        # Recursively split the new ranges
        return (split_url(first_url, first_range[0], first_range[1], max_pages) +
                split_url(second_url, second_range[0], second_range[1], max_pages))

    except Exception as e:
        logger.error(f"Split error for {url}: {start_date} to {end_date} - {str(e)}")
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
    """Process a post to extract media with exact date, including all media if page title matches username."""
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
    """Generate HTML with single-selection username buttons, media type selection, and batched items per user for 'All'."""
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
"""]

    total_items = 0
    media_counts = {}
    for username in usernames:
        media_by_date = media_by_date_per_username[username]
        count = sum(
            len(media_by_date[media_type][date])
            for media_type in media_by_date
            for date in media_by_date[media_type]
        )
        media_counts[username] = count
        total_items += count

    if total_items == 0:
        logger.warning(f"No media items found for {usernames_str}")
        return None

    html_fragments.append("    const mediaData = {\n")
    for username in usernames:
        media_by_date = media_by_date_per_username[username]
        media_list = []
        for media_type in ['images', 'videos', 'gifs']:
            for date in sorted(media_by_date[media_type].keys(), reverse=True):
                for item in media_by_date[media_type][date]:
                    if not item.startswith(('http://', 'https://')):
                        logger.warning(f"Skipping invalid URL for {username}: {item}")
                        continue
                    safe_src = item.replace('"', '\\"').replace('\n', '')
                    media_list.append({'type': media_type, 'src': safe_src, 'date': date})

        media_list = sorted(media_list, key=lambda x: x['date'], reverse=True)

        html_fragments.append(f"      {username.replace(' ', '_')}: [\n")
        for item in media_list:
            item_str = f'        {{type: "{item["type"]}", src: "{item["src"]}", date: "{item["date"]}"}},\n'
            html_fragments.append(item_str)
        html_fragments.append("      ],\n")
        logger.info(f"Username {username}: {media_counts[username]} media items")
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
                element.src = allMedia[index].src;
                element.controls = true;
                element.alt = "Video";
                element.onerror = () => console.error('Failed to load video:', allMedia[index].src);
              }} else {{
                element = document.createElement("img");
                element.src = allMedia[index].src;
                element.alt = allMedia[index].type.charAt(0).toUpperCase() + allMedia[index].type.slice(1);
                element.onerror = () => console.error('Failed to load image:', allMedia[index].src);
              }}
              columns[actualCol].appendChild(element);
            }}
          }}
        }}
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
    """Send a Telegram message with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            return bot.send_message(chat_id=chat_id, text=text, **kwargs)
        except Exception as e:
            logger.warning(f"Send message attempt {attempt + 1} failed: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise ScraperError(f"Failed to send message: {str(e)}")
            time.sleep(1 * (attempt + 1))

def cancel_task(chat_id):
    """Cancel an active scraping task."""
    if chat_id in active_tasks:
        executor, futures = active_tasks[chat_id]
        for future in futures:
            future.cancel()
        executor._threads.clear()
        del active_tasks[chat_id]
        return True
    return False

@app.route('/telegram', methods=['POST'])
def telegram_webhook():
    """Handle Telegram webhook requests."""
    try:
        update = request.get_json()
        if not update or 'message' not in update:
            return '', 200

        chat_id = update['message']['chat']['id']
        message_id = update['message'].get('message_id')
        text = update['message'].get('text', '').strip()

        if chat_id not in ALLOWED_CHAT_IDS:
            send_telegram_message(chat_id=chat_id, text="❌ Restricted to specific users.", reply_to_message_id=message_id)
            return '', 200

        if not text:
            send_telegram_message(chat_id=chat_id, text="Please send a search query", reply_to_message_id=message_id)
            return '', 200

        if text.lower() == '/stop':
            if cancel_task(chat_id):
                send_telegram_message(chat_id=chat_id, text="✅ Scraping stopped", reply_to_message_id=message_id)
            else:
                send_telegram_message(chat_id=chat_id, text="ℹ️ No active scraping to stop.", reply_to_message_id=message_id)
            return '', 200

        parts = text.split()
        if len(parts) < 1 or (parts[0] == '/start' and len(parts) < 2):
            send_telegram_message(
                chat_id=chat_id,
                text="Usage: username1,username2,... [title_only y/n] [start_year] [end_year]\n"
                     "Example: 'Akshra Singh,Kareena Kapoor' n 2019 2025",
                reply_to_message_id=message_id
            )
            return '', 200

        if chat_id in active_tasks:
            send_telegram_message(chat_id=chat_id, text="⚠️ Scraping already running. Use /stop to cancel.", reply_to_message_id=message_id)
            return '', 200

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
        progress_msg = send_telegram_message(
            chat_id=chat_id,
            text=f"🔍 Processing '{usernames_display}' ({start_year}-{end_year})..."
        )

        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        active_tasks[chat_id] = (executor, [])

        try:
            media_by_date_per_username = {
                username: {
                    'images': {}, 'videos': {}, 'gifs': {}
                } for username in usernames
            }
            global_seen = {'images': set(), 'videos': set(), 'gifs': set()}
            
            for username_idx, username in enumerate(usernames):
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"🔍 Processing '{usernames_display}' ({start_year}-{end_year}): Scraping '{username}'..."
                )
                
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

                processed_count = 0
                total_posts = len(all_post_links)
                update_step = max(10, total_posts // 10)
                for future in as_completed(post_futures):
                    if chat_id not in active_tasks:
                        raise ScraperError("Task cancelled by user")
                    try:
                        future.result()
                        processed_count += 1
                        if processed_count % update_step == 0 or processed_count == total_posts:
                            bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=progress_msg.message_id,
                                text=f"🔍 Processing '{usernames_display}' ({start_year}-{end_year}): "
                                     f"'{username}' - {processed_count}/{total_posts} posts "
                                     f"({(processed_count/total_posts*100):.1f}%)"
                            )
                    except ScraperError as e:
                        logger.error(f"Post processing failed for {username}: {str(e)}")
                        continue
                    except Exception as e:
                        logger.error(f"Unexpected post processing error for {username}: {str(e)}")
                        continue

            bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
            
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
                file_size_mb = len(html_content.encode('utf-8')) / (1024 * 1024)

                progress_msg = send_telegram_message(
                    chat_id=chat_id,
                    text=f"🔍 Uploading {filename} ({file_size_mb:.2f} MB)..."
                )

                try:
                    url = upload_file(html_file, filename)
                    bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
                    send_telegram_message(
                        chat_id=chat_id,
                        text=(
                            f"✅ Uploaded {filename}\n"
                            f"Found {total_items} media items for '{usernames_display}' ({start_year}-{end_year})\n"
                            f"Raw File: {url}\n"
                            f"Rendered: https://htmlpreview.github.io/?{url}"
                        )
                    )
                except ScraperError as e:
                    bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=progress_msg.message_id,
                        text=f"❌ Upload failed for '{usernames_display}': {str(e)}"
                    )
            else:
                send_telegram_message(
                    chat_id=chat_id,
                    text=f"⚠️ No media found for '{usernames_display}' ({start_year}-{end_year})"
                )

        except ScraperError as e:
            if str(e) == "Task cancelled by user":
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"🛑 Scraping stopped for '{usernames_display}'"
                )
            else:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"❌ Error for '{usernames_display}': {str(e)}"
                )
                logger.error(f"Scraper error: {str(e)}\n{traceback.format_exc()}")
        except Exception as e:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=progress_msg.message_id,
                text=f"❌ Error for '{usernames_display}': {str(e)}"
            )
            logger.error(f"Error processing {usernames_display}: {str(e)}\n{traceback.format_exc()}")
        finally:
            if chat_id in active_tasks:
                executor, _ = active_tasks[chat_id]
                executor._threads.clear()
                del active_tasks[chat_id]

        return '', 200
    
    except Exception as e:
        logger.critical(f"Unhandled error: {str(e)}\n{traceback.format_exc()}")
        if 'chat_id' in locals() and 'progress_msg' in locals():
            try:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=f"❌ Critical error: {str(e)}"
                )
            except:
                pass
        if chat_id in active_tasks:
            executor, _ = active_tasks[chat_id]
            executor._threads.clear()
            del active_tasks[chat_id]
        return '', 200

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "time": datetime.now().isoformat()})

def set_webhook():
    """Set Telegram webhook."""
    render_url = os.environ.get('RENDER_PUBLIC_DOMAIN')
    if render_url:
        webhook_url = f"https://{render_url}/telegram"
        bot.set_webhook(url=webhook_url)
        logger.info(f"Webhook set to: {webhook_url}")
    else:
        logger.error("RENDER_PUBLIC_DOMAIN not set")
        raise ScraperError("RENDER_PUBLIC_DOMAIN not set")

# Telegram Bot Setup
try:
    TELEGRAM_BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
    bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
except KeyError:
    logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
    raise
except Exception as e:
    logger.error(f"Failed to initialize bot: {str(e)}")
    raise

if __name__ == "__main__":
    set_webhook()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
