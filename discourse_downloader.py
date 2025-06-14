import requests
import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

CATEGORY_ID = 34
CATEGORY_SLUG = "courses/tds-kb"
BASE_URL = "https://discourse.onlinedegree.iitm.ac.in/"
START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2025, 4, 14, 23, 59, 59, tzinfo=timezone.utc)

RAW_COOKIE_STRING = "_ga=GA1.1.978463042.1718463610; _gcl_au=1.1.1082993342.1742732265; _ga_5HTJMW67XK=GS1.1.1745901014.19.0.1745901026.0.0.0; _bypass_cache=true; _ga_08NPRH5L4M=GS2.1.s1749666975$o228$g0$t1749666975$j60$l0$h0$dAuXmMOLk-nqxNC514YRW9IaeW_a3fKle3A; _t=Ky4BHVuhvZL6pXiK4kTGKVprgNNhoDpOvOhFUbPO8fHHa%2FM2N3bgOZ452XTsnVTbD8w9n58%2BQaGUZK68Lytc96aPhToi%2FLD05frkBXNnOEuKJqDLlk5e%2BZvxfqt6scIiWtV2wilHxRmfn9E8fYUxQSVFwgoCQFe12daRPltp20DH0wTPPk4Q8bG9ccvv7A%2FJs8PuSfc7q0rjIPKXMrO35lzrUEbrHZxzv38DpLEqTvjSLkyL9aEh6OD9F7RPWYgYX%2BAXz4ZPuxB2szPuyxtAQnIICu3dAHA2Uwuf6P9EiqfjgphubdJUBw%3D%3D--7ScEKKYRA4kCpwaz--VEYnjw%2FG7WN%2F1aEugSBO7g%3D%3D; _forum_session=aKcRH0NV6WHD71NJmH7YBT4vU3BA7MpJadwplcsEPTWvC2lIS%2BuBPlUyEPCCV7IDj734W99Mcl9cFol9VzJqZmTu%2FUksMYS4lqIBLc4IqtiGLK1JjpXGFDw54fQabe8eYAhW2tK0Ud9j%2Fk0ysT%2FB%2F%2FBJfnknE7VR%2F9jUPOj4VPaPHnO6yFwOsIlHd5eCgsA7vr3Fw2dErK48pd4MZvRxcSp5%2B%2FH5MYCFMjfYid87H6UYSjPlbHiZ784rdCtqMgLG4TCMbHyWFSl6GPMY3OvrSZtj%2F4WNyo%2B%2F9Jr0vp0iPF4F2bdKFvE0%2Fz28NrcNjeRSceEYFX0GuYIqfvZMZKta7TPreG1iKhOrg0hT%2F%2FaEbT3izlsC3dr3CpZnnjlSvg%3D%3D--O6ovVYF3kHan%2FSL%2B--AyJssC6BHR1Jm6wgR70%2FlQ%3D%3D"

OUTPUT_DIR = "scraped_threads"
MAX_RETRIES = 3
RETRY_DELAY = 3  # seconds

def parse_cookie_string(raw_cookie_string):
    cookies = {}
    for cookie_part in raw_cookie_string.strip().split(";"):
        if "=" in cookie_part:
            key, value = cookie_part.strip().split("=", 1)
            cookies[key] = value
    return cookies

cookies = parse_cookie_string(RAW_COOKIE_STRING)

def robust_request(url, cookies, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY):
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, cookies=cookies, timeout=15)
            if resp.status_code == 200:
                return resp
            else:
                print(f"HTTP {resp.status_code} for {url} (attempt {attempt})")
        except requests.RequestException as e:
            print(f"Request error for {url} (attempt {attempt}): {e}")
        if attempt < max_retries:
            time.sleep(retry_delay)
    print(f"Failed to fetch {url} after {max_retries} attempts.")
    return None

def fetch_topics():
    topics = []
    page = 0
    seen_topic_ids = set()
    while True:
        url = urljoin(BASE_URL, f"c/{CATEGORY_SLUG}/{CATEGORY_ID}.json?page={page}")
        resp = robust_request(url, cookies)
        if not resp:
            break
        try:
            data = resp.json()
        except Exception as e:
            print(f"Error decoding JSON on page {page}: {e}")
            break
        page_topics = data.get("topic_list", {}).get("topics", [])
        if not page_topics:
            break
        new_topics_this_page = 0
        for topic in page_topics:
            created_at = topic.get("created_at")
            topic_id = topic.get("id")
            if created_at and topic_id:
                try:
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                except Exception as e:
                    print(f"Error parsing date '{created_at}' for topic {topic_id}: {e}")
                    continue
                if START_DATE <= dt <= END_DATE and topic_id not in seen_topic_ids:
                    topics.append(topic)
                    seen_topic_ids.add(topic_id)
                    new_topics_this_page += 1
        if new_topics_this_page == 0:
            break
        if not data.get("topic_list", {}).get("more_topics_url"):
            break
        page += 1
    return topics

def fetch_thread_posts(topic_id):
    url = urljoin(BASE_URL, f"t/{topic_id}.json")
    resp = robust_request(url, cookies)
    if not resp:
        return None
    try:
        return resp.json()
    except Exception as e:
        print(f"Error decoding thread {topic_id}: {e}")
        return None

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    topics = fetch_topics()
    print(f"Found {len(topics)} threads in date range.")
    failed_topics = []
    for idx, topic in enumerate(topics, 1):
        topic_id = topic.get("id")
        title = topic.get("title", "")
        print(f"[{idx}/{len(topics)}] Scraping topic ID {topic_id}: {title!r} ...")
        thread_data = fetch_thread_posts(topic_id)
        if thread_data:
            filename = os.path.join(OUTPUT_DIR, f"thread_{topic_id}.json")
            try:
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(thread_data, f, indent=2, ensure_ascii=False)
                print(f"Saved thread {topic_id} to {filename}")
            except Exception as e:
                print(f"Failed to save thread {topic_id}: {e}")
                failed_topics.append(topic_id)
        else:
            print(f"Failed to scrape topic ID {topic_id}")
            failed_topics.append(topic_id)
    if failed_topics:
        print(f"\nFailed to fetch/save {len(failed_topics)} topics: {failed_topics}")
    else:
        print("\nAll topics scraped and saved successfully.")

if __name__ == "__main__":
    main()