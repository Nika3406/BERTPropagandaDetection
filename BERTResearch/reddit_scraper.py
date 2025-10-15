"""
reddit_scraper.py
Smart scraper with rate limiting for Reddit Free API.
"""

import praw
import pandas as pd
import os
import time
import random
from dotenv import load_dotenv
from tqdm import tqdm

# ==== CONFIG ====
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_AGENT = os.getenv("USER_AGENT")

KEYWORDS_FILE = "keywords.txt"
SEMEVAL_FILE = "propaganda_examples.csv"
OUTPUT_FILE = "reddit_data.csv"

SUBREDDITS = [
    "politics",
    "PoliticalCompassMemes",
    "conservative",
    "liberal",
    "news",
    "politicaldiscussion",
    "CapitalismVSocialism",
    "NeutralPolitics"
]

LIMIT_PER_SEARCH = 150
MIN_SLEEP = 4         # minimum seconds between queries
MAX_SLEEP = 7         # randomized delay
MAX_QUERIES_PER_MIN = 90  # keep below Reddit's 100/min limit

# ==== AUTH ====
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT
)

# ==== LOAD DATA ====
keywords = []
if os.path.exists(KEYWORDS_FILE):
    with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
        keywords = [line.strip().lower() for line in f if line.strip()]

if os.path.exists(SEMEVAL_FILE):
    df = pd.read_csv(SEMEVAL_FILE)
    propaganda_phrases = (
        df["text_snippet"].dropna().astype(str).str.lower().tolist()
    )
    propaganda_phrases = [p for p in propaganda_phrases if 3 <= len(p.split()) <= 12]
    propaganda_phrases = list(set(propaganda_phrases))
else:
    propaganda_phrases = []

print(f"[INFO] Loaded {len(keywords)} keywords and {len(propaganda_phrases)} phrases")

# ==== HELPERS ====
def contains_phrase(text, phrases):
    t = text.lower()
    return any(p in t for p in phrases)

def rate_limit_control(query_count, start_time):
    elapsed = time.time() - start_time
    if query_count >= MAX_QUERIES_PER_MIN:
        if elapsed < 60:
            sleep_for = 60 - elapsed + 1
            print(f"[RATE LIMIT] Hit {MAX_QUERIES_PER_MIN} queries in under a minute. Sleeping {sleep_for:.1f}s...")
            time.sleep(sleep_for)
        return 0, time.time()  # reset counter
    return query_count, start_time


# ==== SCRAPE ====
posts = []
query_count = 0
start_time = time.time()

for subreddit in SUBREDDITS:
    print(f"\n[SCRAPING SUBREDDIT] r/{subreddit}")
    for kw in keywords:
        print(f"  [SEARCH] '{kw}'")
        try:
            # Check rate limit
            query_count, start_time = rate_limit_control(query_count, start_time)

            # One query per search
            submissions = reddit.subreddit(subreddit).search(kw, limit=LIMIT_PER_SEARCH, sort="new")
            query_count += 1

            for submission in tqdm(submissions, desc=f"{subreddit}/{kw}"):
                title = submission.title or ""
                body = submission.selftext or ""
                combined = (title + " " + body).lower()
                if contains_phrase(combined, propaganda_phrases):
                    posts.append({
                        "keyword": kw,
                        "subreddit": subreddit,
                        "title": submission.title,
                        "text": submission.selftext,
                        "score": submission.score,
                        "url": submission.url,
                        "created_utc": submission.created_utc,
                        "id": submission.id
                    })

            # Randomized pause between queries
            sleep_for = random.uniform(MIN_SLEEP, MAX_SLEEP)
            time.sleep(sleep_for)

        except Exception as e:
            print(f"[WARN] Error searching '{kw}' in r/{subreddit}:", e)
            time.sleep(10)

# ==== SAVE ====
df = pd.DataFrame(posts)
df.drop_duplicates(subset="id", inplace=True)
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
print(f"\n[DONE] Saved {len(df)} filtered posts to {OUTPUT_FILE}")
