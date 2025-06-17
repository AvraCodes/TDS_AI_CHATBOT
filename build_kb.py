import os
import json
import sqlite3
import aiohttp
import asyncio
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_PATH = "knowledge_base.db"

async def get_embedding(text):
    url = "https://aipipe.org/openai/v1/embeddings"
    headers = {"Authorization": API_KEY, "Content-Type": "application/json"}
    payload = {"model": "text-embedding-3-small", "input": text}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as resp:
            data = await resp.json()
            return data["data"][0]["embedding"]

def chunk_text(text, chunk_size=500):
    # Simple chunking by words
    words = text.split()
    return [" ".join(words[i:i+chunk_size]) for i in range(0, len(words), chunk_size)]

async def process_discourse():
    print("Processing discourse_posts.json...")
    with open("discourse_posts.json") as f:
        posts = json.load(f)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for post in posts:
        content = post.get("content", "")
        if not content.strip():
            continue
        chunks = chunk_text(content)
        for idx, chunk in enumerate(chunks):
            embedding = await get_embedding(chunk)
            c.execute(
                "INSERT INTO discourse_chunks (post_id, topic_id, topic_title, post_number, author, created_at, likes, chunk_index, content, url, embedding) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    post.get("post_id"),
                    post.get("topic_id"),
                    post.get("topic_title"),
                    post.get("post_number"),
                    post.get("author"),
                    post.get("created_at"),
                    post.get("like_count", 0),
                    idx,
                    chunk,
                    post.get("url"),
                    json.dumps(embedding),
                ),
            )
    conn.commit()
    conn.close()
    print("Finished processing Discourse posts.")

async def process_markdown():
    print("Processing markdown files...")
    import glob
    from datetime import datetime
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for md_file in glob.glob("tds_pages_md/*.md"):
        with open(md_file, encoding="utf-8") as f:
            content = f.read()
        title = os.path.basename(md_file)
        chunks = chunk_text(content)
        for idx, chunk in enumerate(chunks):
            embedding = await get_embedding(chunk)
            c.execute(
                "INSERT INTO markdown_chunks (doc_title, original_url, downloaded_at, chunk_index, content, embedding) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    title,
                    "",  # Fill with URL if you have it
                    datetime.now().isoformat(),
                    idx,
                    chunk,
                    json.dumps(embedding),
                ),
            )
    conn.commit()
    conn.close()
    print("Finished processing markdown files.")

async def main():
    await process_discourse()
    await process_markdown()

if __name__ == "__main__":
    conn = sqlite3.connect("knowledge_base.db")
    c = conn.cursor()

    # Create discourse_chunks table if it doesn't exist
    c.execute('''
    CREATE TABLE IF NOT EXISTS discourse_chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        post_id INTEGER,
        topic_id INTEGER,
        topic_title TEXT,
        post_number INTEGER,
        author TEXT,
        created_at TEXT,
        likes INTEGER,
        chunk_index INTEGER,
        content TEXT,
        url TEXT,
        embedding BLOB
    )
    ''')

    # Create markdown_chunks table if it doesn't exist
    c.execute('''
    CREATE TABLE IF NOT EXISTS markdown_chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_title TEXT,
        original_url TEXT,
        downloaded_at TEXT,
        chunk_index INTEGER,
        content TEXT,
        embedding BLOB
    )
    ''')

    conn.commit()
    conn.close()
    asyncio.run(main())