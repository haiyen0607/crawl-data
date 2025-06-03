import os
import json
import time
import argparse
from queue import Queue
from tqdm import tqdm
from config import Config
import sys
import csv

from src.crawler.dantri import Dantri
from worker import Producer, Consumer
from src.crawler.base import News

def get_existing_urls(file_path='data/data.csv'):
    existing_urls = set()
    if os.path.exists(file_path):
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader, None)  # Bỏ header
            for row in reader:
                if len(row) >= 5:
                    existing_urls.add(row[4])
    return existing_urls

def append_news_to_csv(news: News, file_path='data/data.csv', existing_urls=None):
    if existing_urls is None:
        existing_urls = get_existing_urls(file_path)

    if news.url in existing_urls:
        print(f"[SKIP] Already saved: {news.url}")
        return

    file_exists = os.path.isfile(file_path)
    with open(file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if os.stat(file_path).st_size == 0:
            writer.writerow(['title', 'author', 'created_at', 'content', 'url'])
        writer.writerow([
            news.title,
            news.author,
            news.created_at.isoformat(),
            news.content,
            news.url
        ])
    print(f"[SAVE] -> {news.url}")

def filter_existing_links(links, existing_urls):
    filtered = [link for link in links if link not in existing_urls]
    print(f"[FILTER] Skipped {len(links) - len(filtered)} links already in data.csv")
    return filtered

# --------------------------------------
# Main entry
# --------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl news from Dantri.")
    parser.add_argument("method", type=str, choices=["sync", "thread", "async"], default="sync",
                        help="There are 3 methods: `sync`, `thread` and `async`. Default is `sync`")
    parser.add_argument("--max_pagination", type=int, default=1, help="Number of paginations per topic")
    parser.add_argument("--num_workers", type=int, default=4, help="Number of consumer workers")
    parser.add_argument("--num_links", type=int, default=1000, help="The number of links to crawl")
    parser.add_argument("--force_crawl", action="store_true", help="Force re-crawling new URLs and merge with old ones")

    args = parser.parse_args()

    q = Queue()
    dantri = Dantri()

    if args.force_crawl:
        print(f"[CRAWL] Crawling links from Dân Trí (max_pagination={args.max_pagination}) ...")
        new_links = set(dantri.crawl(max_pagination=args.max_pagination, link_only=True, all_topics=True))

        if os.path.exists("data/urls.json"):
            with open("data/urls.json", "r", encoding="utf-8") as f:
                old_links = set(json.load(f))
        else:
            old_links = set()

        all_links = list(old_links.union(new_links))

        with open("data/urls.json", "w", encoding="utf-8") as f:
            json.dump(all_links, f, ensure_ascii=False, indent=4)
        print(f"[SAVE] Merged and saved {len(all_links)} links to data/urls.json")

    if os.path.exists("data/urls.json"):
        with open("data/urls.json", "r", encoding="utf-8") as f:
            links = json.load(f)
        print(f"[LOAD] Loaded {len(links)} links from urls.json")
    else:
        print("[ERROR] urls.json not found. Please run with --force_crawl to generate URLs.")
        exit(1)

    existing_urls = get_existing_urls(file_path="data/data.csv")
    links = filter_existing_links(links, existing_urls)
    links = links[:args.num_links]

    start_time = time.time()

    if args.method == "sync":
        print("-------------------- Synchronous --------------------")
        scrape_news = dantri.scrape_news()

        for i, link in enumerate(tqdm(links), 1):
            print(f"[{i}/{len(links)}] Fetching: {link}")
            time.sleep(Config.SLEEP_TIME)
            result = scrape_news(url=link)
            if result:
                try:
                    print("[DONE] ✓")
                except UnicodeEncodeError:
                    print("[DONE] Success")
                append_news_to_csv(result, file_path="data/data.csv", existing_urls=existing_urls)
                existing_urls.add(result.url)
            else:
                print(f"[FAIL] Could not crawl: {link}")

        print(f"[FINISH] Sync mode finished in {round(time.time() - start_time, 2)} seconds")

    elif args.method == "thread":
        print("-------------------- Threading --------------------")
        assert args.num_workers > 0, "The number of workers must be greater than 0"
        progress_bar = tqdm(total=len(links), desc="Crawling URLs", unit="URL")

        producer = Producer(q, links)
        producer.start()

        consumers = []
        for id in range(args.num_workers):
            consumer = Consumer(
                queue=q,
                instance=dantri,
                sleep_time=1.0,
                consumer_id=id,
                progress_bar=progress_bar,
            )
            consumer.start()
            consumers.append(consumer)

        producer.join()
        q.join()
        [c.join() for c in consumers]
        progress_bar.close()

        print(f"[FINISH] Thread mode finished in {round(time.time() - start_time, 2)} seconds")

    elif args.method == "async":
        print("-------------------- Asynchronous --------------------")
        print("[TODO] Async mode not implemented yet.")
