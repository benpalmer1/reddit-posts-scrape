import requests
import json
import csv
from datetime import datetime
import time

RATE_LIMIT = 60  # Maximum requests per minute
TIME_WINDOW = 60  # Time window in seconds

def get_posts(subreddit, keywords=None, limit=1000, rate_limit=RATE_LIMIT):
    url = f"https://www.reddit.com/r/{subreddit}/new.json" if not keywords else f"https://www.reddit.com/r/{subreddit}/search.json?q={keywords}&restrict_sr=1"
    headers = {'User-Agent': 'my-app'}
    posts = []
    after = None
    requests_made = 0
    start_time = time.time()
    
    while len(posts) < limit:
        if requests_made >= rate_limit:
            end_time = time.time()
            elapsed_time = end_time - start_time
            if elapsed_time < TIME_WINDOW:
                sleep_time = TIME_WINDOW - elapsed_time
                time.sleep(sleep_time)
            start_time = time.time()
            requests_made = 0

        params = {'limit': 100}
        if after:
            params['after'] = after
            
        response = requests.get(url, headers=headers, params=params)
        requests_made += 1
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            break
            
        data = response.json()
        posts_data = data['data']['children']
        posts.extend(posts_data)
        
        for post in posts_data:
            if requests_made >= rate_limit:
                end_time = time.time()
                elapsed_time = end_time - start_time
                if elapsed_time < TIME_WINDOW:
                    sleep_time = TIME_WINDOW - elapsed_time
                    time.sleep(sleep_time)
                start_time = time.time()
                requests_made = 0

            post_id = post['data']['id']
            comments_url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"
            comments_response = requests.get(comments_url, headers=headers)
            requests_made += 1
            if comments_response.status_code == 200:
                comments_data = comments_response.json()
                post['comments'] = comments_data[1]['data']['children']
        
        after = data['data']['after']
        if not after:
            break

    return posts[:limit]

def write_csv(posts, subreddit):
    with open(f"{subreddit}_posts.csv", 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Post ID", "Title", "Content", "Date"])
        for post in posts:
            date = datetime.utcfromtimestamp(post['data']['created_utc']).strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([post['data']['id'], post['data']['title'], post['data']['selftext'], date])
            
    with open(f"{subreddit}_comments.csv", 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Post ID", "Comment ID", "Comment", "Date"])
        for post in posts:
            post_id = post['data']['id']
            for comment in post.get('comments', []):
                date = datetime.utcfromtimestamp(comment['data']['created_utc']).strftime('%Y-%m-%d %H:%M:%S')
                writer.writerow([post_id, comment['data']['id'], comment['data']['body'], date])

if __name__ == "__main__":
    subreddit = "SubredditName"
    keywords = "Keyword/sToSearch"
    limit = 1000 #number of posts
    posts = get_posts(subreddit, keywords, limit)
    
    write_csv(posts, subreddit)
