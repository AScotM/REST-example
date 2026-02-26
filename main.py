import aiohttp
import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AsyncAPIClient:
    def __init__(self, base_url: str, max_retries: int = 3, timeout: int = 10):
        self.base_url = base_url.rstrip('/')
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            headers={'User-Agent': 'AsyncAPIClient/1.0'}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_with_retry(self, url: str, method: str = 'GET', **kwargs) -> Any:
        for attempt in range(self.max_retries):
            try:
                async with self.session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        try:
                            return await response.json()
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error: {e}")
                            return {"error": "Invalid JSON response", "url": url}
                    elif response.status >= 500:
                        if attempt < self.max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.warning(f"Server error {response.status}, retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            return {"error": f"Server error {response.status}", "url": url}
                    else:
                        logger.error(f"Client error {response.status} for {url}")
                        return {"error": f"Client error {response.status}", "url": url}
            except asyncio.TimeoutError:
                logger.warning(f"Timeout on attempt {attempt + 1} for {url}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    return {"error": "Timeout after max retries", "url": url}
            except aiohttp.ClientError as e:
                logger.error(f"Client error on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    return {"error": str(e), "url": url}
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    return {"error": f"Unexpected error: {str(e)}", "url": url}
        
        return {"error": "Unknown error", "url": url}
    
    async def fetch_multiple(self, endpoints: List[str], method: str = 'GET', **kwargs) -> List[Any]:
        urls = [f"{self.base_url}/{endpoint.lstrip('/')}" for endpoint in endpoints]
        tasks = [self.fetch_with_retry(url, method, **kwargs) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def create_resource(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        result = await self.fetch_with_retry(url, method='POST', json=data)
        if isinstance(result, dict):
            return result
        return {"error": "Unexpected response type", "data": str(result)}
    
    async def update_resource(self, endpoint: str, resource_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}/{resource_id}"
        result = await self.fetch_with_retry(url, method='PUT', json=data)
        if isinstance(result, dict):
            return result
        return {"error": "Unexpected response type", "data": str(result)}
    
    async def delete_resource(self, endpoint: str, resource_id: int) -> bool:
        url = f"{self.base_url}/{endpoint.lstrip('/')}/{resource_id}"
        try:
            async with self.session.delete(url) as response:
                return response.status in [200, 204]
        except Exception as e:
            logger.error(f"Delete failed: {e}")
            return False

async def fetch_data(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            return {"error": f"HTTP {response.status}"}
    except Exception as e:
        return {"error": str(e)}

async def fetch_posts_with_comments():
    async with AsyncAPIClient('https://jsonplaceholder.typicode.com') as client:
        post_endpoints = [f'posts/{i}' for i in range(1, 6)]
        posts = await client.fetch_multiple(post_endpoints)
        
        all_results = []
        for post in posts:
            if isinstance(post, dict) and 'id' in post and 'error' not in post:
                comments_result = await client.fetch_multiple([f'posts/{post["id"]}/comments'])
                if comments_result and not isinstance(comments_result[0], Exception):
                    if isinstance(comments_result[0], list):
                        post['comments'] = comments_result[0]
                    elif isinstance(comments_result[0], dict) and 'error' in comments_result[0]:
                        logger.warning(f"Error fetching comments: {comments_result[0]['error']}")
                        post['comments'] = []
                    else:
                        post['comments'] = []
                else:
                    post['comments'] = []
                all_results.append(post)
            elif isinstance(post, Exception):
                logger.error(f"Error fetching post: {post}")
        
        return all_results

async def demonstrate_crud_operations():
    async with AsyncAPIClient('https://jsonplaceholder.typicode.com') as client:
        print("\n=== CRUD Operations Demo ===")
        
        new_post = {
            'title': 'Async Python Post',
            'body': 'Learning async REST API calls',
            'userId': 1
        }
        print("\n1. Creating new post...")
        created = await client.create_resource('posts', new_post)
        if 'error' not in created:
            print(f"   Created post with ID: {created.get('id')}")
        else:
            print(f"   Error: {created.get('error')}")
        
        print("\n2. Reading post...")
        posts = await client.fetch_multiple(['posts/1'])
        if posts and not isinstance(posts[0], Exception):
            if isinstance(posts[0], dict) and 'error' not in posts[0]:
                print(f"   Post title: {posts[0].get('title')}")
            else:
                print(f"   Error: {posts[0].get('error', 'Unknown error')}")
        else:
            print("   Error reading post")
        
        print("\n3. Updating post...")
        updated_data = {'title': 'Updated Title', 'body': 'Updated content'}
        updated = await client.update_resource('posts', 1, updated_data)
        if isinstance(updated, dict) and 'error' not in updated:
            print(f"   Updated title: {updated.get('title')}")
        else:
            error_msg = updated.get('error', 'Unknown error') if isinstance(updated, dict) else str(updated)
            print(f"   Error updating post: {error_msg}")
        
        print("\n4. Deleting post...")
        deleted = await client.delete_resource('posts', 1)
        print(f"   Deleted: {deleted}")

async def fetch_with_rate_limiting():
    async with AsyncAPIClient('https://jsonplaceholder.typicode.com') as client:
        semaphore = asyncio.Semaphore(3)
        
        async def fetch_with_limit(url):
            async with semaphore:
                return await client.fetch_with_retry(url)
        
        tasks = [fetch_with_limit(f'{client.base_url}/posts/{i}') for i in range(1, 11)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = []
        for r in results:
            if isinstance(r, dict) and 'error' not in r:
                successful.append(r)
            elif isinstance(r, list):
                successful.append(r)
        
        print(f"\nFetched {len(successful)} posts with rate limiting")

async def stream_large_response():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get('https://jsonplaceholder.typicode.com/posts', 
                                  params={'_limit': 100}) as response:
                if response.status == 200:
                    print("\n=== Streaming Response ===")
                    async for chunk in response.content.iter_chunked(1024):
                        if chunk:
                            print(f"Received chunk of size: {len(chunk)} bytes")
                            break
                else:
                    print(f"\n=== Streaming Response Failed: HTTP {response.status} ===")
    except Exception as e:
        print(f"\n=== Streaming Response Failed: {e} ===")

async def main():
    print("=== Basic Concurrent Fetches ===")
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_data(session, 'https://jsonplaceholder.typicode.com/posts/1'),
            fetch_data(session, 'https://jsonplaceholder.typicode.com/posts/2'),
            fetch_data(session, 'https://jsonplaceholder.typicode.com/posts/3')
        ]
        results = await asyncio.gather(*tasks)
        for i, data in enumerate(results, 1):
            if isinstance(data, dict) and 'title' in data:
                print(f"Post {i}: {data['title']}")
            else:
                print(f"Post {i}: Error fetching data")
    
    await demonstrate_crud_operations()
    
    print("\n=== Posts with Comments ===")
    posts_with_comments = await fetch_posts_with_comments()
    for post in posts_with_comments[:2]:
        print(f"\nPost: {post.get('title', 'No title')}")
        if post.get('comments'):
            print(f"Comments: {len(post['comments'])}")
            for comment in post['comments'][:2]:
                print(f"  - {comment.get('name', 'No name')}")
    
    await fetch_with_rate_limiting()
    await stream_large_response()

if __name__ == "__main__":
    start_time = datetime.now()
    asyncio.run(main())
    end_time = datetime.now()
    print(f"\nTotal execution time: {end_time - start_time}")
