import aiohttp
import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Tuple
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APIError(Exception):
    """Custom exception for API errors"""
    pass

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
    
    async def _ensure_session(self):
        """Ensure session exists, create temporary if needed"""
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={'User-Agent': 'AsyncAPIClient/1.0'}
            )
            return True
        return False
    
    async def fetch_with_retry(self, url: str, method: str = 'GET', **kwargs) -> Union[Dict[str, Any], List[Any]]:
        temp_session = await self._ensure_session()
        
        for attempt in range(self.max_retries):
            try:
                async with self.session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        try:
                            return await response.json()
                        except json.JSONDecodeError as e:
                            error_msg = f"JSON decode error: {e}"
                            logger.error(error_msg)
                            if attempt == self.max_retries - 1:
                                raise APIError(error_msg)
                    elif response.status >= 500:
                        if attempt < self.max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.warning(f"Server error {response.status}, retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            raise APIError(f"Server error {response.status} after {self.max_retries} attempts")
                    else:
                        error_text = await response.text()
                        raise APIError(f"Client error {response.status}: {error_text[:200]}")
            except asyncio.TimeoutError:
                logger.warning(f"Timeout on attempt {attempt + 1} for {url}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    raise APIError(f"Timeout after {self.max_retries} attempts")
            except aiohttp.ClientError as e:
                logger.error(f"Client error on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    raise APIError(f"Client error: {str(e)}")
        
        if temp_session:
            await self.session.close()
            self.session = None
        
        raise APIError(f"Unknown error fetching {url}")
    
    async def fetch_multiple(self, endpoints: List[str], method: str = 'GET', **kwargs) -> List[Any]:
        urls = [f"{self.base_url}/{endpoint.lstrip('/')}" for endpoint in endpoints]
        tasks = [self.fetch_with_retry(url, method, **kwargs) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Request failed: {result}")
                processed_results.append(None)
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def create_resource(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        result = await self.fetch_with_retry(url, method='POST', json=data)
        if not isinstance(result, dict):
            raise APIError(f"Expected dict response, got {type(result)}")
        return result
    
    async def update_resource(self, endpoint: str, resource_id: Union[int, str], data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}/{resource_id}"
        result = await self.fetch_with_retry(url, method='PUT', json=data)
        if not isinstance(result, dict):
            raise APIError(f"Expected dict response, got {type(result)}")
        return result
    
    async def delete_resource(self, endpoint: str, resource_id: Union[int, str]) -> bool:
        url = f"{self.base_url}/{endpoint.lstrip('/')}/{resource_id}"
        temp_session = await self._ensure_session()
        
        try:
            async with self.session.delete(url) as response:
                return response.status in [200, 204]
        except Exception as e:
            logger.error(f"Delete failed: {e}")
            return False
        finally:
            if temp_session:
                await self.session.close()
                self.session = None

async def fetch_data(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            return None
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return None

async def fetch_posts_with_comments():
    async with AsyncAPIClient('https://jsonplaceholder.typicode.com') as client:
        post_endpoints = [f'posts/{i}' for i in range(1, 6)]
        posts = await client.fetch_multiple(post_endpoints)
        
        valid_posts = [p for p in posts if p is not None and isinstance(p, dict) and 'id' in p]
        
        comment_tasks = []
        for post in valid_posts:
            comment_tasks.append(client.fetch_multiple([f'posts/{post["id"]}/comments']))
        
        comment_results = await asyncio.gather(*comment_tasks, return_exceptions=True)
        
        for post, comment_result in zip(valid_posts, comment_results):
            if not isinstance(comment_result, Exception) and comment_result and comment_result[0] is not None:
                post['comments'] = comment_result[0]
            else:
                post['comments'] = []
        
        return valid_posts

async def demonstrate_crud_operations():
    try:
        async with AsyncAPIClient('https://jsonplaceholder.typicode.com') as client:
            print("\n=== CRUD Operations Demo ===")
            
            new_post = {
                'title': 'Async Python Post',
                'body': 'Learning async REST API calls',
                'userId': 1
            }
            print("\n1. Creating new post...")
            try:
                created = await client.create_resource('posts', new_post)
                print(f"   Created post with ID: {created.get('id')}")
            except APIError as e:
                print(f"   Error creating post: {e}")
            
            print("\n2. Reading post...")
            try:
                posts = await client.fetch_multiple(['posts/1'])
                if posts and posts[0] is not None:
                    print(f"   Post title: {posts[0].get('title')}")
                else:
                    print("   Failed to read post")
            except APIError as e:
                print(f"   Error reading post: {e}")
            
            print("\n3. Updating post...")
            try:
                updated_data = {'title': 'Updated Title', 'body': 'Updated content'}
                updated = await client.update_resource('posts', 1, updated_data)
                print(f"   Updated title: {updated.get('title')}")
            except APIError as e:
                print(f"   Error updating post: {e}")
            
            print("\n4. Deleting post...")
            deleted = await client.delete_resource('posts', 1)
            print(f"   Deleted: {deleted}")
    except Exception as e:
        print(f"CRUD operations failed: {e}")

async def fetch_with_rate_limiting():
    try:
        async with AsyncAPIClient('https://jsonplaceholder.typicode.com') as client:
            semaphore = asyncio.Semaphore(3)
            
            async def fetch_with_limit(url):
                async with semaphore:
                    try:
                        return await client.fetch_with_retry(url)
                    except APIError as e:
                        logger.error(f"Rate limited fetch failed: {e}")
                        return None
            
            tasks = [fetch_with_limit(f'{client.base_url}/posts/{i}') for i in range(1, 11)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful = [r for r in results if r is not None and not isinstance(r, Exception)]
            print(f"\nFetched {len(successful)} posts with rate limiting")
    except Exception as e:
        print(f"Rate limiting demo failed: {e}")

async def stream_large_response():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get('https://jsonplaceholder.typicode.com/posts', 
                                  params={'_limit': 100}) as response:
                if response.status == 200:
                    print("\n=== Streaming Response ===")
                    chunk_count = 0
                    total_bytes = 0
                    async for chunk in response.content.iter_chunked(1024):
                        if chunk:
                            chunk_count += 1
                            total_bytes += len(chunk)
                            if chunk_count == 1:
                                print(f"First chunk size: {len(chunk)} bytes")
                    print(f"Total chunks: {chunk_count}, Total bytes: {total_bytes}")
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
            if data and isinstance(data, dict):
                print(f"Post {i}: {data.get('title', 'No title')}")
            else:
                print(f"Post {i}: Failed to fetch")
    
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
