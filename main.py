import aiohttp
import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Callable, TypeVar, Generic
import logging
from dataclasses import dataclass, field

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

T = TypeVar('T')

class APIError(Exception):
    pass

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
    
    def record_success(self):
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def can_request(self) -> bool:
        if self.state == 'CLOSED':
            return True
        elif self.state == 'OPEN':
            if self.last_failure_time:
                time_since_failure = (datetime.now() - self.last_failure_time).total_seconds()
                if time_since_failure > self.recovery_timeout:
                    self.state = 'HALF_OPEN'
                    return True
            return False
        elif self.state == 'HALF_OPEN':
            return True
        return False

class MetricsCollector:
    def __init__(self):
        self.request_count = 0
        self.error_count = 0
        self.request_durations = []
    
    def record_request(self, duration: float):
        self.request_count += 1
        self.request_durations.append(duration)
    
    def record_error(self):
        self.error_count += 1
    
    def get_average_duration(self) -> float:
        if not self.request_durations:
            return 0.0
        return sum(self.request_durations) / len(self.request_durations)
    
    def get_success_rate(self) -> float:
        if self.request_count == 0:
            return 1.0
        return (self.request_count - self.error_count) / self.request_count

@dataclass
class ClientConfig:
    base_url: str
    max_retries: int = 3
    timeout: int = 10
    rate_limit: Optional[int] = None
    headers: Dict[str, str] = field(default_factory=lambda: {'User-Agent': 'AsyncAPIClient/1.0'})
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60

class AsyncAPIClient(Generic[T]):
    def __init__(self, config: ClientConfig):
        self.config = config
        self.base_url = config.base_url.rstrip('/')
        self.max_retries = config.max_retries
        self.timeout = config.timeout
        self.session = None
        self._owns_session = False
        self._request_interceptors = []
        self._response_interceptors = []
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_threshold,
            recovery_timeout=config.circuit_breaker_timeout
        )
        self.metrics = MetricsCollector()
        self._semaphore = asyncio.Semaphore(config.rate_limit) if config.rate_limit else None
    
    def add_request_interceptor(self, func: Callable):
        self._request_interceptors.append(func)
    
    def add_response_interceptor(self, func: Callable):
        self._response_interceptors.append(func)
    
    async def __aenter__(self):
        await self._get_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._close_session()
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None:
            timeout_settings = aiohttp.ClientTimeout(total=self.timeout)
            self.session = aiohttp.ClientSession(
                timeout=timeout_settings,
                headers=self.config.headers
            )
            self._owns_session = True
        return self.session
    
    async def _close_session(self):
        if self.session and self._owns_session:
            await self.session.close()
            self.session = None
            self._owns_session = False
    
    async def _apply_request_interceptors(self, url: str, kwargs: Dict) -> tuple:
        for interceptor in self._request_interceptors:
            url, kwargs = await interceptor(url, kwargs) if asyncio.iscoroutinefunction(interceptor) else interceptor(url, kwargs)
        return url, kwargs
    
    async def _apply_response_interceptors(self, response: Any) -> Any:
        for interceptor in self._response_interceptors:
            response = await interceptor(response) if asyncio.iscoroutinefunction(interceptor) else interceptor(response)
        return response
    
    async def fetch_with_retry(self, url: str, method: str = 'GET', expected_type: Optional[type] = None, **kwargs) -> Union[Dict[str, Any], List[Any]]:
        if not self.circuit_breaker.can_request():
            raise APIError("Circuit breaker is OPEN")
        
        start_time = datetime.now()
        
        if self._semaphore:
            await self._semaphore.acquire()
        
        try:
            session = await self._get_session()
            url, kwargs = await self._apply_request_interceptors(url, kwargs)
            
            for attempt in range(self.max_retries):
                try:
                    async with session.request(method, url, **kwargs) as response:
                        if response.status == 200:
                            try:
                                result = await response.json()
                                if expected_type and not isinstance(result, expected_type):
                                    raise APIError(f"Expected {expected_type}, got {type(result)}")
                                result = await self._apply_response_interceptors(result)
                                self.circuit_breaker.record_success()
                                duration = (datetime.now() - start_time).total_seconds()
                                self.metrics.record_request(duration)
                                return result
                            except json.JSONDecodeError as e:
                                error_msg = f"JSON decode error: {e}"
                                logger.error(error_msg)
                                if attempt == self.max_retries - 1:
                                    self.circuit_breaker.record_failure()
                                    self.metrics.record_error()
                                    raise APIError(error_msg)
                        elif response.status >= 500:
                            if attempt < self.max_retries - 1:
                                wait_time = 2 ** attempt
                                logger.warning(f"Server error {response.status}, retrying in {wait_time}s...")
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                self.circuit_breaker.record_failure()
                                self.metrics.record_error()
                                raise APIError(f"Server error {response.status} after {self.max_retries} attempts")
                        else:
                            error_text = await response.text()
                            self.circuit_breaker.record_failure()
                            self.metrics.record_error()
                            raise APIError(f"Client error {response.status}: {error_text[:200]}")
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout on attempt {attempt + 1} for {url}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(1)
                    else:
                        self.circuit_breaker.record_failure()
                        self.metrics.record_error()
                        raise APIError(f"Timeout after {self.max_retries} attempts")
                except aiohttp.ClientError as e:
                    logger.error(f"Client error on attempt {attempt + 1}: {e}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(1)
                    else:
                        self.circuit_breaker.record_failure()
                        self.metrics.record_error()
                        raise APIError(f"Client error: {str(e)}")
            
            raise APIError(f"Unknown error fetching {url}")
        finally:
            if self._semaphore:
                self._semaphore.release()
    
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
        result = await self.fetch_with_retry(url, method='POST', expected_type=dict, json=data)
        return result
    
    async def update_resource(self, endpoint: str, resource_id: Union[int, str], data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}/{resource_id}"
        result = await self.fetch_with_retry(url, method='PUT', expected_type=dict, json=data)
        return result
    
    async def delete_resource(self, endpoint: str, resource_id: Union[int, str]) -> bool:
        try:
            url = f"{self.base_url}/{endpoint.lstrip('/')}/{resource_id}"
            await self.fetch_with_retry(url, method='DELETE')
            return True
        except APIError as e:
            logger.error(f"Delete failed: {e}")
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        return {
            'request_count': self.metrics.request_count,
            'error_count': self.metrics.error_count,
            'average_duration': self.metrics.get_average_duration(),
            'success_rate': self.metrics.get_success_rate(),
            'circuit_breaker_state': self.circuit_breaker.state
        }

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
    config = ClientConfig(base_url='https://jsonplaceholder.typicode.com')
    async with AsyncAPIClient(config) as client:
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
        config = ClientConfig(base_url='https://jsonplaceholder.typicode.com')
        async with AsyncAPIClient(config) as client:
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
            
            print("\n=== Metrics ===")
            metrics = client.get_metrics()
            for key, value in metrics.items():
                print(f"   {key}: {value}")
    except Exception as e:
        print(f"CRUD operations failed: {e}")

async def fetch_with_rate_limiting():
    try:
        config = ClientConfig(
            base_url='https://jsonplaceholder.typicode.com',
            rate_limit=3
        )
        async with AsyncAPIClient(config) as client:
            async def fetch_with_limit(i):
                try:
                    return await client.fetch_with_retry(f'{client.base_url}/posts/{i}')
                except APIError as e:
                    logger.error(f"Rate limited fetch failed: {e}")
                    return None
            
            tasks = [fetch_with_limit(i) for i in range(1, 11)]
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

async def interceptor_example(url: str, kwargs: Dict) -> tuple:
    logger.info(f"Request to {url}")
    if 'params' not in kwargs:
        kwargs['params'] = {}
    kwargs['params']['_t'] = datetime.now().timestamp()
    return url, kwargs

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
    
    print("\n=== Interceptor Demo ===")
    config = ClientConfig(base_url='https://jsonplaceholder.typicode.com')
    async with AsyncAPIClient(config) as client:
        client.add_request_interceptor(interceptor_example)
        result = await client.fetch_with_retry(f'{client.base_url}/posts/1')
        print(f"Fetched post with interceptor: {result.get('title')}")

if __name__ == "__main__":
    start_time = datetime.now()
    asyncio.run(main())
    end_time = datetime.now()
    print(f"\nTotal execution time: {end_time - start_time}")
