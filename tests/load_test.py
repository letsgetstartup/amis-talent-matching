#!/usr/bin/env python3
"""
Load testing script for job→candidates and candidate→jobs matching.
Tests concurrent requests and performance under load.
"""
import asyncio
import aiohttp
import time
import statistics
from typing import List, Dict, Any
import json


class LoadTester:
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        self.base_url = base_url
        self.results = []
        
    async def make_request(self, session: aiohttp.ClientSession, question: str, stream: bool = False) -> Dict[str, Any]:
        """Make a single chat query request"""
        start_time = time.time()
        
        url = f"{self.base_url}/chat/query"
        if stream:
            url += "?stream=1"
            
        payload = {"question": question, "detailsOnly": True}
        
        try:
            async with session.post(url, json=payload) as response:
                if stream:
                    # For streaming, read all chunks
                    content = ""
                    async for chunk in response.content.iter_chunked(8192):
                        content += chunk.decode('utf-8', errors='ignore')
                    result = {"text": content}
                else:
                    result = await response.json()
                
                end_time = time.time()
                return {
                    "status": response.status,
                    "elapsed": end_time - start_time,
                    "success": response.status == 200,
                    "data": result
                }
        except Exception as e:
            end_time = time.time()
            return {
                "status": 0,
                "elapsed": end_time - start_time,
                "success": False,
                "error": str(e)
            }
    
    async def run_concurrent_requests(self, questions: List[str], num_concurrent: int = 10, 
                                    duration_seconds: int = 30) -> Dict[str, Any]:
        """Run concurrent requests for specified duration"""
        print(f"Starting load test: {num_concurrent} concurrent users for {duration_seconds}s")
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
        timeout = aiohttp.ClientTimeout(total=10)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            
            # Create workers
            for i in range(num_concurrent):
                task = asyncio.create_task(self._worker(session, questions, end_time, i))
                tasks.append(task)
            
            # Wait for all workers to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
        # Compile statistics
        all_requests = []
        for worker_results in results:
            if isinstance(worker_results, list):
                all_requests.extend(worker_results)
        
        return self._compile_stats(all_requests, duration_seconds)
    
    async def _worker(self, session: aiohttp.ClientSession, questions: List[str], 
                     end_time: float, worker_id: int) -> List[Dict[str, Any]]:
        """Worker that makes continuous requests until end_time"""
        requests = []
        question_idx = 0
        
        while time.time() < end_time:
            question = questions[question_idx % len(questions)]
            
            # Alternate between stream and non-stream
            use_stream = (len(requests) % 2 == 0)
            
            result = await self.make_request(session, question, stream=use_stream)
            result['worker_id'] = worker_id
            result['stream'] = use_stream
            requests.append(result)
            
            question_idx += 1
            
            # Small delay to prevent overwhelming
            await asyncio.sleep(0.1)
        
        return requests
    
    def _compile_stats(self, requests: List[Dict[str, Any]], duration: int) -> Dict[str, Any]:
        """Compile performance statistics"""
        successful = [r for r in requests if r['success']]
        failed = [r for r in requests if not r['success']]
        
        if not successful:
            return {
                "total_requests": len(requests),
                "successful": 0,
                "failed": len(failed),
                "success_rate": 0.0,
                "error": "No successful requests"
            }
        
        response_times = [r['elapsed'] for r in successful]
        
        stats = {
            "duration_seconds": duration,
            "total_requests": len(requests),
            "successful": len(successful),
            "failed": len(failed),
            "success_rate": len(successful) / len(requests) * 100,
            "requests_per_second": len(requests) / duration,
            "response_times": {
                "min": min(response_times),
                "max": max(response_times),
                "mean": statistics.mean(response_times),
                "median": statistics.median(response_times),
                "p95": statistics.quantiles(response_times, n=20)[18] if len(response_times) > 20 else max(response_times),
                "p99": statistics.quantiles(response_times, n=100)[98] if len(response_times) > 100 else max(response_times),
            },
            "errors": {},
            "stream_vs_nonstream": {
                "stream": len([r for r in successful if r.get('stream')]),
                "nonstream": len([r for r in successful if not r.get('stream')])
            }
        }
        
        # Compile error details
        for req in failed:
            error = req.get('error', f"HTTP {req['status']}")
            stats["errors"][error] = stats["errors"].get(error, 0) + 1
        
        return stats


async def main():
    """Main load test execution"""
    # Sample questions for testing (mix of j2c and c2j)
    test_questions = [
        "68ae892edc8b36d3dcc08ac3",  # Job ID
        "68ae892edc8b36d3dcc08ac4",  # Candidate ID
        "מועמדים למשרה 68ae892edc8b36d3dcc08ac3",
        "משרות למועמד 68ae892edc8b36d3dcc08ac4",
        "68b4441c4bd286be22eecd65",  # Another job ID
        "68b444064bd286be22eecd5c",  # Another candidate ID
    ]
    
    tester = LoadTester()
    
    # Test 1: Light load
    print("=== Light Load Test (5 concurrent users, 10s) ===")
    stats = await tester.run_concurrent_requests(test_questions, num_concurrent=5, duration_seconds=10)
    print_stats(stats)
    
    # Test 2: Medium load
    print("\n=== Medium Load Test (15 concurrent users, 20s) ===")
    stats = await tester.run_concurrent_requests(test_questions, num_concurrent=15, duration_seconds=20)
    print_stats(stats)
    
    # Test 3: Heavy load
    print("\n=== Heavy Load Test (30 concurrent users, 15s) ===")
    stats = await tester.run_concurrent_requests(test_questions, num_concurrent=30, duration_seconds=15)
    print_stats(stats)


def print_stats(stats: Dict[str, Any]):
    """Print formatted statistics"""
    if "error" in stats:
        print(f"❌ Load test failed: {stats['error']}")
        return
    
    rt = stats["response_times"]
    
    print(f"📊 Total Requests: {stats['total_requests']}")
    print(f"✅ Successful: {stats['successful']} ({stats['success_rate']:.1f}%)")
    print(f"❌ Failed: {stats['failed']}")
    print(f"🔄 RPS: {stats['requests_per_second']:.1f}")
    print(f"⏱️  Response Times:")
    print(f"   Mean: {rt['mean']:.3f}s")
    print(f"   Median: {rt['median']:.3f}s")
    print(f"   P95: {rt['p95']:.3f}s")
    print(f"   P99: {rt['p99']:.3f}s")
    print(f"   Range: {rt['min']:.3f}s - {rt['max']:.3f}s")
    
    sv = stats["stream_vs_nonstream"]
    print(f"🔀 Stream vs Non-stream: {sv['stream']} vs {sv['nonstream']}")
    
    if stats['errors']:
        print(f"🚨 Errors:")
        for error, count in stats['errors'].items():
            print(f"   {error}: {count}")
    
    # Performance assessment
    if rt['p95'] < 1.5:
        print("🎉 Performance: EXCELLENT (P95 < 1.5s)")
    elif rt['p95'] < 3.0:
        print("✅ Performance: GOOD (P95 < 3.0s)")
    elif rt['p95'] < 5.0:
        print("⚠️ Performance: ACCEPTABLE (P95 < 5.0s)")
    else:
        print("🚨 Performance: POOR (P95 > 5.0s)")


if __name__ == "__main__":
    asyncio.run(main())
