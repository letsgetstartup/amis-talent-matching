#!/usr/bin/env python3
"""
Comprehensive Copilot Chat Testing Script
Tests the recruiter copilot chat functionality with various queries
"""

import requests
import json
import time
import logging
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CopilotChatTester:
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        self.base_url = base_url
        self.session = requests.Session()

    def test_chat_query(self, question: str, use_streaming: bool = True) -> Dict[str, Any]:
        """Test a single chat query"""
        endpoint = f"{self.base_url}/chat/query"
        if use_streaming:
            endpoint += "?stream=1"

        payload = {"question": question}

        logger.info(f"Testing query: {question}")
        logger.info(f"Endpoint: {endpoint}")

        try:
            start_time = time.time()
            response = self.session.post(
                endpoint,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            end_time = time.time()

            result = {
                "question": question,
                "status_code": response.status_code,
                "response_time": round(end_time - start_time, 2),
                "success": response.status_code == 200
            }

            if use_streaming:
                # Handle streaming response
                content = response.text.strip()
                if content:
                    lines = content.split('\n')
                    events = []
                    for line in lines:
                        if line.strip():
                            try:
                                event = json.loads(line)
                                events.append(event)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse event: {line}, error: {e}")

                    result["events"] = events
                    result["event_count"] = len(events)
                    result["has_text_delta"] = any(e.get("type") == "text_delta" for e in events)
                    result["has_assistant_ui"] = any(e.get("type") == "assistant_ui" for e in events)
                    result["has_done"] = any(e.get("type") == "done" for e in events)
                else:
                    result["events"] = []
                    result["event_count"] = 0
            else:
                # Handle regular JSON response
                try:
                    data = response.json()
                    result["response_data"] = data
                    result["has_answer"] = "answer" in data or "message" in data
                    result["has_actions"] = "actions" in data and len(data.get("actions", [])) > 0
                except json.JSONDecodeError:
                    result["response_text"] = response.text[:500]  # First 500 chars

            logger.info(f"Query completed in {result['response_time']}s with status {result['status_code']}")
            return result

        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            return {
                "question": question,
                "success": False,
                "error": str(e)
            }

    def run_comprehensive_tests(self) -> List[Dict[str, Any]]:
        """Run comprehensive tests with various query types"""

        test_queries = [
            # Basic queries
            "Hello, what can you help me with?",
            "Show me system status",
            "What data do you have?",

            # Job-related queries
            "Show me all jobs",
            "Find Python developer jobs",
            "Show me jobs in Tel Aviv",
            "Display top 10 highest paying jobs",

            # Candidate-related queries
            "Show me all candidates",
            "Find candidates with Python skills",
            "Show candidates from Tel Aviv",
            "Display top candidates by experience",

            # Match-related queries
            "Show me job-candidate matches",
            "Find best matches for Python jobs",
            "Show matches with score above 80%",
            "Display matches for Tel Aviv",

            # Complex queries
            "Show top 5 matches for Python developer in Tel Aviv sorted by score",
            "Find candidates with 5+ years experience for senior positions",
            "Show me jobs that match candidates with React skills",
            "Display statistics about our talent pool",

            # Edge cases
            "Show me something that doesn't exist",
            "Find jobs with impossible requirements",
            "",  # Empty query
            "This is a very long query that should test the system's ability to handle lengthy input and see if it can process complex requests without timing out or failing " * 10
        ]

        results = []
        logger.info(f"Starting comprehensive test with {len(test_queries)} queries")

        for i, query in enumerate(test_queries, 1):
            logger.info(f"Test {i}/{len(test_queries)}: {query[:50]}{'...' if len(query) > 50 else ''}")

            # Test both streaming and non-streaming
            streaming_result = self.test_chat_query(query, use_streaming=True)
            non_streaming_result = self.test_chat_query(query, use_streaming=False)

            combined_result = {
                "test_number": i,
                "query": query,
                "streaming": streaming_result,
                "non_streaming": non_streaming_result,
                "both_successful": streaming_result.get("success", False) and non_streaming_result.get("success", False)
            }

            results.append(combined_result)

            # Small delay between tests
            time.sleep(0.5)

        return results

    def generate_report(self, results: List[Dict[str, Any]]) -> str:
        """Generate a comprehensive test report"""

        total_tests = len(results)
        successful_tests = sum(1 for r in results if r["both_successful"])
        success_rate = (successful_tests / total_tests) * 100 if total_tests > 0 else 0

        report = f"""
# Copilot Chat Testing Report
Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- Total Tests: {total_tests}
- Successful Tests: {successful_tests}
- Success Rate: {success_rate:.1f}%

## Test Results

"""

        for result in results:
            status = "✅ PASS" if result["both_successful"] else "❌ FAIL"
            streaming_time = result["streaming"].get("response_time", "N/A")
            non_streaming_time = result["non_streaming"].get("response_time", "N/A")

            report += f"""
### Test {result["test_number"]}: {status}
**Query:** {result["query"][:100]}{"..." if len(result["query"]) > 100 else ""}

**Streaming Response:**
- Status: {result["streaming"].get("status_code", "N/A")}
- Time: {streaming_time}s
- Events: {result["streaming"].get("event_count", 0)}
- Has Text: {result["streaming"].get("has_text_delta", False)}
- Has UI: {result["streaming"].get("has_assistant_ui", False)}

**Non-Streaming Response:**
- Status: {result["non_streaming"].get("status_code", "N/A")}
- Time: {non_streaming_time}s
- Has Answer: {result["non_streaming"].get("has_answer", False)}
- Has Actions: {result["non_streaming"].get("has_actions", False)}

"""

        # Add performance analysis
        streaming_times = [r["streaming"].get("response_time", 0) for r in results if r["streaming"].get("success")]
        non_streaming_times = [r["non_streaming"].get("response_time", 0) for r in results if r["non_streaming"].get("success")]

        if streaming_times:
            avg_streaming_time = sum(streaming_times) / len(streaming_times)
            report += f"\n## Performance Analysis\n"
            report += f"- Average Streaming Response Time: {avg_streaming_time:.2f}s\n"
            report += f"- Fastest Streaming Response: {min(streaming_times):.2f}s\n"
            report += f"- Slowest Streaming Response: {max(streaming_times):.2f}s\n"

        if non_streaming_times:
            avg_non_streaming_time = sum(non_streaming_times) / len(non_streaming_times)
            report += f"- Average Non-Streaming Response Time: {avg_non_streaming_time:.2f}s\n"
            report += f"- Fastest Non-Streaming Response: {min(non_streaming_times):.2f}s\n"
            report += f"- Slowest Non-Streaming Response: {max(non_streaming_times):.2f}s\n"

        return report

def main():
    """Main test execution"""
    print("🚀 Starting Copilot Chat Testing...")
    print("Make sure the server is running at http://127.0.0.1:8000")

    tester = CopilotChatTester()

    # Quick connectivity test
    print("\n🔍 Testing connectivity...")
    quick_test = tester.test_chat_query("Hello", use_streaming=False)
    if not quick_test.get("success"):
        print("❌ Server not responding. Please start the server first.")
        return

    print("✅ Server is responding!")

    # Run comprehensive tests
    print("\n🧪 Running comprehensive tests...")
    results = tester.run_comprehensive_tests()

    # Generate and save report
    report = tester.generate_report(results)
    report_file = "/Users/avirammizrahi/Desktop/amis/copilot_test_report.md"

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"\n📊 Test report saved to: {report_file}")
    print(report)

    # Summary
    successful = sum(1 for r in results if r["both_successful"])
    total = len(results)
    print(f"\n🎯 Final Results: {successful}/{total} tests passed ({successful/total*100:.1f}%)")

if __name__ == "__main__":
    main()
