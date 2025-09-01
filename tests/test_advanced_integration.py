import pytest
import time
from fastapi.testclient import TestClient
from talentdb.scripts.api import app

client = TestClient(app)


class TestStreamingParity:
    """Test that streaming and non-streaming chat behave identically at the envelope level"""
    
    def test_j2c_stream_vs_nonstream_ui_parity(self):
        # Job-to-candidates: stream and non-stream should have same final UI structure
        job_id = "68ae892edc8b36d3dcc08ac3"
        
        # Non-stream
        r_nonstream = client.post('/chat/query', json={"question": job_id, "detailsOnly": True})
        assert r_nonstream.status_code == 200
        nonstream_ui = r_nonstream.json().get('ui', [])
        
        # Stream
        r_stream = client.post('/chat/query?stream=1', json={"question": job_id, "detailsOnly": True})
        assert r_stream.status_code == 200
        
        # Parse stream to find final assistant_ui envelope
        lines = [ln for ln in r_stream.text.splitlines() if ln.strip()]
        stream_ui = None
        for ln in reversed(lines):
            try:
                import json
                obj = json.loads(ln)
                if obj.get('type') == 'assistant_ui':
                    stream_ui = obj.get('ui', [])
                    break
            except Exception:
                continue
        
        assert stream_ui is not None, "Stream should produce assistant_ui envelope"
        
        # Compare UI structure (kinds and basic properties)
        nonstream_kinds = [c.get('kind') for c in nonstream_ui]
        stream_kinds = [c.get('kind') for c in stream_ui]
        
        # Should have same UI component types
        assert set(nonstream_kinds) == set(stream_kinds), f"UI kinds differ: {nonstream_kinds} vs {stream_kinds}"
        
        # Both should not have Table in detailsOnly mode
        assert 'Table' not in nonstream_kinds
        assert 'Table' not in stream_kinds

    def test_c2j_stream_vs_nonstream_ui_parity(self):
        # Candidate-to-jobs: stream and non-stream should have same final UI structure
        cand_id = "68ae892edc8b36d3dcc08ac4"
        
        # Non-stream
        r_nonstream = client.post('/chat/query', json={"question": cand_id, "detailsOnly": True})
        assert r_nonstream.status_code == 200
        nonstream_ui = r_nonstream.json().get('ui', [])
        
        # Stream
        r_stream = client.post('/chat/query?stream=1', json={"question": cand_id, "detailsOnly": True})
        assert r_stream.status_code == 200
        
        # Parse stream to find final assistant_ui envelope
        lines = [ln for ln in r_stream.text.splitlines() if ln.strip()]
        stream_ui = None
        for ln in reversed(lines):
            try:
                import json
                obj = json.loads(ln)
                if obj.get('type') == 'assistant_ui':
                    stream_ui = obj.get('ui', [])
                    break
            except Exception:
                continue
        
        assert stream_ui is not None, "Stream should produce assistant_ui envelope"
        
        # Compare UI structure
        nonstream_kinds = [c.get('kind') for c in nonstream_ui]
        stream_kinds = [c.get('kind') for c in stream_ui]
        
        assert set(nonstream_kinds) == set(stream_kinds), f"UI kinds differ: {nonstream_kinds} vs {stream_kinds}"
        
        # Both should not have Table in detailsOnly mode
        assert 'Table' not in nonstream_kinds
        assert 'Table' not in stream_kinds


class TestRealDataIntegrity:
    """Advanced tests for real data integrity"""
    
    def test_no_placeholder_chips_in_results(self):
        # Find a job that actually has matches and verify chips are not placeholders
        from talentdb.scripts.ingest_agent import db
        jobs = list(db['jobs'].find({}, {'_id': 1}).limit(10))
        
        for job in jobs:
            job_id = str(job['_id'])
            r = client.post('/chat/query', json={"question": job_id, "detailsOnly": True})
            if r.status_code != 200:
                continue
                
            ui = r.json().get('ui', [])
            match_lists = [b for b in ui if b.get('kind') == 'MatchList']
            
            if match_lists and match_lists[0].get('items'):
                # Found results, verify chips are real
                for item in match_lists[0]['items']:
                    chips = item.get('chips', {})
                    must_skills = chips.get('must', [])
                    nice_skills = chips.get('nice', [])
                    
                    # If chips exist, they should not be placeholder values
                    if must_skills:
                        assert not any(skill == "—" for skill in must_skills), "Found placeholder in must skills"
                    if nice_skills:
                        assert not any(skill == "—" for skill in nice_skills), "Found placeholder in nice skills"
                break  # Found a job with results, test passed

    def test_breakdown_components_realistic(self):
        # Breakdown components should have realistic labels and percentages
        job_id = "68ae892edc8b36d3dcc08ac3"
        r = client.post('/chat/query', json={"question": job_id, "detailsOnly": True})
        assert r.status_code == 200
        
        ui = r.json().get('ui', [])
        match_lists = [b for b in ui if b.get('kind') == 'MatchList']
        
        if match_lists and match_lists[0].get('items'):
            for item in match_lists[0]['items']:
                breakdown = item.get('breakdown', {})
                parts = breakdown.get('parts', [])
                
                if parts:
                    # Each part should have realistic data
                    for part in parts:
                        label = part.get('label', '')
                        pct = part.get('pct', 0)
                        
                        # Label should be meaningful
                        assert len(label) > 0, "Breakdown part should have non-empty label"
                        assert not label.startswith('undefined'), "Label should not be 'undefined'"
                        
                        # Percentage should be reasonable
                        assert 0 <= pct <= 100, f"Percentage {pct} should be between 0-100"


class TestErrorHandling:
    """Test error handling and edge cases"""
    
    def test_malformed_request_handling(self):
        # Test various malformed requests
        test_cases = [
            {"question": None, "detailsOnly": True},
            {"question": 123, "detailsOnly": True},
            {"question": "test", "detailsOnly": "invalid"},
            {},  # missing question
        ]
        
        for test_case in test_cases:
            r = client.post('/chat/query', json=test_case)
            # Should either handle gracefully (200) or return proper error (4xx)
            assert r.status_code in [200, 400, 422], f"Failed for case: {test_case}"

    def test_extremely_long_question_handling(self):
        # Test with very long question
        long_question = "מועמדים למשרה " + "x" * 10000  # Very long question
        r = client.post('/chat/query', json={"question": long_question, "detailsOnly": True})
        # Should handle gracefully without crashing
        assert r.status_code in [200, 400, 413, 422]  # Various acceptable error codes

    def test_special_characters_in_question(self):
        # Test with special characters and edge cases
        special_questions = [
            "מועמדים למשרה <script>alert('xss')</script>",
            "test'; DROP TABLE jobs; --",
            "מועמדים\x00\x01\x02למשרה",
            "🚀 מועמדים למשרה 💻",
        ]
        
        for question in special_questions:
            r = client.post('/chat/query', json={"question": question, "detailsOnly": True})
            # Should handle without crashing
            assert r.status_code in [200, 400, 422], f"Failed for question: {question}"


class TestUIConsistency:
    """Test UI component consistency"""
    
    def test_matchlist_items_have_required_fields(self):
        # MatchList items should have consistent structure
        job_id = "68ae892edc8b36d3dcc08ac3"
        r = client.post('/chat/query', json={"question": job_id, "detailsOnly": True})
        assert r.status_code == 200
        
        ui = r.json().get('ui', [])
        match_lists = [b for b in ui if b.get('kind') == 'MatchList']
        
        if match_lists and match_lists[0].get('items'):
            for item in match_lists[0]['items']:
                # Each item should have required fields
                assert 'title' in item, "MatchList item should have title"
                assert 'subtitle' in item, "MatchList item should have subtitle"
                assert 'chips' in item, "MatchList item should have chips"
                assert 'breakdown' in item, "MatchList item should have breakdown"
                
                # Chips should have expected structure
                chips = item['chips']
                assert 'must' in chips or 'nice' in chips or 'counters' in chips, "Chips should have some content"
                
                # Breakdown should have expected structure
                breakdown = item['breakdown']
                if 'parts' in breakdown:
                    assert isinstance(breakdown['parts'], list), "Breakdown parts should be list"

    def test_metric_components_have_valid_values(self):
        # Metric components should have valid values
        job_id = "68ae892edc8b36d3dcc08ac3"
        r = client.post('/chat/query', json={"question": job_id, "detailsOnly": True})
        assert r.status_code == 200
        
        ui = r.json().get('ui', [])
        metrics = [b for b in ui if b.get('kind') == 'Metric']
        
        for metric in metrics:
            # Should have valid metric structure
            assert 'id' in metric, "Metric should have id"
            assert 'value' in metric, "Metric should have value"
            
            # Value should be reasonable
            value = metric['value']
            if isinstance(value, int):
                assert value >= 0, "Metric value should be non-negative"
                assert value < 1000000, "Metric value should be reasonable"
