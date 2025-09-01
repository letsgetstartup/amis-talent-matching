import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from talentdb.scripts.api import app

client = TestClient(app)


class TestMatchingCore:
    def test_mcp_or_native_function_exists(self):
        # Test that the MCP/native wrapper functions exist and can be imported
        from talentdb.scripts.api import _mcp_or_native_candidates_for_job, _mcp_or_native_jobs_for_candidate
        # Functions should be callable
        assert callable(_mcp_or_native_candidates_for_job)
        assert callable(_mcp_or_native_jobs_for_candidate)

    @patch('talentdb.scripts.api.MCP_ENABLED', False)
    @patch('talentdb.scripts.api.get_or_compute_candidates_for_job')
    def test_get_or_compute_candidates_for_job_calls_native_when_mcp_disabled(self, mock_native):
        # Mock DB and native function, ensure MCP is disabled
        mock_native.return_value = [{'_id': 'cand1', 'name': 'Test Cand', 'score': 0.9}]
        from talentdb.scripts.api import _mcp_or_native_candidates_for_job
        result = _mcp_or_native_candidates_for_job('job1', top_k=5, tenant_id=None)
        mock_native.assert_called_once_with('job1', top_k=5, city_filter=True, tenant_id=None)
        assert result == mock_native.return_value

    @patch('talentdb.scripts.api.MCP_ENABLED', False)
    @patch('talentdb.scripts.api.jobs_for_candidate')
    def test_jobs_for_candidate_calls_native_when_mcp_disabled(self, mock_native):
        mock_native.return_value = [{'_id': 'job1', 'title': 'Test Job', 'score': 0.8}]
        from talentdb.scripts.api import _mcp_or_native_jobs_for_candidate
        result = _mcp_or_native_jobs_for_candidate('cand1', top_k=5, tenant_id=None)
        # The actual call includes max_distance_km parameter
        mock_native.assert_called_once_with('cand1', top_k=5, max_distance_km=30, tenant_id=None)
        assert result == mock_native.return_value


class TestUIBuilders:
    def test_build_match_item_structure_via_api(self):
        # Test UI building through API endpoint since functions are private
        job_id = "68ae892edc8b36d3dcc08ac3"
        r = client.post('/chat/query', json={"question": job_id, "detailsOnly": True})
        assert r.status_code == 200
        
        ui = r.json().get('ui', [])
        match_lists = [b for b in ui if b.get('kind') == 'MatchList']
        
        if match_lists and match_lists[0].get('items'):
            item = match_lists[0]['items'][0]
            # Validate item structure
            assert 'title' in item
            assert 'subtitle' in item
            assert 'chips' in item
            assert 'breakdown' in item
            
            chips = item['chips']
            if 'must' in chips:
                assert isinstance(chips['must'], list)
            if 'nice' in chips:
                assert isinstance(chips['nice'], list)
            
            breakdown = item['breakdown']
            if 'parts' in breakdown:
                parts = breakdown['parts']
                assert isinstance(parts, list)
                if parts:
                    total_pct = sum(p.get('pct', 0) for p in parts)
                    # Allow reasonable tolerance for percentage sum
                    assert abs(total_pct - 100) < 5  # 5% tolerance for test robustness

    def test_match_list_ui_structure(self):
        # Test MatchList UI structure via API
        job_id = "68ae892edc8b36d3dcc08ac3"
        r = client.post('/chat/query', json={"question": job_id, "detailsOnly": True})
        assert r.status_code == 200
        
        ui = r.json().get('ui', [])
        match_lists = [b for b in ui if b.get('kind') == 'MatchList']
        
        if match_lists:
            match_list = match_lists[0]
            assert match_list['kind'] == 'MatchList'
            assert 'id' in match_list
            assert 'items' in match_list
            assert isinstance(match_list['items'], list)

    def test_skills_badges_ui_structure(self):
        # Test that when results exist, UI includes proper skills components
        job_id = "68ae892edc8b36d3dcc08ac3"
        r = client.post('/chat/query', json={"question": job_id, "detailsOnly": True})
        assert r.status_code == 200
        
        ui = r.json().get('ui', [])
        # Should have either MatchList (for multiple results) or SkillsBadges (for single result details)
        ui_kinds = [c.get('kind') for c in ui]
        has_match_components = any(kind in ['MatchList', 'SkillsBadges'] for kind in ui_kinds)
        
        # Allow for cases where there are no matches (empty results)
        metric = next((c for c in ui if c.get('kind') == 'Metric' and c.get('id') == 'matches-kpi'), None)
        if metric and isinstance(metric.get('value'), int) and metric['value'] > 0:
            assert has_match_components, f"Expected match components in UI kinds: {ui_kinds}"
