from scripts.ingest_agent import ingest_file, db, create_indexes
import os, tempfile, pathlib, json

def test_candidate_ingest_basic(tmp_path):
    sample = """Full Name: John Doe\nTitle: Senior Data Engineer\nCity: Tel Aviv\nExperience: ...\nSkills: Python, SQL, Data Modeling, ETL, Airflow, AWS\nEmail: john@example.com\nPhone: +1 555-123-4567\n"""
    f = tmp_path / "cv.txt"
    f.write_text(sample)
    doc = ingest_file(str(f), kind='candidate', force_llm=False)
    assert doc.get('share_id')
    assert 'skills' in doc
    skill_set = set(doc.get('skill_set') or [])
    assert 'python' in skill_set
    assert 'sql' in skill_set
    # PII scrub should replace raw email/phone in text_blob
    assert '[EMAIL]' in doc.get('text_blob','') or 'john@example.com' not in doc.get('text_blob','')
    assert '[PHONE]' in doc.get('text_blob','') or '+1 555-123-4567' not in doc.get('text_blob','')
    # city canonical stored
    assert 'city_canonical' in doc

def test_candidate_versioning(tmp_path):
    sample = "Title: DevOps Engineer\nCity: Haifa\nSkills: Docker, Kubernetes, Terraform, AWS, CI/CD\nEmail: me@company.com"
    f = tmp_path / "cv2.txt"
    f.write_text(sample)
    first = ingest_file(str(f), kind='candidate', force_llm=False)
    # modify file to trigger version snapshot
    f.write_text(sample + "\nAdded line new skill: Prometheus")
    second = ingest_file(str(f), kind='candidate', force_llm=False)
    assert second.get('_content_hash') != first.get('_content_hash')
    # versions collection has at least one snapshot
    assert db['candidates_versions'].count_documents({}) >= 1

def test_candidate_metrics_meta(tmp_path):
    sample = "Title: QA Engineer\nSkills: Testing, Selenium, Python, Automation\nEmail: qa@test.com"
    f = tmp_path / "cv3.txt"
    f.write_text(sample)
    ingest_file(str(f), kind='candidate', force_llm=False)
    meta = db['_meta'].find_one({'key':'last_candidate_ingest_metrics'})
    assert meta and 'value' in meta and 'skill_count' in meta['value']

def test_candidate_esco_and_synthetic_skills(tmp_path):
    sample = (
        "Full Name: Jane Roe\n"
        "Title: Data Analyst\n"
        "City: Jerusalem\n"
        "Skills: SQL, Python, Power BI; Data Visualization; ETL Processes\n"
        "Email: jane@example.com\n"
    )
    f = tmp_path / "cv_esco.txt"
    f.write_text(sample)
    doc = ingest_file(str(f), kind='candidate', force_llm=False)
    # Canonicalized skill_set should use lowercase underscores
    skill_set = set(doc.get('skill_set') or [])
    assert any(s in skill_set for s in ["sql","python"])
    # esco_skills present and aligned to names
    esco = doc.get('esco_skills') or []
    assert isinstance(esco, list) and len(esco) >= 2
    assert all(isinstance(e, dict) and 'name' in e for e in esco)
    # synthetic_skills list exists (may be empty, but pipeline can add)
    assert 'synthetic_skills' in doc

def test_candidate_city_parsing_variants(tmp_path):
    # English City label
    t1 = "Full Name: A\nCity: Haifa\nEmail: a@a.com\n"
    f1 = tmp_path / "cv_city_en.txt"
    f1.write_text(t1)
    d1 = ingest_file(str(f1), kind='candidate', force_llm=False)
    assert d1.get('city')
    assert d1.get('city_canonical')
    # English Location label
    t2 = "Title: QA\nLocation: Tel Aviv\n"
    f2 = tmp_path / "cv_city_loc.txt"
    f2.write_text(t2)
    d2 = ingest_file(str(f2), kind='candidate', force_llm=False)
    assert d2.get('city')
    # Hebrew labels עיר / מיקום
    t3 = "שם: ב\nעיר: ירושלים\nדוא""ל: b@b.com\n"
    f3 = tmp_path / "cv_city_he.txt"
    f3.write_text(t3)
    d3 = ingest_file(str(f3), kind='candidate', force_llm=False)
    assert d3.get('city')
    # Promote from contact.city if only there
    t4 = "contact:\n  city: Beersheba\n"
    f4 = tmp_path / "cv_city_contact.txt"
    f4.write_text(t4)
    d4 = ingest_file(str(f4), kind='candidate', force_llm=False)
    assert d4.get('city')
