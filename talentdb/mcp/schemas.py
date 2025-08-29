from __future__ import annotations

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class SearchCandidatesInput(BaseModel):
    skills: List[str] = Field(default_factory=list)
    city: Optional[str] = None
    min_experience: Optional[int] = None
    k: int = 10


class CandidateSummary(BaseModel):
    id: str
    title: Optional[str] = None
    city: Optional[str] = None
    skills: List[str] = Field(default_factory=list)
    experience_years: Optional[float] = None


class SearchCandidatesOutput(BaseModel):
    candidates: List[CandidateSummary] = Field(default_factory=list)


class SearchJobsInput(BaseModel):
    skills: List[str] = Field(default_factory=list)
    city: Optional[str] = None
    seniority: Optional[str] = None
    k: int = 10


class JobSummary(BaseModel):
    id: str
    title: Optional[str] = None
    city: Optional[str] = None
    must_have: List[str] = Field(default_factory=list)
    nice_to_have: List[str] = Field(default_factory=list)


class SearchJobsOutput(BaseModel):
    jobs: List[JobSummary] = Field(default_factory=list)


class MatchJobToCandidatesInput(BaseModel):
    job_id: str
    k: int = 10


class MatchCandidateToJobsInput(BaseModel):
    candidate_id: str
    k: int = 10


class MatchRow(BaseModel):
    score: float = 0.0
    candidate_id: Optional[str] = None
    job_id: Optional[str] = None
    title: Optional[str] = None
    city: Optional[str] = None
    breakdown: Dict[str, float] = Field(default_factory=dict)
    counters: Dict[str, Any] = Field(default_factory=dict)


class MatchListOutput(BaseModel):
    rows: List[MatchRow] = Field(default_factory=list)


class GetMatchAnalysisInput(BaseModel):
    candidate_id: str
    job_id: str


class GetMatchAnalysisOutput(BaseModel):
    row: MatchRow


class GetCandidateProfileInput(BaseModel):
    candidate_id: str


class CandidateProfile(BaseModel):
    id: str
    title: Optional[str] = None
    city: Optional[str] = None
    skills_must: List[str] = Field(default_factory=list)
    skills_nice: List[str] = Field(default_factory=list)


class GetCandidateProfileOutput(BaseModel):
    candidate: CandidateProfile


class GetJobDetailsInput(BaseModel):
    job_id: str


class JobDetails(BaseModel):
    id: str
    title: Optional[str] = None
    city: Optional[str] = None
    must_have: List[str] = Field(default_factory=list)
    nice_to_have: List[str] = Field(default_factory=list)


class GetJobDetailsOutput(BaseModel):
    job: JobDetails


class CreateOutreachMessageInput(BaseModel):
    candidate_id: str
    job_ids: List[str] = Field(default_factory=list)
    tone: Optional[str] = None


class OutreachMessage(BaseModel):
    job_id: str
    whatsapp: str


class CreateOutreachMessageOutput(BaseModel):
    messages: List[OutreachMessage] = Field(default_factory=list)


class AddDiscussionNoteInput(BaseModel):
    target_type: str
    target_id: str
    text: str


class AddDiscussionNoteOutput(BaseModel):
    ok: bool
    id: Optional[str] = None


class GetAnalyticsSummaryInput(BaseModel):
    window_days: int = 7


class GetAnalyticsSummaryOutput(BaseModel):
    candidates: int
    jobs: int
    matches: int
    top_skills: List[str] = Field(default_factory=list)
