import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { PortalResumeUploadResponse } from '../api';
import { uploadPortalCandidateResume } from '../api';

export interface CVUploadModalProps {
  isOpen: boolean;
  portalSlug: string;
  sessionId: string;
  conversationId?: string | null;
  onClose: () => void;
  onUploadComplete: (result: PortalResumeUploadResponse) => void;
}

const readableSize = (size: number): string => {
  if (!Number.isFinite(size) || size <= 0) {
    return '0 KB';
  }
  const kb = size / 1024;
  if (kb < 1024) {
    return `${kb.toFixed(1)} KB`;
  }
  const mb = kb / 1024;
  return `${mb.toFixed(1)} MB`;
};

const ACCEPTED_MIME = ['application/pdf', 'application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'text/plain'];
const ACCEPTED_EXTENSIONS = ['.pdf', '.doc', '.docx', '.txt'];

const MAX_SIZE_BYTES = 10 * 1024 * 1024; // 10MB

type UploadStage = 'idle' | 'uploading' | 'success';

const CVUploadModal: React.FC<CVUploadModalProps> = ({
  isOpen,
  portalSlug,
  sessionId,
  conversationId,
  onClose,
  onUploadComplete,
}) => {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [stage, setStage] = useState<UploadStage>('idle');
  const [isDragging, setIsDragging] = useState(false);
  const [result, setResult] = useState<PortalResumeUploadResponse | null>(null);
  const [uploading, setUploading] = useState(false);
  const inputRef = useRef<HTMLInputElement | null>(null);

  const resetState = useCallback(() => {
    setSelectedFile(null);
    setError(null);
    setStage('idle');
    setIsDragging(false);
    setResult(null);
    setUploading(false);
  }, []);

  useEffect(() => {
    if (isOpen) {
      resetState();
    }
  }, [isOpen, resetState]);

  useEffect(() => {
    if (!isOpen) {
      return;
    }
    const handleKey = (evt: KeyboardEvent) => {
      if (evt.key === 'Escape') {
        evt.preventDefault();
        onClose();
      }
    };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [isOpen, onClose]);

  const acceptAttr = useMemo(() => ACCEPTED_EXTENSIONS.join(','), []);

  const handleFileSelection = useCallback((file: File | null) => {
    if (!file) {
      setSelectedFile(null);
      return;
    }
    const ext = file.name?.slice(file.name.lastIndexOf('.')).toLowerCase();
    const mimeValid = ACCEPTED_MIME.includes(file.type);
    const extValid = ext ? ACCEPTED_EXTENSIONS.includes(ext) : false;
    if (!mimeValid && !extValid) {
      setError('Please choose a PDF, DOCX, DOC, or TXT file.');
      setSelectedFile(null);
      return;
    }
    if (file.size > MAX_SIZE_BYTES) {
      setError('File is too large. Maximum size is 10 MB.');
      setSelectedFile(null);
      return;
    }
    setError(null);
    setSelectedFile(file);
  }, []);

  const onInputChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0] || null;
    handleFileSelection(file);
  }, [handleFileSelection]);

  const onDrop = useCallback((event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    setIsDragging(false);
    const file = event.dataTransfer?.files?.[0];
    if (file) {
      handleFileSelection(file);
    }
  }, [handleFileSelection]);

  const onDragOver = useCallback((event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    if (!isDragging) {
      setIsDragging(true);
    }
  }, [isDragging]);

  const onDragLeave = useCallback((event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    setIsDragging(false);
  }, []);

  const handleUpload = useCallback(async () => {
    if (!selectedFile) {
      setError('Choose a file to upload.');
      return;
    }
    setUploading(true);
    setStage('uploading');
    setError(null);
    try {
      const response = await uploadPortalCandidateResume(portalSlug, selectedFile, {
        sessionId,
        conversationId,
      });
      setResult(response);
      setStage('success');
      onUploadComplete(response);
    } catch (err: any) {
      const detail = err?.response?.data?.detail || err?.message || 'Failed to upload CV.';
      setError(typeof detail === 'string' ? detail : 'Upload failed. Please try again.');
      setStage('idle');
    } finally {
      setUploading(false);
    }
  }, [conversationId, onUploadComplete, portalSlug, selectedFile, sessionId]);

  const handleRetry = useCallback(() => {
    setResult(null);
    setStage('idle');
    setSelectedFile(null);
  }, []);

  if (!isOpen) {
    return null;
  }

  const hasResult = Boolean(result);

  return (
    <div className="cv-upload-modal__backdrop" role="presentation">
      <div className="cv-upload-modal" role="dialog" aria-modal="true" aria-label="Upload your CV for personalized matches">
        <header className="cv-upload-modal__header">
          <div>
            <h2>Upload your CV</h2>
            <p>We'll analyze it to recommend roles that match your profile.</p>
          </div>
          <button type="button" className="cv-upload-modal__close" onClick={onClose} aria-label="Close upload dialog">
            ×
          </button>
        </header>

        <div className="cv-upload-modal__body">
          {!hasResult ? (
            <>
              <div
                className={`cv-upload-modal__dropzone${isDragging ? ' is-dragging' : ''}${selectedFile ? ' has-file' : ''}`}
                onDrop={onDrop}
                onDragOver={onDragOver}
                onDragLeave={onDragLeave}
                role="button"
                tabIndex={0}
                onClick={() => inputRef.current?.click()}
                onKeyDown={(evt) => {
                  if (evt.key === 'Enter' || evt.key === ' ') {
                    evt.preventDefault();
                    inputRef.current?.click();
                  }
                }}
              >
                <input
                  ref={inputRef}
                  type="file"
                  accept={acceptAttr}
                  className="visually-hidden"
                  onChange={onInputChange}
                />
                {selectedFile ? (
                  <div className="cv-upload-modal__file-preview">
                    <strong>{selectedFile.name}</strong>
                    <span>{readableSize(selectedFile.size)}</span>
                    <button type="button" onClick={(evt) => { evt.stopPropagation(); setSelectedFile(null); }}>
                      Choose another file
                    </button>
                  </div>
                ) : (
                  <div className="cv-upload-modal__prompt">
                    <span className="cv-upload-modal__icon" aria-hidden="true">📁</span>
                    <p>Drag & drop your CV here or click to browse</p>
                    <small>Accepted formats: PDF, DOCX, DOC, TXT (max 10MB)</small>
                  </div>
                )}
              </div>

              {error ? <div className="cv-upload-modal__error" role="alert">{error}</div> : null}

              <div className="cv-upload-modal__actions">
                <button type="button" className="cv-upload-modal__secondary" onClick={onClose} disabled={uploading}>
                  Cancel
                </button>
                <button type="button" className="cv-upload-modal__primary" onClick={handleUpload} disabled={!selectedFile || uploading}>
                  {stage === 'uploading' ? 'Uploading…' : 'Upload CV'}
                </button>
              </div>
            </>
          ) : (
            <div className="cv-upload-modal__success">
              <div className="cv-upload-modal__success-hero">
                <span aria-hidden="true">✓</span>
                <h3>CV uploaded successfully</h3>
                <p>We found {result?.matches?.length || 0} tailored job suggestions for you.</p>
              </div>

              {result?.profile ? (
                <section className="cv-upload-modal__profile" aria-label="Profile summary">
                  <h4>Profile highlights</h4>
                  <ul>
                    {result.profile.full_name ? <li><strong>Name:</strong> {result.profile.full_name}</li> : null}
                    {result.profile.title ? <li><strong>Role:</strong> {result.profile.title}</li> : null}
                    {result.profile.city ? <li><strong>Location:</strong> {result.profile.city}</li> : null}
                    {result.profile.top_skills?.length ? (
                      <li><strong>Top skills:</strong> {result.profile.top_skills.slice(0, 5).join(', ')}</li>
                    ) : null}
                  </ul>
                </section>
              ) : null}

              {result?.matches?.length ? (
                <section className="cv-upload-modal__matches" aria-label="Top matches">
                  <h4>Suggested roles</h4>
                  <ol>
                    {result.matches.slice(0, 5).map((match) => (
                      <li key={match.job_id || match.title}>
                        <div className="cv-upload-modal__match-title">{match.title || 'Open role'}</div>
                        <div className="cv-upload-modal__match-meta">
                          {match.company_name ? <span>{match.company_name}</span> : null}
                          {match.location ? <span>{match.location}</span> : null}
                          {typeof match.score === 'number' ? <span>{Math.round(match.score)}% match</span> : null}
                        </div>
                        {match.match_reason ? <p className="cv-upload-modal__match-reason">{match.match_reason}</p> : null}
                      </li>
                    ))}
                  </ol>
                </section>
              ) : null}

              <div className="cv-upload-modal__actions">
                <button type="button" className="cv-upload-modal__secondary" onClick={handleRetry}>
                  Upload another CV
                </button>
                <button type="button" className="cv-upload-modal__primary" onClick={onClose}>
                  Close
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default CVUploadModal;
