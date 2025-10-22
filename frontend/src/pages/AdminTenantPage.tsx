import React, { useState } from 'react';
import { useParams } from 'react-router-dom';
import { apiUpload } from '../api';

interface UploadResponse {
  inserted_count: number;
  replaced_count: number;
}

const AdminTenantPage: React.FC = () => {
  const { tenantId } = useParams<{ tenantId: string }>();
  const [file, setFile] = useState<File | null>(null);
  const [status, setStatus] = useState<string>('');
  const [uploading, setUploading] = useState<boolean>(false);

  const handleUpload = async () => {
    if (!tenantId || !file) return;
    setUploading(true);
    setStatus('Uploading…');
    try {
      const response = await apiUpload<UploadResponse>(`/tenants/${tenantId}/jobs/upload`, file);
      setStatus(`Uploaded ${response.inserted_count} jobs (replaced ${response.replaced_count})`);
    } catch (err: any) {
      const message = err?.response?.data?.detail || err?.message || 'Upload failed';
      setStatus(typeof message === 'string' ? `Error: ${message}` : `Error: ${JSON.stringify(message)}`);
    } finally {
      setUploading(false);
    }
  };

  return (
    <div style={{ padding: 24, maxWidth: 720, margin: '0 auto' }}>
      <h2 style={{ marginBottom: 8 }}>Manage Tenant Jobs</h2>
      <p style={{ color: '#555', marginBottom: 24 }}>
        Upload a CSV to replace all existing jobs for this tenant. Use the official template to ensure consistent formatting.
      </p>
      <div style={{ marginBottom: 16 }}>
        <strong>Tenant ID:</strong> {tenantId}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12, marginBottom: 24 }}>
        <input
          type="file"
          accept=".csv"
          onChange={(e) => {
            const selected = e.target.files?.[0] ?? null;
            setFile(selected);
            setStatus('');
          }}
        />
        <div style={{ fontSize: 14, color: '#6b7280' }}>
          Need an example? Download the <a href="/job_upload_template.csv" target="_blank" rel="noreferrer">CSV template</a>.
        </div>
      </div>
      <button
        type="button"
        onClick={handleUpload}
        disabled={!file || uploading}
        style={{ padding: '10px 18px', borderRadius: 8, border: 'none', background: '#0f172a', color: '#fff', fontWeight: 600 }}
      >
        {uploading ? 'Uploading…' : 'Upload CSV'}
      </button>
      {status && <div style={{ marginTop: 16, color: status.startsWith('Error') ? '#c00' : '#047857' }}>{status}</div>}
    </div>
  );
};

export default AdminTenantPage;
