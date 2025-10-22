import React, { useState } from 'react';
import { apiPost } from '../api';

interface RegistrationForm {
  name: string;
  admin_name: string;
  admin_email: string;
  admin_password: string;
  portcos: string;
  ats: string;
  focus: string;
}

interface TenantResult {
  tenant_id: string;
  slug: string;
  name: string;
  portal_url?: string;
}

const RegistrationPage: React.FC = () => {
  const [step, setStep] = useState<number>(1);
  const [form, setForm] = useState<RegistrationForm>({
    name: '',
    admin_name: '',
    admin_email: '',
    admin_password: '',
    portcos: '',
    ats: '',
    focus: '',
  });
  const [result, setResult] = useState<TenantResult | null>(null);
  const [submitting, setSubmitting] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;
    setForm((prev) => ({ ...prev, [name]: value }));
  };

  const handleNext = () => setStep((prev) => Math.min(prev + 1, 2));
  const handlePrev = () => setStep((prev) => Math.max(prev - 1, 1));

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const payload = {
        name: form.name.trim(),
        admin_name: form.admin_name.trim(),
        admin_email: form.admin_email.trim(),
        admin_password: form.admin_password,
      };
      const tenant = await apiPost<TenantResult>('/tenants', payload);
      setResult(tenant);
    } catch (err: any) {
      const message = err?.response?.data?.detail || err?.message || 'Registration failed';
      setError(typeof message === 'string' ? message : JSON.stringify(message));
    } finally {
      setSubmitting(false);
    }
  };

  if (result) {
    const origin = typeof window !== 'undefined' ? window.location.origin : '';
    const slug = result.slug;
    const portalPath = result.portal_url || `/portal/${slug}`;
    return (
      <div style={{ padding: 24, maxWidth: 720, margin: '0 auto' }}>
        <h2 style={{ marginBottom: 16 }}>Registration complete</h2>
        <p>Your VC portal is live and ready to use.</p>
        <p>
          <strong>Portal Link:</strong>{' '}
          <a href={portalPath.startsWith('http') ? portalPath : `${origin}${portalPath}`} target="_blank" rel="noreferrer">
            {portalPath.startsWith('http') ? portalPath : `${origin}${portalPath}`}
          </a>
        </p>
        <p>
          <strong>Tenant ID:</strong> {result.tenant_id}
        </p>
        <p style={{ marginTop: 24 }}>Save this URL to share with candidates or embed on your website.</p>
      </div>
    );
  }

  return (
    <div style={{ maxWidth: 680, margin: '0 auto', padding: 24 }}>
      <h2 style={{ marginBottom: 8 }}>Create Your VC Portal</h2>
      <p style={{ color: '#555', marginBottom: 24 }}>Provide a few quick details and launch your live job portal.</p>
      <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
        {error && <div style={{ color: '#c00', fontWeight: 600 }}>{error}</div>}
        {step === 1 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <label>
              <span style={{ display: 'block', fontWeight: 600, marginBottom: 4 }}>Fund or Platform Name</span>
              <input
                name="name"
                value={form.name}
                onChange={handleChange}
                required
                placeholder="e.g. Horizon Ventures"
                style={{ width: '100%', padding: '12px 14px', borderRadius: 8, border: '1px solid #d0d0d0' }}
              />
            </label>
            <label>
              <span style={{ display: 'block', fontWeight: 600, marginBottom: 4 }}>Your Full Name</span>
              <input
                name="admin_name"
                value={form.admin_name}
                onChange={handleChange}
                required
                placeholder="e.g. Alex Morgan"
                style={{ width: '100%', padding: '12px 14px', borderRadius: 8, border: '1px solid #d0d0d0' }}
              />
            </label>
            <label>
              <span style={{ display: 'block', fontWeight: 600, marginBottom: 4 }}>Work Email</span>
              <input
                name="admin_email"
                type="email"
                value={form.admin_email}
                onChange={handleChange}
                required
                placeholder="you@fund.com"
                style={{ width: '100%', padding: '12px 14px', borderRadius: 8, border: '1px solid #d0d0d0' }}
              />
            </label>
            <label>
              <span style={{ display: 'block', fontWeight: 600, marginBottom: 4 }}>Password</span>
              <input
                name="admin_password"
                type="password"
                value={form.admin_password}
                onChange={handleChange}
                required
                minLength={8}
                placeholder="Minimum 8 characters"
                style={{ width: '100%', padding: '12px 14px', borderRadius: 8, border: '1px solid #d0d0d0' }}
              />
            </label>
          </div>
        )}
        {step === 2 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <label>
              <span style={{ display: 'block', fontWeight: 600, marginBottom: 4 }}>Portfolio Size (optional)</span>
              <input
                name="portcos"
                value={form.portcos}
                onChange={handleChange}
                placeholder="Number of companies, stage focus, etc."
                style={{ width: '100%', padding: '12px 14px', borderRadius: 8, border: '1px solid #d0d0d0' }}
              />
            </label>
            <label>
              <span style={{ display: 'block', fontWeight: 600, marginBottom: 4 }}>ATS Tools (optional)</span>
              <input
                name="ats"
                value={form.ats}
                onChange={handleChange}
                placeholder="Greenhouse, Lever, etc."
                style={{ width: '100%', padding: '12px 14px', borderRadius: 8, border: '1px solid #d0d0d0' }}
              />
            </label>
            <label>
              <span style={{ display: 'block', fontWeight: 600, marginBottom: 4 }}>Focus Areas (optional)</span>
              <textarea
                name="focus"
                value={form.focus}
                onChange={handleChange}
                placeholder="What types of candidates or companies are you prioritising?"
                rows={4}
                style={{ width: '100%', padding: '12px 14px', borderRadius: 8, border: '1px solid #d0d0d0', resize: 'vertical' }}
              />
            </label>
          </div>
        )}
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
          {step > 1 ? (
            <button type="button" onClick={handlePrev} style={{ padding: '10px 18px', borderRadius: 8, border: '1px solid #b0b0b0', background: '#fff' }}>
              Back
            </button>
          ) : (
            <span />
          )}
          {step < 2 && (
            <button type="button" onClick={handleNext} style={{ padding: '10px 18px', borderRadius: 8, border: 'none', background: '#0f172a', color: '#fff' }}>
              Next
            </button>
          )}
          {step === 2 && (
            <button type="submit" disabled={submitting} style={{ padding: '10px 18px', borderRadius: 8, border: 'none', background: '#0f172a', color: '#fff' }}>
              {submitting ? 'Registering…' : 'Register'}
            </button>
          )}
        </div>
      </form>
    </div>
  );
};

export default RegistrationPage;
