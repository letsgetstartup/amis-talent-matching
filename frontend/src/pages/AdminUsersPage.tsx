import React, { useEffect, useState } from 'react';
import { apiDelete, apiGet, apiPost } from '../api';

interface UserRow {
  user_id: string;
  email: string;
  role: string;
  tenant_id?: string | null;
  name?: string | null;
  created_at?: number;
}

interface UserPayload {
  name: string;
  email: string;
  password: string;
  role: string;
  tenant_id?: string;
}

const emptyForm: UserPayload = {
  name: '',
  email: '',
  password: '',
  role: 'vc_admin',
  tenant_id: '',
};

const AdminUsersPage: React.FC = () => {
  const [users, setUsers] = useState<UserRow[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [creating, setCreating] = useState<boolean>(false);
  const [form, setForm] = useState<UserPayload>(emptyForm);
  const [submitting, setSubmitting] = useState<boolean>(false);

  const loadUsers = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await apiGet<{ items: UserRow[] }>('/tenants/users');
      setUsers(data.items || []);
    } catch (err: any) {
      const message = err?.response?.data?.detail || err?.message || 'Failed to load users';
      setError(typeof message === 'string' ? message : JSON.stringify(message));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadUsers();
  }, []);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value } = e.target;
    setForm((prev) => ({ ...prev, [name]: value }));
  };

  const handleCreate = async () => {
    setSubmitting(true);
    setError(null);
    try {
      const payload = { ...form, tenant_id: form.tenant_id?.trim() || undefined };
      await apiPost('/tenants/users', payload);
      setCreating(false);
      setForm(emptyForm);
      await loadUsers();
    } catch (err: any) {
      const message = err?.response?.data?.detail || err?.message || 'Failed to create user';
      setError(typeof message === 'string' ? message : JSON.stringify(message));
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (userId: string) => {
    if (!window.confirm('Delete this user?')) return;
    try {
      await apiDelete(`/tenants/users/${userId}`);
      await loadUsers();
    } catch (err: any) {
      const message = err?.response?.data?.detail || err?.message || 'Failed to delete user';
      setError(typeof message === 'string' ? message : JSON.stringify(message));
    }
  };

  return (
    <div style={{ padding: 24, maxWidth: 960, margin: '0 auto' }}>
      <h2 style={{ marginBottom: 16 }}>User Management</h2>
      <p style={{ color: '#555', marginBottom: 20 }}>Manage admins, VC operators and other tenant users.</p>
      {error && <div style={{ marginBottom: 16, color: '#c00', fontWeight: 600 }}>{error}</div>}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <button
          type="button"
          onClick={() => {
            setCreating((prev) => !prev);
            setError(null);
          }}
          style={{ padding: '10px 16px', borderRadius: 8, border: 'none', background: '#0f172a', color: '#fff', fontWeight: 600 }}
        >
          {creating ? 'Close' : 'New User'}
        </button>
        <button
          type="button"
          onClick={loadUsers}
          style={{ padding: '8px 14px', borderRadius: 8, border: '1px solid #d0d0d0', background: '#fff' }}
        >
          Refresh
        </button>
      </div>

      {creating && (
        <div style={{ border: '1px solid #d0d0d0', borderRadius: 12, padding: 20, marginBottom: 24 }}>
          <h3 style={{ marginTop: 0, marginBottom: 16 }}>Create New User</h3>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(220px,1fr))', gap: 16 }}>
            <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <span>Name</span>
              <input name="name" value={form.name} onChange={handleChange} placeholder="Full name" required />
            </label>
            <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <span>Email</span>
              <input name="email" type="email" value={form.email} onChange={handleChange} placeholder="user@domain.com" required />
            </label>
            <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <span>Password</span>
              <input name="password" type="password" value={form.password} onChange={handleChange} minLength={8} placeholder="Minimum 8 characters" required />
            </label>
            <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <span>Role</span>
              <select name="role" value={form.role} onChange={handleChange}>
                <option value="vc_admin">VC Admin</option>
                <option value="admin">Global Admin</option>
                <option value="recruiter">Recruiter</option>
                <option value="analyst">Analyst</option>
                <option value="candidate">Candidate</option>
              </select>
            </label>
            <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <span>Tenant ID (optional)</span>
              <input name="tenant_id" value={form.tenant_id ?? ''} onChange={handleChange} placeholder="Provide for VC admins & tenant roles" />
            </label>
          </div>
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12, marginTop: 20 }}>
            <button
              type="button"
              onClick={() => {
                setCreating(false);
                setForm(emptyForm);
              }}
              style={{ padding: '10px 18px', borderRadius: 8, border: '1px solid #d0d0d0', background: '#fff' }}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleCreate}
              disabled={submitting}
              style={{ padding: '10px 18px', borderRadius: 8, border: 'none', background: '#0f172a', color: '#fff', fontWeight: 600 }}
            >
              {submitting ? 'Saving…' : 'Create User'}
            </button>
          </div>
        </div>
      )}

      <div style={{ border: '1px solid #d0d0d0', borderRadius: 12, overflow: 'hidden' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead style={{ background: '#f1f5f9', textAlign: 'left' }}>
            <tr>
              <th style={{ padding: '12px 16px' }}>Email</th>
              <th style={{ padding: '12px 16px' }}>Role</th>
              <th style={{ padding: '12px 16px' }}>Tenant</th>
              <th style={{ padding: '12px 16px' }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr>
                <td colSpan={4} style={{ padding: '16px 20px', textAlign: 'center', color: '#64748b' }}>
                  Loading users…
                </td>
              </tr>
            )}
            {!loading && users.length === 0 && (
              <tr>
                <td colSpan={4} style={{ padding: '16px 20px', textAlign: 'center', color: '#64748b' }}>
                  No users found.
                </td>
              </tr>
            )}
            {!loading &&
              users.map((user) => (
                <tr key={user.user_id} style={{ borderTop: '1px solid #e2e8f0' }}>
                  <td style={{ padding: '12px 16px' }}>{user.email}</td>
                  <td style={{ padding: '12px 16px' }}>{user.role}</td>
                  <td style={{ padding: '12px 16px' }}>{user.tenant_id || '—'}</td>
                  <td style={{ padding: '12px 16px' }}>
                    <button
                      type="button"
                      onClick={() => handleDelete(user.user_id)}
                      style={{ padding: '6px 12px', borderRadius: 6, border: '1px solid #fca5a5', background: '#fff1f2', color: '#b91c1c' }}
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default AdminUsersPage;
