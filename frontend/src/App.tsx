import React, { type FC } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import LegacyApp from './pages/LegacyApp';
import RegistrationPage from './pages/RegistrationPage';
import PortalPage from './pages/PortalPage';
import PortalDynamicPage from './pages/PortalDynamicPage';
import AdminUsersPage from './pages/AdminUsersPage';
import AdminTenantPage from './pages/AdminTenantPage';

const App: FC = () => (
  <BrowserRouter>
    <Routes>
      <Route path="/" element={<LegacyApp />} />
      <Route path="/registration" element={<RegistrationPage />} />
      <Route path="/portal/:slug" element={<PortalPage />} />
  <Route path="/portal/dynamic/:slug" element={<PortalDynamicPage />} />
      <Route path="/admin/users" element={<AdminUsersPage />} />
      <Route path="/admin/tenants/:tenantId" element={<AdminTenantPage />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  </BrowserRouter>
);

export default App;
