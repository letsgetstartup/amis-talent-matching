import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // Core API routes
  '/auth': 'http://localhost:8080',
  '/tenant': 'http://localhost:8080',
      '/match': 'http://localhost:8080',
      '/candidates': 'http://localhost:8080',
      '/jobs': 'http://localhost:8080',
      '/candidate': 'http://localhost:8080',
      '/job': 'http://localhost:8080',
      '/config': 'http://localhost:8080',
      '/llm': 'http://localhost:8080',
      '/search': 'http://localhost:8080',
      '/share': 'http://localhost:8080',
      '/maintenance': 'http://localhost:8080',
      '/upload': 'http://localhost:8080',
      '/ingest': 'http://localhost:8080',
      '/personal-letter': 'http://localhost:8080',
      '/db': 'http://localhost:8080',
      '/health': 'http://localhost:8080',
      '/ready': 'http://localhost:8080',
      '/static': 'http://localhost:8080',
    },
  },
});
