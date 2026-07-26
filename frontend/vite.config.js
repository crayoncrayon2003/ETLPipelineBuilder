import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  // Electron loads the production bundle through file://, so asset URLs must
  // remain relative to index.html.
  base: './',
})
