// src/api/apiClient.js
import axios from 'axios';

// Vite exposes environment variables starting with VITE_ on the `import.meta.env` object.
const apiBaseUrl = import.meta.env.VITE_API_BASE_URL;

if (!apiBaseUrl) {
  // If the environment variable is not set, throw an error to fail fast.
  throw new Error("VITE_API_BASE_URL is not defined. Please check your .env.local file.");
}

const apiClient = axios.create({
  baseURL: apiBaseUrl,
  headers: {
    'Content-Type': 'application/json',
  },
});

export default apiClient;