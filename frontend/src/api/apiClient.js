import axios from 'axios';

// Vite exposes environment variables starting with VITE_ on the `import.meta.env` object.
const apiBaseUrl = (import.meta.env?.VITE_API_BASE_URL || 'http://127.0.0.1:8000/api/v1')
  .replace(/\/+$/, '');

const apiClient = axios.create({
  baseURL: apiBaseUrl,
  timeout: 30_000,
  headers: {
    'Content-Type': 'application/json',
  },
});

const requestThroughElectron = async (method, path, body) => {
  const result = await globalThis.window.electronAPI.apiRequest({ method, path, body });
  if (!result?.ok) {
    const error = new Error(result?.data?.detail || 'The backend request failed.');
    error.response = {
      status: result?.status || 0,
      data: result?.data || { detail: error.message },
    };
    throw error;
  }
  return { data: result.data, status: result.status };
};

const request = (method, path, body) => {
  if (globalThis.window?.electronAPI?.apiRequest) {
    return requestThroughElectron(method, path, body);
  }
  return apiClient.request({ method, url: path, data: body });
};


/**
 * Fetches the list of all available plugins from the backend.
 * @returns {Promise<Array>} A promise that resolves to the list of plugins.
 */
export const fetchPlugins = () => {
  return request('GET', '/plugins/');
};

/**
 * Submits a pipeline definition to the backend for an immediate run.
 * @param {object} pipelineDefinition - The pipeline definition object.
 * @returns {Promise<object>} A promise that resolves to the API response.
 */
export const runPipeline = (pipelineDefinition) => {
  return request('POST', '/pipelines/run', pipelineDefinition);
};

/**
 * Fetches the JSON Schema for the PipelineDefinition model.
 * @returns {Promise<object>} A promise that resolves to the JSON Schema.
 */
export const fetchPipelineSchema = () => {
  return request('GET', '/schemas/pipeline-definition');
};


export default apiClient;
