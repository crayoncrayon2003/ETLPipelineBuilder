import axios from 'axios';

// Vite exposes environment variables starting with VITE_ on the `import.meta.env` object.
const apiBaseUrl = import.meta.env.VITE_API_BASE_URL;

if (!apiBaseUrl) {
  throw new Error("VITE_API_BASE_URL is not defined. Please check your .env.local file.");
}

const apiClient = axios.create({
  baseURL: apiBaseUrl,
  headers: {
    'Content-Type': 'application/json',
  },
});


/**
 * Fetches the list of all available plugins from the backend.
 * @returns {Promise<Array>} A promise that resolves to the list of plugins.
 */
export const fetchPlugins = () => {
  return apiClient.get('/plugins/');
};

/**
 * Submits a pipeline definition to the backend for an immediate run.
 * @param {object} pipelineDefinition - The pipeline definition object.
 * @returns {Promise<object>} A promise that resolves to the API response.
 */
export const runPipeline = (pipelineDefinition) => {
  return apiClient.post('/pipelines/run', pipelineDefinition);
};

/**
 * Fetches the JSON Schema for the PipelineDefinition model.
 * @returns {Promise<object>} A promise that resolves to the JSON Schema.
 */
export const fetchPipelineSchema = () => {
  return apiClient.get('/schemas/pipeline-definition');
};


export default apiClient;