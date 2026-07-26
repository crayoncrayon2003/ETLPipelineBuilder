import React, { useState } from 'react';

import PluginSidebar from './components/PluginSidebar';
import FlowCanvas from './components/FlowCanvas';
import ParamsSidebar from './components/ParamsSidebar';
import PipelineTabs from './components/PipelineTabs';

import { useFlowStore } from './store/useFlowStore';
import { runPipeline } from './api/apiClient';

function App() {
  const { pipelines, activePipelineId, loadPipeline } = useFlowStore();
  const activePipeline = activePipelineId ? pipelines[activePipelineId] : null;
  const [isRunning, setIsRunning] = useState(false);

  const getErrorMessage = (error) => {
    const detail = error?.response?.data?.detail;
    if (typeof detail === 'string') return detail;
    if (detail) return JSON.stringify(detail);
    return error?.message || 'An unknown error occurred.';
  };

  const createPipelineDefinition = (includeUi = false) => ({
    name: activePipeline.name.trim() || 'Untitled Pipeline',
    schedule: includeUi ? activePipeline.schedule || null : undefined,
    nodes: activePipeline.nodes.map(node => ({
      id: node.id,
      plugin: node.data.pluginInfo.name,
      params: node.data.params || {},
      ...(includeUi ? { _ui: { position: node.position } } : {}),
    })),
    edges: activePipeline.edges.map(edge => ({
      source_node_id: edge.source,
      target_node_id: edge.target,
    })),
  });

  const handleRunTest = async () => {
    if (!activePipeline) {
      window.alert('No active pipeline to run.');
      return;
    }
    if (activePipeline.nodes.length === 0) {
      window.alert('Pipeline is empty.');
      return;
    }

    setIsRunning(true);
    try {
      const response = await runPipeline(createPipelineDefinition());
      window.alert(`Test run for '${response.data.pipeline_name}' started!`);
    } catch (error) {
      window.alert(`Failed to start test run: ${getErrorMessage(error)}`);
    } finally {
      setIsRunning(false);
    }
  };

  const saveInBrowser = (content, filename) => {
    const blob = new Blob([content], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    link.click();
    URL.revokeObjectURL(url);
  };

  const openInBrowser = () => new Promise((resolve, reject) => {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.json,application/json';
    input.addEventListener('change', async () => {
      try {
        const file = input.files?.[0];
        if (!file) {
          resolve(null);
          return;
        }
        resolve({ data: JSON.parse(await file.text()), path: file.name });
      } catch (error) {
        reject(error);
      }
    }, { once: true });
    input.addEventListener('cancel', () => resolve(null), { once: true });
    input.click();
  });

  const handleSavePipeline = async () => {
    if (!activePipeline) {
      window.alert('No active pipeline to save.');
      return;
    }

    const saveData = createPipelineDefinition(true);
    const content = JSON.stringify(saveData, null, 2);
    const sanitizedName = saveData.name
      .replace(/[<>:"/\\|?*]/g, '_')
      .split('')
      .filter(character => character.charCodeAt(0) >= 32)
      .join('')
      .trim();
    const safeName = (sanitizedName || 'pipeline') + '.json';

    try {
      if (window.electronAPI?.savePipeline) {
        const result = await window.electronAPI.savePipeline(content, safeName);
        if (result.success) {
          window.alert(`Pipeline saved to: ${result.path}`);
        } else if (result.error) {
          throw new Error(result.error);
        }
      } else {
        saveInBrowser(content, safeName);
      }
    } catch (error) {
      window.alert(`Failed to save pipeline: ${getErrorMessage(error)}`);
    }
  };

  const handleLoadPipeline = async () => {
    try {
      const result = window.electronAPI?.openPipeline
        ? await window.electronAPI.openPipeline()
        : await openInBrowser();

      if (result?.success === false && result.error) {
        throw new Error(result.error);
      }
      if (result?.data) {
        loadPipeline(result.data);
        window.alert(`Pipeline loaded successfully from: ${result.path}`);
      }
    } catch (error) {
      window.alert(`Failed to load pipeline: ${getErrorMessage(error)}`);
    }
  };

  return (
    <div className="app-container">
      <header className="app-header">
        <h1>ETL Pipeline Builder</h1>
        <div className="header-buttons">
          <button onClick={handleLoadPipeline} style={{ marginRight: '10px' }}>Load</button>
          <button onClick={handleSavePipeline} style={{ marginRight: '10px' }}>Save</button>
          <button
            onClick={handleRunTest}
            disabled={isRunning}
            style={{ backgroundColor: '#4CAF50', color: 'white' }}
          >
            {isRunning ? 'Starting…' : 'Run Test'}
          </button>
        </div>
      </header>

      <PipelineTabs />

      <div className="main-content">
        <PluginSidebar />
        <FlowCanvas />
        <ParamsSidebar />
      </div>

    </div>
  );
}

export default App;
