import React from 'react';

import PluginSidebar from './components/PluginSidebar';
import FlowCanvas from './components/FlowCanvas';
import ParamsSidebar from './components/ParamsSidebar';
import PipelineTabs from './components/PipelineTabs';

import { useFlowStore } from './store/useFlowStore';
import { runPipeline } from './api/apiClient';

function App() {
  const { pipelines, activePipelineId, loadPipeline } = useFlowStore();
  const activePipeline = activePipelineId ? pipelines[activePipelineId] : null;

  const handleRunTest = () => {
    if (!activePipeline) return alert("No active pipeline to run.");
    if (activePipeline.nodes.length === 0) return alert("Pipeline is empty.");

    const pipelineDefinition = {
      name: `${activePipeline.name} (GUI Test Run)`,
      nodes: activePipeline.nodes.map(node => ({
        id: node.id,
        plugin: node.data.pluginInfo.name,
        params: node.data.params || {},
      })),
      edges: activePipeline.edges.map(edge => ({
        source_node_id: edge.source,
        target_node_id: edge.target,
        target_input_name: 'input_data',
      })),
    };

    runPipeline(pipelineDefinition)
      .then(response => {
        alert(`Test run for '${response.data.pipeline_name}' started!`);
      })
      .catch(error => {
        alert(`Failed to start test run: ${error.response?.data?.detail || error.message}`);
      });
  };

  const handleSavePipeline = async () => {
    if (!activePipeline) return alert("No active pipeline to save.");
    if (!window.electronAPI) return console.error("Electron API is not available.");

    const saveData = {
      name: activePipeline.name,
      schedule: activePipeline.schedule || null,
      nodes: activePipeline.nodes.map(node => ({
        id: node.id,
        plugin: node.data.pluginInfo.name,
        params: node.data.params || {},
        _ui: { position: node.position }
      })),
      edges: activePipeline.edges.map(edge => ({
        source_node_id: edge.source,
        target_node_id: edge.target,
        target_input_name: 'input_data',
      })),
    };

    const result = await window.electronAPI.savePipeline(JSON.stringify(saveData, null, 2), `${activePipeline.name}.json`);
    if (result.success) alert(`Pipeline saved to: ${result.path}`);
  };

  const handleLoadPipeline = async () => {
    if (!window.electronAPI) return console.error("Electron API is not available.");
    const result = await window.electronAPI.openPipeline();
    if (result && result.success) {
      loadPipeline(result.data);
      alert(`Pipeline loaded successfully from: ${result.path}`);
    }
  };

  return (
    <div className="app-container">
      <header className="app-header">
        <h1>ETL Pipeline Builder</h1>
        <div className="header-buttons">
          <button onClick={handleLoadPipeline} style={{ marginRight: '10px' }}>Load</button>
          <button onClick={handleSavePipeline} style={{ marginRight: '10px' }}>Save</button>
          <button onClick={handleRunTest} style={{ backgroundColor: '#4CAF50', color: 'white' }}>
            Run Test
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