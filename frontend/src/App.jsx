// src/App.jsx
import React from 'react';
import PluginSidebar from './components/PluginSidebar';
import FlowCanvas from './components/FlowCanvas';
import ParamsSidebar from './components/ParamsSidebar';
import { useFlowStore } from './store/useFlowStore';
import apiClient from './api/apiClient';

function App() {
  const { nodes, edges } = useFlowStore();

  const handleRunPipeline = () => {
    if (nodes.length === 0) {
      alert("Pipeline is empty. Please add some nodes.");
      return;
    }
    const pipelineDefinition = {
      name: `GUI Pipeline - ${new Date().toISOString()}`,
      nodes: nodes.map(node => ({
        id: node.id,
        plugin: node.data.pluginInfo.name,
        params: node.data.params || {},
      })),
      edges: edges.map(edge => ({
        source_node_id: edge.source,
        target_node_id: edge.target,
        target_input_name: 'input_data',
      })),
    };

    console.log("Submitting pipeline definition:", pipelineDefinition);
    apiClient.post('/pipelines/run', pipelineDefinition)
      .then(response => {
        alert(`Pipeline '${response.data.pipeline_name}' started!`);
      })
      .catch(error => {
        console.error('Failed to run pipeline', error);
        alert('Failed to start pipeline. See console for details.');
      });
  };

  return (
    <div className="app-container">
      
      <header className="app-header">
        <h1>ETL Pipeline Builder</h1>
        <button onClick={handleRunPipeline}>
          Run Pipeline
        </button>
      </header>

      <div className="main-content">
        <PluginSidebar />
        <FlowCanvas />
        <ParamsSidebar />
      </div>
      
    </div>
  );
}

export default App;