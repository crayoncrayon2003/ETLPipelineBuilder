import React from 'react';
import { useFlowStore } from '../store/useFlowStore';

import Form from '@rjsf/mui';
import validator from '@rjsf/validator-ajv8';

const FilePathWidget = (props) => {
  const { id, value, onChange, schema } = props;

  const handleBrowseClick = async () => {
    if (window.electronAPI && typeof window.electronAPI.openFileDialog === 'function') {
      try {
        const filePath = await window.electronAPI.openFileDialog();
        if (filePath) {
          onChange(filePath);
        }
      } catch (error) {
        console.error("Error opening file dialog:", error);
      }
    } else {
      console.error("electronAPI.openFileDialog is not available.");
    }
  };

  return (
    <div style={{ display: 'flex', alignItems: 'center', width: '100%' }}>
      <input
        type="text"
        id={id}
        className="MuiInputBase-input MuiOutlinedInput-input"
        style={{ flexGrow: 1, padding: '8.5px 14px', border: '1px solid #ccc', borderRadius: '4px' }}
        value={value || ''}
        onChange={(event) => onChange(event.target.value)}
        placeholder={schema.description || 'File path...'}
      />
      <button
        type="button"
        onClick={handleBrowseClick}
        style={{ marginLeft: '8px', padding: '8px 12px' }}
      >
        Browse...
      </button>
    </div>
  );
};


// --- Component for Pipeline-level Settings ---
const PipelineSettings = () => {
  const { pipelines, activePipelineId, updateActivePipelineName } = useFlowStore();
  const activePipeline = activePipelineId ? pipelines[activePipelineId] : null;

  if (!activePipeline) {
    return null; // Don't render if no pipeline is active
  }

  return (
    <div>
      <h3 style={{ marginTop: 0 }}>Pipeline Settings</h3>
      <div style={{ marginBottom: '20px' }}>
        <label htmlFor="pipeline-name" style={{ display: 'block', marginBottom: '5px' }}>Pipeline Name</label>
        <input
          type="text"
          id="pipeline-name"
          className="MuiInputBase-input MuiOutlinedInput-input"
          style={{ width: '100%', padding: '8.5px 14px' }}
          value={activePipeline.name}
          onChange={(e) => updateActivePipelineName(e.target.value)}
        />
      </div>
      {/* Schedule input has been removed from the UI */}
    </div>
  );
};


// --- Main Sidebar Component ---
const ParamsSidebar = () => {
  const { pipelines, activePipelineId, selectedNodeId, updateNodeParams } = useFlowStore();

  const activePipeline = activePipelineId ? pipelines[activePipelineId] : undefined;
  const selectedNode = activePipeline?.nodes?.find(node => node.id === selectedNodeId);

  // --- RENDER LOGIC ---
  if (selectedNode) {
    // If a node IS selected, render the parameter form for that node
    const schema = selectedNode.data.pluginInfo.parameters_schema;
    const formData = selectedNode.data.params || {};
    const widgets = { FilePathWidget: FilePathWidget };
    const uiSchema = {
      ...Object.keys(schema.properties || {})
        .filter(key => key.includes('path') || key.includes('_file'))
        .reduce((acc, key) => { acc[key] = { "ui:widget": "FilePathWidget" }; return acc; }, {})
    };

    return (
      <aside style={{
        borderLeft: '1px solid #eee', padding: '15px', width: '350px',
        overflowY: 'auto', flexShrink: 0
      }}>
        <h3 style={{ marginTop: 0 }}>Parameters for "{selectedNode.data.label}"</h3>
        <Form
          schema={schema}
          uiSchema={uiSchema}
          formData={formData}
          validator={validator}
          onChange={(e) => updateNodeParams(selectedNodeId, e.formData)}
          widgets={widgets}
          liveValidate
          showErrorList={false}
        >
          <button type="submit" style={{ display: 'none' }} />
        </Form>
      </aside>
    );
  } else {
    // If NO node is selected, render the pipeline-level settings
    return (
      <aside style={{
        borderLeft: '1px solid #eee', padding: '15px', width: '350px',
        backgroundColor: '#fafafa', overflowY: 'auto', flexShrink: 0
      }}>
        <PipelineSettings />
      </aside>
    );
  }
};

export default ParamsSidebar;