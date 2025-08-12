import React from 'react';
import { useFlowStore } from '../store/useFlowStore';

import Form from '@rjsf/mui';
import validator from '@rjsf/validator-ajv8';


// This custom component creates a text input with a "Browse..." button
const FilePathWidget = (props) => {
  const { id, value, onChange, schema, rawErrors = [] } = props;

  const handleBrowseClick = async () => {
    // Check if the electronAPI is available on the window object
    if (window.electronAPI && typeof window.electronAPI.openFileDialog === 'function') {
      try {
        // Call the API exposed by our preload.js script
        const filePath = await window.electronAPI.openFileDialog();
        if (filePath) {
          // Update the form's state with the selected path
          onChange(filePath);
        }
      } catch (error) {
        console.error("Error opening file dialog:", error);
      }
    } else {
      console.error("electronAPI.openFileDialog is not available. Ensure you are running in Electron.");
    }
  };

  return (
    <div style={{ display: 'flex', alignItems: 'center', width: '100%' }}>
      <input
        type="text"
        id={id}
        // Use MUI's input classes for consistent styling
        className="MuiInputBase-input MuiOutlinedInput-input"
        style={{
          flexGrow: 1,
          padding: '8.5px 14px',
          border: `1px solid ${rawErrors.length > 0 ? 'red' : '#ccc'}`, // Highlight on error
          borderRadius: '4px',
          marginRight: '8px'
        }}
        value={value || ''}
        onChange={(event) => onChange(event.target.value)}
        placeholder={schema.description || 'File path...'}
      />
      <button
        type="button"
        onClick={handleBrowseClick}
        style={{
          padding: '8px 12px',
          border: '1px solid #ccc',
          borderRadius: '4px',
          backgroundColor: '#f0f0f0',
          cursor: 'pointer'
        }}
      >
        Browse...
      </button>
    </div>
  );
};


// --- Main Sidebar Component ---
const ParamsSidebar = () => {
  const { nodes, selectedNodeId, updateNodeParams } = useFlowStore();
  const selectedNode = nodes.find(node => node.id === selectedNodeId);

  const handleParamsChange = ({ formData }) => {
    if (selectedNode) {
      updateNodeParams(selectedNodeId, formData);
    }
  };

  if (!selectedNode) {
    return (
      <aside style={{
        borderLeft: '1px solid #eee', padding: '15px', width: '350px',
        backgroundColor: '#fafafa', overflowY: 'auto', flexShrink: 0
      }}>
        <h3 style={{ marginTop: 0 }}>Parameters</h3>
        <p style={{ color: '#777', fontSize: '14px' }}>
          Select a node to view and edit its parameters.
        </p>
      </aside>
    );
  }

  const schema = selectedNode.data.pluginInfo.parameters_schema;
  const formData = selectedNode.data.params || {};

  const widgets = {
    FilePathWidget: FilePathWidget,
  };

  const uiSchema = {
    ...Object.keys(schema.properties || {})
      .filter(key => key.includes('path') || key.includes('_file'))
      .reduce((acc, key) => {
        acc[key] = { "ui:widget": "FilePathWidget" };
        return acc;
      }, {})
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
        onChange={handleParamsChange}
        widgets={widgets}
        liveValidate
        showErrorList={false}
      >
        <button type="submit" style={{ display: 'none' }} />
      </Form>
    </aside>
  );
};

export default ParamsSidebar;