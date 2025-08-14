import React, { memo } from 'react';
import { Handle, Position, useReactFlow } from 'reactflow';

const PluginNode = ({ data, isConnectable, id }) => {
  const { deleteElements } = useReactFlow();

  const handleRemoveClick = () => {
    deleteElements({ nodes: [{ id }] });
  };

  const pluginType = data.pluginInfo.type;
  const hasSourceHandle = ['extractor', 'cleanser', 'transformer', 'validator'].includes(pluginType);
  const hasTargetHandle = ['cleanser', 'transformer', 'validator', 'loader'].includes(pluginType);

  return (
    <div style={{
      border: '2px solid #ddd',
      borderRadius: '8px',
      padding: '10px 15px',
      backgroundColor: 'white',
      minWidth: '180px',
      boxShadow: '0 2px 5px rgba(0,0,0,0.1)',
    }}>
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        fontWeight: 'bold',
        borderBottom: '1px solid #eee',
        paddingBottom: '5px',
        marginBottom: '8px',
      }}>
        <span>{data.label}</span>
        <button 
          onClick={handleRemoveClick} 
          style={{
            border: 'none',
            background: 'transparent',
            cursor: 'pointer',
            fontSize: '18px',
            color: '#aaa',
            padding: '0 5px',
            lineHeight: 1,
          }}
          title="Remove Node"
        >
          ×
        </button>
      </div>

      <div style={{ fontSize: '12px', color: '#777' }}>
        Type: {pluginType}
      </div>

      {hasTargetHandle && (
        <Handle type="target" position={Position.Left} isConnectable={isConnectable} />
      )}

      {hasSourceHandle && (
        <Handle type="source" position={Position.Right} isConnectable={isConnectable} />
      )}
    </div>
  );
};

export default memo(PluginNode);