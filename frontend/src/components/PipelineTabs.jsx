import React from 'react';
import { useFlowStore } from '../store/useFlowStore';

const PipelineTabs = () => {
  const { pipelines, activePipelineId, setActivePipelineId, addNewPipeline } = useFlowStore();

  return (
    <div style={{ display: 'flex', borderBottom: '1px solid #ccc', padding: '0 10px' }}>
      {Object.values(pipelines).map(p => (
        <button
          key={p.id}
          onClick={() => setActivePipelineId(p.id)}
          style={{
            padding: '10px 15px',
            border: 'none',
            borderBottom: p.id === activePipelineId ? '3px solid blue' : '3px solid transparent',
            background: p.id === activePipelineId ? '#f0f8ff' : 'transparent',
            cursor: 'pointer'
          }}
        >
          {p.name}
        </button>
      ))}
      <button onClick={addNewPipeline} style={{ padding: '10px 15px', border: 'none', background: 'transparent', cursor: 'pointer', fontSize: '18px' }}>
        +
      </button>
    </div>
  );
};

export default PipelineTabs;