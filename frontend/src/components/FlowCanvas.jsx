import React, { useRef, useCallback } from 'react';
import ReactFlow, { Background, Controls, MiniMap } from 'reactflow';
import 'reactflow/dist/style.css';
import { useFlowStore } from '../store/useFlowStore';
import PluginNode from './PluginNode';

const nodeTypes = {
  pluginNode: PluginNode,
};


const connectionRules = {
  extractor: ['cleanser', 'transformer', 'validator', 'loader'],
  cleanser: ['cleanser', 'transformer', 'validator', 'loader'],
  transformer: ['transformer', 'validator', 'loader'],
  validator: ['validator', 'loader'],
  loader: [], 
};

const FlowCanvas = () => {
  const reactFlowWrapper = useRef(null);
  const { pipelines, activePipelineId, onNodesChange, onEdgesChange, onConnect, addNode } = useFlowStore();
  const activePipeline = activePipelineId ? pipelines[activePipelineId] : null;

  // The useMemo for nodeTypes is no longer needed here.

  const isValidConnection = useCallback((connection) => {
    const currentNodes = useFlowStore.getState().pipelines[activePipelineId]?.nodes || [];
    const sourceNode = currentNodes.find(node => node.id === connection.source);
    const targetNode = currentNodes.find(node => node.id === connection.target);
    if (!sourceNode || !targetNode) return false;
    const sourceType = sourceNode.data.pluginInfo.type;
    const targetType = targetNode.data.pluginInfo.type;
    if (connectionRules[sourceType] && connectionRules[sourceType].includes(targetType)) {
      return true;
    }
    console.warn(`Invalid connection: from '${sourceType}' to '${targetType}'`);
    return false;
  }, [activePipelineId]);

  const onDragOver = useCallback((event) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback((event) => {
    event.preventDefault();
    const plugin = JSON.parse(event.dataTransfer.getData('application/reactflow'));
    if (!plugin) return;
    const position = {
      x: event.clientX - reactFlowWrapper.current.getBoundingClientRect().left,
      y: event.clientY - reactFlowWrapper.current.getBoundingClientRect().top,
    };
    const newNode = {
      id: `node-${plugin.name}-${+new Date()}`,
      type: 'pluginNode',
      position,
      data: { label: `${plugin.name}`, pluginInfo: plugin, params: {} },
    };
    addNode(newNode);
  }, [addNode]);

  if (!activePipeline) {
    return <div style={{ flexGrow: 1, height: '100%', backgroundColor: '#f9f9f9' }} />;
  }

  return (
    <div style={{ flexGrow: 1, height: '100%' }} ref={reactFlowWrapper}>
      <ReactFlow
        nodes={activePipeline.nodes}
        edges={activePipeline.edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onDrop={onDrop}
        onDragOver={onDragOver}
        nodeTypes={nodeTypes} // Pass the constant object
        isValidConnection={isValidConnection}
        fitView
        key={activePipelineId}
      >
        <Background />
        <Controls />
        <MiniMap />
      </ReactFlow>
    </div>
  );
};

export default FlowCanvas;