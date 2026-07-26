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
  const reactFlowInstance = useRef(null);
  const { pipelines, activePipelineId, onNodesChange, onEdgesChange, onConnect, addNode } = useFlowStore();
  const activePipeline = activePipelineId ? pipelines[activePipelineId] : null;

  // The useMemo for nodeTypes is no longer needed here.

  const isValidConnection = useCallback((connection) => {
    const currentPipeline = useFlowStore.getState().pipelines[activePipelineId];
    const currentNodes = currentPipeline?.nodes || [];
    const currentEdges = currentPipeline?.edges || [];
    const sourceNode = currentNodes.find(node => node.id === connection.source);
    const targetNode = currentNodes.find(node => node.id === connection.target);
    if (!sourceNode || !targetNode) return false;
    if (connection.source === connection.target) return false;

    const sourceType = sourceNode.data.pluginInfo?.type;
    const targetType = targetNode.data.pluginInfo?.type;
    if (!connectionRules[sourceType]?.includes(targetType)) {
      return false;
    }

    // The backend accepts at most one input for a node.
    if (currentEdges.some(edge => edge.target === connection.target)) return false;
    if (currentEdges.some(edge => edge.source === connection.source && edge.target === connection.target)) return false;

    // Adding source -> target is cyclic when target can already reach source.
    const adjacency = new Map();
    currentEdges.forEach((edge) => {
      const targets = adjacency.get(edge.source) || [];
      targets.push(edge.target);
      adjacency.set(edge.source, targets);
    });
    const pending = [connection.target];
    const visited = new Set();
    while (pending.length > 0) {
      const nodeId = pending.pop();
      if (nodeId === connection.source) return false;
      if (visited.has(nodeId)) continue;
      visited.add(nodeId);
      pending.push(...(adjacency.get(nodeId) || []));
    }
    return true;
  }, [activePipelineId]);

  const onDragOver = useCallback((event) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'copy';
  }, []);

  const onDrop = useCallback((event) => {
    event.preventDefault();
    const serializedPlugin = event.dataTransfer.getData('application/reactflow');
    if (!serializedPlugin || !reactFlowInstance.current) return;

    let plugin;
    try {
      plugin = JSON.parse(serializedPlugin);
    } catch {
      return;
    }
    if (!plugin?.name) return;

    const position = reactFlowInstance.current.screenToFlowPosition({
      x: event.clientX,
      y: event.clientY,
    });
    const newNode = {
      id: `node-${globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(36).slice(2)}`}`,
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
        onInit={(instance) => { reactFlowInstance.current = instance; }}
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
