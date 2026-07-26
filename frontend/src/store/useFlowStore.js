import { create } from 'zustand';
import {
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
} from 'reactflow';
import { fetchPlugins } from '../api/apiClient.js';

const createId = (prefix) => {
  const id = globalThis.crypto?.randomUUID?.()
    || `${Date.now()}-${Math.random().toString(36).slice(2)}`;
  return `${prefix}-${id}`;
};

const wouldCreateCycle = (edges, source, target) => {
  if (source === target) return true;
  const adjacency = new Map();
  edges.forEach((edge) => {
    const targets = adjacency.get(edge.source) || [];
    targets.push(edge.target);
    adjacency.set(edge.source, targets);
  });
  const pending = [target];
  const visited = new Set();
  while (pending.length > 0) {
    const nodeId = pending.pop();
    if (nodeId === source) return true;
    if (visited.has(nodeId)) continue;
    visited.add(nodeId);
    pending.push(...(adjacency.get(nodeId) || []));
  }
  return false;
};

const createNewPipeline = (name = 'Untitled Pipeline') => ({
  id: createId('pipeline'),
  name,
  nodes: [],
  edges: [],
  schedule: null,
});

export const useFlowStore = create((set, get) => ({
  masterPlugins: [],
  pluginsLoading: false,
  pluginsError: null,
  pipelines: {},
  activePipelineId: null,
  selectedNodeId: null,

  fetchAndSetMasterPlugins: async () => {
    if (get().pluginsLoading) return;
    set({ pluginsLoading: true, pluginsError: null });
    try {
      const response = await fetchPlugins();
      const plugins = Array.isArray(response.data) ? response.data : [];
      const pluginMap = new Map(plugins.map(plugin => [plugin.name, plugin]));
      set((state) => ({
        masterPlugins: plugins,
        pluginsLoading: false,
        pipelines: Object.fromEntries(
          Object.entries(state.pipelines).map(([id, pipeline]) => [
            id,
            {
              ...pipeline,
              nodes: pipeline.nodes.map(node => ({
                ...node,
                data: {
                  ...node.data,
                  pluginInfo: pluginMap.get(node.data.pluginInfo?.name) || node.data.pluginInfo,
                },
              })),
            },
          ]),
        ),
      }));
    } catch (error) {
      console.error("Failed to fetch master plugin list:", error);
      set({
        pluginsLoading: false,
        pluginsError: error.response?.data?.detail || error.message || 'Failed to fetch plugins.',
      });
    }
  },

  addNewPipeline: () => {
    const newPipeline = createNewPipeline();
    set((state) => ({
      pipelines: { ...state.pipelines, [newPipeline.id]: newPipeline },
      activePipelineId: newPipeline.id,
      selectedNodeId: null,
    }));
    return newPipeline.id;
  },

  loadPipeline: (pipelineDataFromFile) => {
    if (!pipelineDataFromFile || typeof pipelineDataFromFile !== 'object' || Array.isArray(pipelineDataFromFile)) {
      throw new Error('The selected file does not contain a pipeline object.');
    }

    const masterPlugins = get().masterPlugins;
    const pluginMap = new Map(masterPlugins.map(p => [p.name, p]));
    const rawNodes = Array.isArray(pipelineDataFromFile.nodes) ? pipelineDataFromFile.nodes : [];
    const rawEdges = Array.isArray(pipelineDataFromFile.edges) ? pipelineDataFromFile.edges : [];

    const nodes = rawNodes.map((node, index) => {
      if (!node || typeof node !== 'object' || !node.id || !node.plugin) {
        throw new Error(`Node ${index + 1} must have both "id" and "plugin".`);
      }
      const fullPluginInfo = pluginMap.get(node.plugin);
      return {
        id: String(node.id),
        type: 'pluginNode',
        position: {
          x: Number.isFinite(node._ui?.position?.x) ? node._ui.position.x : 80 + index * 40,
          y: Number.isFinite(node._ui?.position?.y) ? node._ui.position.y : 80 + index * 40,
        },
        data: {
          label: node.plugin,
          pluginInfo: fullPluginInfo || {
            name: node.plugin,
            type: 'unknown',
            description: 'Plugin metadata is unavailable.',
            parameters_schema: { type: 'object', properties: {} },
          },
          params: node.params && typeof node.params === 'object' ? node.params : {},
        },
      };
    });

    const nodeIds = new Set(nodes.map(node => node.id));
    if (nodeIds.size !== nodes.length) {
      throw new Error('Node IDs must be unique.');
    }

    const edges = rawEdges.map((edge, index) => {
      const source = edge?.source_node_id ?? edge?.source;
      const target = edge?.target_node_id ?? edge?.target;
      if (!source || !target || !nodeIds.has(String(source)) || !nodeIds.has(String(target))) {
        throw new Error(`Edge ${index + 1} refers to a missing source or target node.`);
      }
      return {
        id: edge.id || createId('edge'),
        source: String(source),
        target: String(target),
        type: 'smoothstep',
        style: { strokeWidth: 2 },
      };
    });
    const targets = new Set();
    edges.forEach((edge) => {
      if (targets.has(edge.target)) {
        throw new Error(`Node "${edge.target}" has more than one incoming edge.`);
      }
      targets.add(edge.target);
      if (wouldCreateCycle(edges.filter(candidate => candidate !== edge), edge.source, edge.target)) {
        throw new Error('The pipeline graph contains a cycle.');
      }
    });

    const newPipeline = {
      id: createId('pipeline'),
      name: typeof pipelineDataFromFile.name === 'string' && pipelineDataFromFile.name.trim()
        ? pipelineDataFromFile.name
        : 'Imported Pipeline',
      schedule: pipelineDataFromFile.schedule ?? null,
      nodes,
      edges,
    };
    set((state) => ({
      pipelines: { ...state.pipelines, [newPipeline.id]: newPipeline },
      activePipelineId: newPipeline.id,
      selectedNodeId: null,
    }));
  },

  setActivePipelineId: (pipelineId) => {
    set({ activePipelineId: pipelineId, selectedNodeId: null });
  },

  updateActivePipelineName: (newName) => {
    const activeId = get().activePipelineId;
    if (!activeId) return;
    set((state) => ({
      pipelines: {
        ...state.pipelines,
        [activeId]: { ...state.pipelines[activeId], name: String(newName) },
      },
    }));
  },

  onNodesChange: (changes) => {
    const activeId = get().activePipelineId;
    if (!activeId || !get().pipelines[activeId]) return;
    set((state) => {
      const nodes = applyNodeChanges(changes, state.pipelines[activeId].nodes);
      const selectedNode = nodes.find(node => node.selected);
      return {
        selectedNodeId: selectedNode?.id || null,
        pipelines: {
          ...state.pipelines,
          [activeId]: { ...state.pipelines[activeId], nodes },
        },
      };
    });
  },

  onEdgesChange: (changes) => {
    const activeId = get().activePipelineId;
    if (!activeId || !get().pipelines[activeId]) return;
    set((state) => ({
      pipelines: {
        ...state.pipelines,
        [activeId]: { ...state.pipelines[activeId], edges: applyEdgeChanges(changes, state.pipelines[activeId].edges) },
      },
    }));
  },

  onConnect: (connection) => {
    const activeId = get().activePipelineId;
    if (!activeId || !connection.source || !connection.target) return;
    set((state) => {
      const pipeline = state.pipelines[activeId];
      if (!pipeline) return state;
      const duplicate = pipeline.edges.some(
        edge => edge.source === connection.source && edge.target === connection.target,
      );
      const alreadyHasInput = pipeline.edges.some(edge => edge.target === connection.target);
      if (duplicate || alreadyHasInput || wouldCreateCycle(pipeline.edges, connection.source, connection.target)) {
        return state;
      }
      return {
        pipelines: {
          ...state.pipelines,
          [activeId]: {
            ...pipeline,
            edges: addEdge(
              { ...connection, type: 'smoothstep', style: { strokeWidth: 2 } },
              pipeline.edges,
            ),
          },
        },
      };
    });
  },

  addNode: (newNode) => {
    const activeId = get().activePipelineId;
    if (!activeId || !get().pipelines[activeId]) return;
    set((state) => ({
      pipelines: {
        ...state.pipelines,
        [activeId]: { ...state.pipelines[activeId], nodes: [...state.pipelines[activeId].nodes, newNode] },
      },
    }));
  },

  updateNodeParams: (nodeId, newParams) => {
    const activeId = get().activePipelineId;
    if (!activeId || !get().pipelines[activeId]) return;
    set((state) => ({
      pipelines: {
        ...state.pipelines,
        [activeId]: {
          ...state.pipelines[activeId],
          nodes: state.pipelines[activeId].nodes.map((node) => {
            if (node.id === nodeId) {
              return { ...node, data: { ...node.data, params: newParams } };
            }
            return node;
          }),
        },
      },
    }));
  },
}));
