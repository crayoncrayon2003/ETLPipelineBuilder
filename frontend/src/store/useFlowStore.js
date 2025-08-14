import { create } from 'zustand';
import {
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
} from 'reactflow';
import { nanoid } from 'nanoid';
import { fetchPlugins } from '../api/apiClient';

const createNewPipeline = (name = 'Untitled Pipeline') => ({
  id: `pipeline-${nanoid()}`,
  name,
  nodes: [],
  edges: [],
  schedule: null,
});

export const useFlowStore = create((set, get) => ({
  masterPlugins: [],
  pipelines: {},
  activePipelineId: null,
  selectedNodeId: null,

  /**
   * Fetches the master list of plugins from the backend API and stores it.
   * This should be called once when the application starts.
   */
  fetchAndSetMasterPlugins: async () => {
    try {
      const response = await fetchPlugins();
      set({ masterPlugins: response.data });
      console.log("Master plugin list has been fetched and stored globally.");
    } catch (error) {
      console.error("Failed to fetch master plugin list:", error);
      set({ masterPlugins: [] }); // Ensure it's an array on failure
    }
  },

  /**
   * Adds a new, empty pipeline to the state and makes it active.
   */
  addNewPipeline: () => {
    const newPipeline = createNewPipeline();
    set((state) => ({
      pipelines: { ...state.pipelines, [newPipeline.id]: newPipeline },
      activePipelineId: newPipeline.id,
      selectedNodeId: null,
    }));
    return newPipeline.id;
  },

  /**
   * Loads pipeline data from a file, re-hydrates it, and adds it as a new active tab.
   */
  loadPipeline: (pipelineDataFromFile) => {
    const masterPlugins = get().masterPlugins;
    const pluginMap = new Map(masterPlugins.map(p => [p.name, p]));

    if (pipelineDataFromFile.nodes) {
      pipelineDataFromFile.nodes.forEach(node => {
        const fullPluginInfo = pluginMap.get(node.plugin);
        if (node._ui?.position) {
          node.position = node._ui.position;
        }
        node.data = {
          label: node.plugin,
          pluginInfo: fullPluginInfo || { name: node.plugin, type: 'unknown', parameters_schema: {} },
          params: node.params,
        };
      });
    }

    if (pipelineDataFromFile.edges) {
      pipelineDataFromFile.edges.forEach(edge => {
        edge.source = edge.source_node_id;
        edge.target = edge.target_node_id;
      });
    }

    const newPipeline = { id: `pipeline-${nanoid()}`, ...pipelineDataFromFile };
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
      pipelines: { ...state.pipelines, [activeId]: { ...state.pipelines[activeId], name: newName } },
    }));
  },

  updateActivePipelineSchedule: (newSchedule) => {
    const activeId = get().activePipelineId;
    if (!activeId) return;
    set((state) => ({
      pipelines: { ...state.pipelines, [activeId]: { ...state.pipelines[activeId], schedule: newSchedule } },
    }));
  },


  onNodesChange: (changes) => {
    const activeId = get().activePipelineId;
    if (!activeId || !get().pipelines[activeId]) return;

    const selectionChange = changes.find(c => c.type === 'select');
    if (selectionChange) {
      set({ selectedNodeId: selectionChange.selected ? selectionChange.id : null });
    }

    set((state) => ({
      pipelines: {
        ...state.pipelines,
        [activeId]: { ...state.pipelines[activeId], nodes: applyNodeChanges(changes, state.pipelines[activeId].nodes) },
      },
    }));
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
    if (!activeId) return;
    set((state) => ({
      pipelines: {
        ...state.pipelines,
        [activeId]: {
          ...state.pipelines[activeId],
          edges: addEdge({ ...connection, type: 'smoothstep', style: { strokeWidth: 2 } }, state.pipelines[activeId].edges),
        },
      },
    }));
  },

  addNode: (newNode) => {
    const activeId = get().activePipelineId;
    if (!activeId) return;
    set((state) => ({
      pipelines: {
        ...state.pipelines,
        [activeId]: { ...state.pipelines[activeId], nodes: [...state.pipelines[activeId].nodes, newNode] },
      },
    }));
  },

  updateNodeParams: (nodeId, newParams) => {
    const activeId = get().activePipelineId;
    if (!activeId) return;
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