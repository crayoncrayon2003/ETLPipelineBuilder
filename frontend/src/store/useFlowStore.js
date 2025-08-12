import { create } from 'zustand';
import {
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
} from 'reactflow';

export const useFlowStore = create((set, get) => ({
  nodes: [],
  edges: [],
  selectedNodeId: null,

  setSelectedNodeId: (nodeId) => {
    set({ selectedNodeId: nodeId });
  },

  updateNodeParams: (nodeId, newParams) => {
    set({
      nodes: get().nodes.map((node) => {
        if (node.id === nodeId) {
          const newData = { ...node.data, params: newParams };
          return { ...node, data: newData };
        }
        return node;
      }),
    });
  },

  onNodesChange: (changes) => {
    const newNodes = applyNodeChanges(changes, get().nodes);

    const selectionChange = changes.find(c => c.type === 'select');
    if (selectionChange) {
      set({ selectedNodeId: selectionChange.selected ? selectionChange.id : null });
    }

    set({ nodes: newNodes });
  },

  onEdgesChange: (changes) => {
    set({
      edges: applyEdgeChanges(changes, get().edges),
    });
  },

  onConnect: (connection) => {
    set((state) => ({
      edges: addEdge(
        { ...connection, type: 'smoothstep', style: { strokeWidth: 2 } },
        state.edges
      ),
    }));
  },

  addNode: (newNode) => {
    set((state) => ({ nodes: [...state.nodes, newNode] }));
  },

  removeNode: (nodeIdToRemove) => {
    set((state) => ({
      nodes: state.nodes.filter(node => node.id !== nodeIdToRemove),
      edges: state.edges.filter(edge => edge.source !== nodeIdToRemove && edge.target !== nodeIdToRemove),
      selectedNodeId: state.selectedNodeId === nodeIdToRemove ? null : state.selectedNodeId,
    }));
  },
}));