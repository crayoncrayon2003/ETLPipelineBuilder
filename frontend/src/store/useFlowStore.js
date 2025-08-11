// src/store/useFlowStore.js
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

  // --- Start of Modification: Enhance onNodesChange ---
  onNodesChange: (changes) => {
    // Process all changes (like position, etc.) using the helper
    const newNodes = applyNodeChanges(changes, get().nodes);

    // Specifically look for selection changes to update our custom state
    const selectionChange = changes.find(c => c.type === 'select');
    if (selectionChange) {
      // If a node was selected, set its ID. If deselected, set to null.
      set({ selectedNodeId: selectionChange.selected ? selectionChange.id : null });
    }

    // Apply the node changes to the store
    set({ nodes: newNodes });
  },
  // --- End of Modification ---

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
      // Also deselect if the removed node was selected
      selectedNodeId: state.selectedNodeId === nodeIdToRemove ? null : state.selectedNodeId,
    }));
  },
}));