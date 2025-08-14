const { contextBridge, ipcRenderer } = require('electron');

console.log("--- Preload script (.cjs) is executing! ---");

try {
  // Expose a controlled API to the renderer process (the React app)
  // under the `window.electronAPI` object.
  contextBridge.exposeInMainWorld(
    'electronAPI',
    {
      /**
       * Opens a native file selection dialog.
       * This function sends an IPC message to the main process and returns a Promise
       * that resolves with the selected file path or null.
       * @returns {Promise<string|null>}
       */
      openFileDialog: () => ipcRenderer.invoke('dialog:openFile'),
      savePipeline: (content, defaultName) => ipcRenderer.invoke('dialog:saveFile', content, defaultName),
      openPipeline: () => ipcRenderer.invoke('dialog:openPipeline'),
    }
  );
  console.log("--- contextBridge.exposeInMainWorld succeeded! ---");
} catch (error) {
  console.error("--- Error in preload script (.cjs): ---", error);
}

