const { app, BrowserWindow, ipcMain, dialog, net } = require('electron');
const fs = require('fs');
const path = require('path');
const isDev = !app.isPackaged;
const maxPipelineFileSize = 10 * 1024 * 1024;

const apiBaseUrl = (process.env.ETL_API_BASE_URL || 'http://127.0.0.1:8000/api/v1')
  .replace(/\/+$/, '');
const allowedApiRequests = new Set([
  'GET /plugins/',
  'GET /schemas/pipeline-definition',
  'POST /pipelines/run',
]);

function createWindow() {
  // Create the browser window.
  const mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    webPreferences: {
      preload: path.join(__dirname, 'preload.cjs'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  // This will remove the default menu bar (File, Edit, etc.) on Windows/Linux.
  mainWindow.setMenuBarVisibility(false);
  mainWindow.webContents.setWindowOpenHandler(() => ({ action: 'deny' }));
  mainWindow.webContents.on('will-navigate', (event, navigationUrl) => {
    const currentUrl = mainWindow.webContents.getURL();
    if (navigationUrl !== currentUrl) {
      event.preventDefault();
    }
  });

  if (isDev) {
    mainWindow.loadURL('http://localhost:5173');
  } else {
    mainWindow.loadFile(path.join(__dirname, '../dist/index.html'));
  }

  // Automatically open the Developer Tools in development mode.
  if (isDev) {
    mainWindow.webContents.openDevTools({ mode: 'detach' });
  }
}

// IPC Handlers
ipcMain.handle('dialog:openFile', async () => {
  const { canceled, filePaths } = await dialog.showOpenDialog({
    title: 'Select a File',
    properties: ['openFile']
  });
  if (!canceled && filePaths.length > 0) return filePaths[0];
  return null;
});

ipcMain.handle('dialog:saveFile', async (event, content, defaultName) => {
  if (typeof content !== 'string') {
    return { success: false, error: 'Pipeline content must be a string.' };
  }
  if (Buffer.byteLength(content, 'utf8') > maxPipelineFileSize) {
    return { success: false, error: 'Pipeline files must be 10 MB or smaller.' };
  }
  const { canceled, filePath } = await dialog.showSaveDialog({
    title: 'Save Pipeline Definition',
    defaultPath: path.basename(defaultName || 'pipeline.json'),
    filters: [{ name: 'JSON Files', extensions: ['json'] }],
  });
  if (!canceled && filePath) {
    try {
      fs.writeFileSync(filePath, content, 'utf-8');
      return { success: true, path: filePath };
    } catch (error) {
      console.error("Failed to save file:", error);
      return { success: false, error: error.message };
    }
  }
  return { success: false };
});

ipcMain.handle('dialog:openPipeline', async () => {
  const { canceled, filePaths } = await dialog.showOpenDialog({
    title: 'Open Pipeline Definition',
    properties: ['openFile'],
    filters: [{ name: 'JSON Files', extensions: ['json'] }],
  });
  if (!canceled && filePaths.length > 0) {
    try {
      if (fs.statSync(filePaths[0]).size > maxPipelineFileSize) {
        return { success: false, error: 'Pipeline files must be 10 MB or smaller.' };
      }
      const content = fs.readFileSync(filePaths[0], 'utf-8');
      const data = JSON.parse(content);
      return { success: true, data: data, path: filePaths[0] };
    } catch (error) {
      console.error("Failed to open or parse file:", error);
      return { success: false, error: error.message };
    }
  }
  return { success: false };
});

// Packaged renderers use file:// and cannot call the HTTP backend directly
// without CORS issues. Keep the reachable API surface deliberately narrow.
ipcMain.handle('api:request', async (event, request) => {
  const method = String(request?.method || 'GET').toUpperCase();
  const requestPath = String(request?.path || '');
  if (!allowedApiRequests.has(`${method} ${requestPath}`)) {
    return { ok: false, status: 403, data: { detail: 'API request is not allowed.' } };
  }

  try {
    const response = await net.fetch(`${apiBaseUrl}${requestPath}`, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: request?.body === undefined ? undefined : JSON.stringify(request.body),
      signal: AbortSignal.timeout(30_000),
    });
    const responseText = await response.text();
    let data = null;
    if (responseText) {
      try {
        data = JSON.parse(responseText);
      } catch {
        data = { detail: responseText };
      }
    }
    return { ok: response.ok, status: response.status, data };
  } catch (error) {
    return {
      ok: false,
      status: 0,
      data: { detail: error instanceof Error ? error.message : String(error) },
    };
  }
});


// App Lifecycle Events
app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});
