const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const isDev = require('electron-is-dev');

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

  // Determine the URL to load.
  const startUrl = isDev
    ? 'http://localhost:5173'
    : `file://${path.join(__dirname, '../dist/index.html')}`;

  mainWindow.loadURL(startUrl);

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
  const { canceled, filePath } = await dialog.showSaveDialog({
    title: 'Save Pipeline Definition',
    defaultPath: defaultName || 'pipeline.json',
    filters: [{ name: 'JSON Files', extensions: ['json'] }],
  });
  if (!canceled && filePath) {
    const fs = require('fs');
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
    const fs = require('fs');
    try {
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