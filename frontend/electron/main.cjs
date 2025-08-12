const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const isDev = require('electron-is-dev');

function createWindow() {
  const mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    webPreferences: {
      // The preload script is a crucial security feature. It runs in a privileged
      // context and can safely expose a limited API to the renderer process (React app).
      preload: path.join(__dirname, 'preload.cjs'),
      // Security best practices (defaults in modern Electron):
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  // Determine the URL to load.
  const startUrl = isDev
    ? 'http://localhost:5173' // Vite dev server URL
    : `file://${path.join(__dirname, '../dist/index.html')}`; // Production build path

  mainWindow.loadURL(startUrl);

  // Automatically open the Developer Tools in development mode.
  if (isDev) {
    mainWindow.webContents.openDevTools({ mode: 'detach' });
  }
}

// --- Inter-Process Communication (IPC) ---

// Listen for the 'dialog:openFile' channel from the renderer process.
ipcMain.handle('dialog:openFile', async () => {
  console.log("ipcMain: Received 'dialog:openFile' event!");
  const { canceled, filePaths } = await dialog.showOpenDialog({
    title: 'Select a File',
    properties: ['openFile']
  });

  if (!canceled && filePaths.length > 0) {
    console.log(`ipcMain: File selected: ${filePaths[0]}`);
    return filePaths[0];
  }

  console.log("ipcMain: Dialog was canceled.");
  return null;
});


// --- Electron App Lifecycle Events ---

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