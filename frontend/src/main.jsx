import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'
import './index.css'

import { useFlowStore } from './store/useFlowStore.js';

if (Object.keys(useFlowStore.getState().pipelines).length === 0) {
  useFlowStore.getState().addNewPipeline();
}


ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
