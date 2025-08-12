import React, { useState, useEffect, useMemo } from 'react';
import apiClient from '../api/apiClient';

const formatTypeName = (type) => {
  if (!type) return "Unknown";
  return type.charAt(0).toUpperCase() + type.slice(1) + 's';
};

const PluginSidebar = () => {
  const [plugins, setPlugins] = useState([]);
  const [openGroups, setOpenGroups] = useState({ extractor: true });

  useEffect(() => {
    apiClient.get('/plugins/')
      .then(response => setPlugins(response.data))
      .catch(error => console.error('Failed to fetch plugins', error));
  }, []);

  const groupedPlugins = useMemo(() => {
    if (!plugins.length) return {};
    return plugins.reduce((acc, plugin) => {
      const type = plugin.type || 'unknown';
      if (!acc[type]) acc[type] = [];
      acc[type].push(plugin);
      return acc;
    }, {});
  }, [plugins]);

  const onDragStart = (event, plugin) => {
    event.dataTransfer.setData('application/reactflow', JSON.stringify(plugin));
    event.dataTransfer.effectAllowed = 'move';
  };

  const toggleGroup = (groupKey) => {
    setOpenGroups(prev => ({ ...prev, [groupKey]: !prev[groupKey] }));
  };

  const groupOrder = ['extractor', 'cleanser', 'transformer', 'validator', 'loader', 'unknown'];

  return (
    <aside style={{
      borderRight: '1px solid #eee',
      padding: '15px',
      width: '250px',
      overflowY: 'auto',
      backgroundColor: '#fcfcfc',
      flexShrink: 0
    }}>
      <h3 style={{ marginTop: 0 }}>Plugins</h3>
      {groupOrder.map(groupKey => (
        groupedPlugins[groupKey] && (
          <div key={groupKey} style={{ marginBottom: '10px' }}>
            <h4 onClick={() => toggleGroup(groupKey)} style={{
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
              borderBottom: '1px solid #ccc', paddingBottom: '5px', marginBottom: '10px',
              cursor: 'pointer', userSelect: 'none'
            }}>
              {formatTypeName(groupKey)}
              <span style={{ transform: openGroups[groupKey] ? 'rotate(90deg)' : 'rotate(0deg)', transition: 'transform 0.2s' }}>▶</span>
            </h4>
            {openGroups[groupKey] && (
              <div>
                {groupedPlugins[groupKey].map((plugin) => (
                  <div key={plugin.name} onDragStart={(e) => onDragStart(e, plugin)} draggable style={{ 
                    padding: '10px', border: '1px solid #ddd', borderRadius: '5px', 
                    marginBottom: '10px', cursor: 'grab', backgroundColor: '#f9f9f9'
                  }}>
                    <strong>{plugin.name}</strong>
                    <div style={{ fontSize: '12px', color: '#777', marginTop: '4px' }}>
                      {plugin.description.split('.')[0]}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )
      ))}
    </aside>
  );
};

export default PluginSidebar;