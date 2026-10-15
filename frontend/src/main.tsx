import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './styles.css';

// 应用入口：挂载路由与全局认证上下文
ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
