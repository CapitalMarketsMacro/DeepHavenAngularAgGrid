// Shared helpers for UI kits
window.Chrome = (() => {
  const { useState, useEffect, useRef } = React;

  const Icon = ({ name, size = 16, stroke = 1.5, color = 'currentColor', style }) => {
    const ref = useRef();
    useEffect(() => {
      if (window.lucide && ref.current) {
        ref.current.innerHTML = '';
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('width', size); svg.setAttribute('height', size);
        svg.setAttribute('viewBox','0 0 24 24'); svg.setAttribute('fill','none');
        svg.setAttribute('stroke', color); svg.setAttribute('stroke-width', stroke);
        svg.setAttribute('stroke-linecap','round'); svg.setAttribute('stroke-linejoin','round');
        svg.setAttribute('data-lucide', name);
        ref.current.appendChild(svg);
        window.lucide.createIcons({ nameAttr: 'data-lucide' });
      }
    }, [name, size, stroke]);
    return <span ref={ref} style={{ display:'inline-flex', ...style }} />;
  };

  const TitleBar = ({ title = 'Macro · E-Trading' }) => (
    <div className="titlebar">
      <div className="title">
        <img src="../../assets/favicon.svg" alt="" />
        <span>{title}</span>
      </div>
      <div className="spacer" />
      <div className="wincontrols">
        <button title="Minimize">—</button>
        <button title="Maximize">▢</button>
        <button className="close" title="Close">✕</button>
      </div>
    </div>
  );

  const TopNav = ({ product, tabs = [], active, onTab, right }) => (
    <div className="topnav">
      <div className="brand">
        <img src="../../assets/logo-lockup.svg" alt="Macro" />
      </div>
      <div className="tabs">
        {tabs.map(t => (
          <div key={t.id} className={'tab' + (t.id === active ? ' active' : '')} onClick={() => onTab && onTab(t.id)}>
            <span>{t.label}</span>
            {t.count != null && <span className="count">{t.count}</span>}
          </div>
        ))}
      </div>
      <div className="right">
        {right}
        <span className="badge badge-live"><span className="dot"/>Live</span>
        <span className="user">JL</span>
      </div>
    </div>
  );

  const StatusBar = ({ items = [] }) => {
    const [clock, setClock] = useState(() => new Date());
    useEffect(() => { const t = setInterval(() => setClock(new Date()), 1000); return () => clearInterval(t); }, []);
    const t = clock.toISOString().substr(11,8);
    return (
      <div className="statusbar">
        <span className="item"><span className="dot" />FIX 4.4 · 4 venues</span>
        <span className="item muted">p99 8ms</span>
        <span className="item muted">6,142 msg/s</span>
        <span className="spacer" />
        {items.map((it, i) => <span key={i} className="item muted">{it}</span>)}
        <span className="item">UTC {t}</span>
      </div>
    );
  };

  const Panel = ({ title, actions, children, style }) => (
    <div className="panel" style={style}>
      <div className="panel-header">
        <span className="title-main">{title}</span>
        <div className="actions">{actions}</div>
      </div>
      <div className="panel-body">{children}</div>
    </div>
  );

  // Useful for price ticking. onTick(row,col,direction) lets callers flash cells.
  const useTicker = (intervalMs, cb) => {
    useEffect(() => {
      const t = setInterval(cb, intervalMs);
      return () => clearInterval(t);
    }, [intervalMs, cb]);
  };

  const fmtPrice = (n, dp = 4) => n.toFixed(dp);
  const fmtSize = (mm) => mm >= 1000 ? (mm/1000).toFixed(2)+'BN' : mm+'MM';
  const fmtPnl = (n) => (n >= 0 ? '+' : '−') + '$' + Math.abs(n).toFixed(1) + 'K';

  return { Icon, TitleBar, TopNav, StatusBar, Panel, useTicker, fmtPrice, fmtSize, fmtPnl };
})();
