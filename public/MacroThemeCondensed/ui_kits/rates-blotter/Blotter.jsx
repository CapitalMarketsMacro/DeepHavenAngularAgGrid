// Rates Blotter components
const { useState, useMemo, useEffect } = React;
const { Icon, Panel } = Chrome;

// Seed data for the rates blotter
const INSTRUMENTS = [
  { id: 'USD-2Y',  name: 'USD 2Y SOFR',   tenor: '2Y',  ccy: 'USD', ac: 'IRS',  bid: 4.1820, ask: 4.1840, venue: 'TradeWeb' },
  { id: 'USD-5Y',  name: 'USD 5Y SOFR',   tenor: '5Y',  ccy: 'USD', ac: 'IRS',  bid: 4.1090, ask: 4.1120, venue: 'TradeWeb' },
  { id: 'USD-10Y', name: 'USD 10Y SOFR',  tenor: '10Y', ccy: 'USD', ac: 'IRS',  bid: 4.2375, ask: 4.2395, venue: 'BBG' },
  { id: 'USD-30Y', name: 'USD 30Y SOFR',  tenor: '30Y', ccy: 'USD', ac: 'IRS',  bid: 4.4510, ask: 4.4540, venue: 'TradeWeb' },
  { id: 'EUR-2Y',  name: 'EUR 2Y ESTR',   tenor: '2Y',  ccy: 'EUR', ac: 'IRS',  bid: 2.4210, ask: 2.4230, venue: 'MarketAxess' },
  { id: 'EUR-5Y',  name: 'EUR 5Y ESTR',   tenor: '5Y',  ccy: 'EUR', ac: 'IRS',  bid: 2.1830, ask: 2.1850, venue: 'TradeWeb' },
  { id: 'EUR-10Y', name: 'EUR 10Y ESTR',  tenor: '10Y', ccy: 'EUR', ac: 'IRS',  bid: 2.3410, ask: 2.3430, venue: 'TradeWeb' },
  { id: 'GBP-5Y',  name: 'GBP 5Y SONIA',  tenor: '5Y',  ccy: 'GBP', ac: 'IRS',  bid: 4.0180, ask: 4.0205, venue: 'BBG' },
  { id: 'GBP-10Y', name: 'GBP 10Y SONIA', tenor: '10Y', ccy: 'GBP', ac: 'IRS',  bid: 4.5110, ask: 4.5130, venue: 'BBG' },
  { id: 'DBR-34',  name: 'DBR 2.1 02/34', tenor: '10Y', ccy: 'EUR', ac: 'Gov',  bid: 99.42,  ask: 99.46,  venue: 'MTS' },
  { id: 'UST-34',  name: 'UST 3.875 08/34',tenor:'10Y', ccy: 'USD', ac: 'Gov',  bid: 98.18,  ask: 98.22,  venue: 'BrokerTec' },
  { id: 'UKT-33',  name: 'UKT 4.625 10/33',tenor:'10Y', ccy: 'GBP', ac: 'Gov',  bid: 102.35, ask: 102.40, venue: 'BrokerTec' },
  { id: 'RP-USD',  name: 'USD Repo O/N',  tenor: 'O/N', ccy: 'USD', ac: 'Repo', bid: 5.3200, ask: 5.3250, venue: 'BrokerTec' },
  { id: 'FXSW-EUR',name: 'EUR/USD 3M FWD',tenor: '3M',  ccy: 'EUR', ac: 'FX',   bid: 0.00234,ask: 0.00238,venue: '360T' },
  { id: 'FXSW-GBP',name: 'GBP/USD 3M FWD',tenor: '3M',  ccy: 'GBP', ac: 'FX',   bid: 0.00189,ask: 0.00192,venue: '360T' },
  { id: 'MM-USD',  name: 'USD 3M Libor',  tenor: '3M',  ccy: 'USD', ac: 'MM',   bid: 5.3850, ask: 5.3870, venue: 'BBG' },
  { id: 'FUT-TY',  name: 'TY Dec-26',     tenor: 'FUT', ccy: 'USD', ac: 'Fut',  bid: 110.18, ask: 110.19, venue: 'CME' },
  { id: 'FUT-RX',  name: 'RX Dec-26',     tenor: 'FUT', ccy: 'EUR', ac: 'Fut',  bid: 132.85, ask: 132.86, venue: 'Eurex' },
];

function PriceBlotter() {
  const [rows, setRows] = useState(() => INSTRUMENTS.map(i => ({ ...i, last: (i.bid + i.ask)/2, chg: 0, flash: null, flashCell: null })));
  const [selected, setSelected] = useState('USD-10Y');
  const [filter, setFilter] = useState('ALL');

  // Ticker — every 900ms flip a few random prices
  useEffect(() => {
    const t = setInterval(() => {
      setRows(prev => {
        const next = prev.map(r => ({ ...r, flashCell: null }));
        const n = 3 + Math.floor(Math.random()*3);
        for (let i = 0; i < n; i++) {
          const idx = Math.floor(Math.random() * next.length);
          const r = { ...next[idx] };
          const step = r.ac === 'Gov' ? 0.01 : r.ac === 'FX' ? 0.000005 : 0.0005;
          const dir = Math.random() > 0.5 ? 1 : -1;
          const delta = dir * step * (1 + Math.floor(Math.random()*3));
          const col = Math.random() > 0.5 ? 'bid' : 'ask';
          r[col] = Math.max(0, r[col] + delta);
          r.last = (r.bid + r.ask) / 2;
          r.chg = r.chg + delta;
          r.flash = dir > 0 ? 'up' : 'down';
          r.flashCell = col;
          next[idx] = r;
        }
        return next;
      });
    }, 900);
    return () => clearInterval(t);
  }, []);

  const filtered = filter === 'ALL' ? rows : rows.filter(r => r.ac === filter);

  return (
    <>
      <Panel
        title="Rates Blotter"
        actions={
          <>
            {['ALL','IRS','Gov','Repo','FX','MM','Fut'].map(f =>
              <button key={f} className={'btn btn-xs ' + (filter === f ? 'btn-primary' : 'btn-ghost')} onClick={()=>setFilter(f)}>{f}</button>)}
            <span style={{width:8}} />
            <button className="btn btn-sm"><Icon name="columns-3" size={12}/> Columns</button>
            <button className="btn btn-sm"><Icon name="filter" size={12}/> Filter</button>
          </>
        }
      >
        <table className="grid">
          <thead><tr>
            <th style={{width:30}}></th>
            <th className="pinned" style={{minWidth:180}}>Instrument</th>
            <th style={{width:50}}>Tenor</th>
            <th style={{width:50}}>Ccy</th>
            <th style={{width:50}}>Class</th>
            <th className="num-r" style={{width:90}}>Bid</th>
            <th className="num-r" style={{width:90}}>Ask</th>
            <th className="num-r" style={{width:90}}>Last</th>
            <th className="num-r" style={{width:90}}>Chg</th>
            <th style={{width:100}}>Venue</th>
            <th style={{width:100}}>Status</th>
            <th style={{width:120}}></th>
          </tr></thead>
          <tbody>
            {filtered.map((r, i) => {
              const dp = r.ac === 'Gov' || r.ac === 'Fut' ? 2 : r.ac === 'FX' ? 5 : 4;
              const sel = r.id === selected;
              return (
                <tr key={r.id} className={(sel ? 'selected ' : '') + (i%2 ? 'alt' : '')} onClick={()=>setSelected(r.id)}>
                  <td className="pinned" style={{textAlign:'center',color:'var(--fg-3)'}}>{i+1}</td>
                  <td className="pinned" style={{fontWeight:500}}>{r.name}</td>
                  <td><span className="tag">{r.tenor}</span></td>
                  <td className="muted">{r.ccy}</td>
                  <td className="muted">{r.ac}</td>
                  <td className={'num-r bid ' + (r.flashCell==='bid' ? ('flash-'+r.flash) : '')}>{r.bid.toFixed(dp)}</td>
                  <td className={'num-r ask ' + (r.flashCell==='ask' ? ('flash-'+r.flash) : '')}>{r.ask.toFixed(dp)}</td>
                  <td className="num-r">{r.last.toFixed(dp)}</td>
                  <td className={'num-r ' + (r.chg >= 0 ? 'up' : 'down')}>{(r.chg >= 0 ? '+' : '−') + Math.abs(r.chg).toFixed(dp)}</td>
                  <td className="muted">{r.venue}</td>
                  <td><span className="badge badge-live"><span className="dot"/>Live</span></td>
                  <td>
                    <button className="btn btn-xs btn-bid">Bid</button>{' '}
                    <button className="btn btn-xs btn-ask">Ask</button>{' '}
                    <button className="btn btn-xs btn-ghost">RFQ</button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </Panel>
    </>
  );
}

function InstrumentSidebar({ groups }) {
  return (
    <Panel title="Watchlists" actions={<button className="btn btn-xs btn-ghost"><Icon name="plus" size={10}/></button>} style={{width:220,flex:'0 0 auto'}}>
      <div style={{padding:'8px 4px'}}>
        {groups.map(g => (
          <div key={g.name} style={{marginBottom:12}}>
            <div className="label" style={{padding:'4px 10px'}}>{g.name}</div>
            {g.items.map(it => (
              <div key={it} style={{padding:'4px 10px',fontSize:12,display:'flex',justifyContent:'space-between',cursor:'pointer',borderRadius:3}} onMouseEnter={e=>e.currentTarget.style.background='var(--bg-grid-hover)'} onMouseLeave={e=>e.currentTarget.style.background=''}>
                <span>{it}</span>
                <span className="num muted" style={{fontSize:11}}>{(Math.random()*5+1).toFixed(3)}</span>
              </div>
            ))}
          </div>
        ))}
      </div>
    </Panel>
  );
}

function WorkingOrders() {
  const [orders] = useState([
    { id: '8431-A', inst: 'USD 10Y SOFR', side: 'Buy',  size: '25MM', price: '4.2380', filled: '10MM', venue: 'TradeWeb', status: 'working', time: '14:22:07' },
    { id: '8431-B', inst: 'DBR 2.1 02/34',side: 'Sell', size: '50MM', price: '99.45',  filled: '0',    venue: 'MTS',      status: 'working', time: '14:21:55' },
    { id: '8430-C', inst: 'UST 3.875 08/34',side:'Buy', size: '100MM',price: '98.20',  filled: '100MM',venue: 'BrokerTec',status: 'filled',  time: '14:18:02' },
    { id: '8429-D', inst: 'GBP 5Y SONIA', side: 'Sell', size: '10MM', price: '4.0200', filled: '0',    venue: 'BBG',      status: 'rejected',time: '14:15:22' },
    { id: '8428-E', inst: 'EUR 10Y ESTR', side: 'Buy',  size: '30MM', price: '2.3420', filled: '30MM', venue: 'TradeWeb', status: 'filled',  time: '14:11:14' },
  ]);
  return (
    <Panel title="Working orders" actions={<><button className="btn btn-xs btn-ghost"><Icon name="x" size={10}/> Cancel all</button></>}>
      <table className="grid">
        <thead><tr>
          <th>ID</th><th>Instrument</th><th>Side</th>
          <th className="num-r">Size</th><th className="num-r">Price</th>
          <th className="num-r">Filled</th><th>Venue</th><th>Status</th><th>Time</th>
        </tr></thead>
        <tbody>
          {orders.map(o => (
            <tr key={o.id}>
              <td style={{fontWeight:500}}>{o.id}</td>
              <td>{o.inst}</td>
              <td className={o.side==='Buy'?'up':'down'}>{o.side}</td>
              <td className="num-r">{o.size}</td>
              <td className="num-r">{o.price}</td>
              <td className="num-r muted">{o.filled}</td>
              <td className="muted">{o.venue}</td>
              <td><span className={'badge badge-' + o.status}><span className="dot"/>{o.status[0].toUpperCase()+o.status.slice(1)}</span></td>
              <td className="muted">{o.time}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  );
}

Object.assign(window, { PriceBlotter, InstrumentSidebar, WorkingOrders, INSTRUMENTS });
