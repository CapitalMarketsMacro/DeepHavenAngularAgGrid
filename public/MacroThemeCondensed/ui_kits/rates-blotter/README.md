# Rates Blotter — UI Kit

Trader-facing blotter. Multi-currency rates across IRS, Gov bonds, Repo, MM, FX swaps, Futures.

## Components
- `Blotter.jsx` — `PriceBlotter`, `InstrumentSidebar`, `WorkingOrders`, `INSTRUMENTS`
- `../_shared/chrome.jsx` — `TitleBar`, `TopNav`, `StatusBar`, `Panel`, `Icon`
- `../_shared/chrome.css` — app chrome styles (panels, grid, buttons)

## Screens demonstrated
- Blotter view with live ticking prices (flash on update)
- Watchlist rail (left)
- Working orders sub-panel (bottom)
- Selection + hover + alt-row striping
- Status pills, venue tags, inline Bid/Ask/RFQ buttons per row

## Tech mapping
- AG Grid: `class="grid"` rows map to standard `ag-row-*` selectors
- Tick flash: `.flash-up` / `.flash-down` keyframes drive 600ms fades
- Density anchor: 22px rows, 11px mono, 4/8px cell padding
