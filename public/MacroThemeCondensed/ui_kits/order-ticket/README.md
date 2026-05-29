# Order / RFQ Ticket — UI Kit

Two ticket variants: **Order** (direct-route) and **RFQ** (multi-dealer quote request).

## Components (inline in `index.html`)
- `OrderTicket` — side toggle, mode strip, size/price/TIF/venue/account fields, live-mid quote card
- `RFQTicket` — 6-dealer quote list with per-dealer status (Quoted/Waiting/Pass), best bid/ask summary, hit/lift buttons

Keyboard-first: ⌘⏎ submit · esc cancel · ↑↓ step price.
