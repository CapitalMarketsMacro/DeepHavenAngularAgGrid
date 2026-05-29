Macro Design System — Workspace bundle
========================================

Open `workspace.html` in any modern browser (Chrome, Edge, Firefox, Safari).
Everything is local; no internet required except for one font fallback
(see "Offline" below).

What's inside
-------------
workspace.html        Splittable workspace shell — the entry point.
colors_and_type.css   Design tokens (colors, type, spacing, motion).
fonts/                Roboto Condensed + IBM Plex Mono (.ttf).
assets/               Brand marks (logo, favicon).
ui_kits/              Six full-page UI kits — Rates Blotter, Order Ticket,
                      Sales Cockpit, Middle Office, Analytics, Admin.
themes/               Drop-in themes for AG Grid v33, PrimeNG 17+,
                      PrimeReact 10+ — each with a live storybook.
preview/              Foundation cards — type, color, spacing, components.
dist/                 Single-file CSS distributions (macro.min.css etc.)
                      and a self-hosted demo page.

How to run a demo
-----------------
1. Unzip if needed.
2. Double-click `workspace.html` (most browsers) — or serve the folder
   with any local web server, e.g.:
       python3 -m http.server 8080
   then open http://localhost:8080/workspace.html
3. Drag any divider to resize panes. Use the dropdown in each pane title
   to swap which page is shown. Use the toolbar presets ("Trading floor",
   "Theme storybooks") to load preset layouts.
4. State (pane selections + divider positions) saves to localStorage.

Offline
-------
Workspace.html, ui_kits, preview cards, and all themes load fully offline.
Two storybook pages still pull a CDN dependency:
  - themes/primereact/preview.html  (Lara theme + primeicons via unpkg)
  - themes/primeng/preview.html     (Aura theme + primeicons via unpkg)
For an air-gapped demo, save those CDN files locally and rewrite the
<link> tags. The other pages do not require internet.

Sharing
-------
The whole folder is self-contained. Zip it, attach to email, drop into
SharePoint, or unpack onto a USB stick.
