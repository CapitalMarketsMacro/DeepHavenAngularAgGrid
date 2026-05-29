# Macro Design System — CDN distribution

Two small files, hosted anywhere (jsDelivr, unpkg, your own S3 + CloudFront).
Consumers get the whole system — tokens, element styles, PrimeNG / PrimeReact
(Aura) overrides, and AG Grid v33 theming — with one `<link>` and, for grid
apps, one `<script>`.

| File | Size | Min | Purpose |
|---|---:|---:|---|
| `macro.css`        | 30 KB | 19 KB | Tokens, base elements, Prime overrides, AG Grid companion styles |
| `macro-ag-grid.js` |  5 KB |  3 KB | AG Grid v33 Theming API object (UMD + ESM) |

Gzipped over the wire: ~6 KB + ~1.5 KB.

---

## Install

### Vanilla HTML (no bundler)

```html
<!-- 1. Macro design system — tokens + Prime + AG Grid companion styles -->
<link rel="stylesheet"
      href="https://cdn.jsdelivr.net/gh/your-org/macro-design-system@v1/dist/macro.min.css">

<!-- 2. Optional: a Prime (Aura) base theme BEFORE macro.css if you use PrimeNG/PrimeReact.
        Macro overrides its tokens — don't load a different Aura variant afterwards. -->
<link rel="stylesheet"
      href="https://cdn.jsdelivr.net/npm/primereact@10/resources/themes/aura-dark-blue/theme.css">
<link rel="stylesheet"
      href="https://cdn.jsdelivr.net/npm/primeicons@7/primeicons.css">

<!-- 3. Optional: AG Grid v33 + Macro grid theme -->
<script src="https://cdn.jsdelivr.net/npm/ag-grid-community@33/dist/ag-grid-community.min.js"></script>
<script src="https://cdn.jsdelivr.net/gh/your-org/macro-design-system@v1/dist/macro-ag-grid.min.js"></script>
<script>
  agGrid.createGrid(document.getElementById('grid'), {
    theme: window.macroTheme,
    rowData: [...],
    columnDefs: [...]
  });
</script>
```

**Load order matters.** Put Prime's Aura base CSS *before* `macro.css` — Macro
only overrides Aura's CSS custom properties, so Aura has to be there first.

### React / Vue / Angular with a bundler

```bash
# Copy the two files into your app's public/static folder,
# or reference via CDN in your index.html.
```

```ts
// main.tsx
import 'primereact/resources/themes/aura-dark-blue/theme.css';
import 'primeicons/primeicons.css';
import 'https://cdn.jsdelivr.net/gh/your-org/macro-design-system@v1/dist/macro.min.css';

// For AG Grid v33 with the new Theming API:
import { themeQuartz, iconSetMaterial, colorSchemeDarkBlue } from 'ag-grid-community';
import { buildMacroTheme } from 'https://cdn.jsdelivr.net/gh/your-org/macro-design-system@v1/dist/macro-ag-grid.js';

const macroTheme = buildMacroTheme({ themeQuartz, iconSetMaterial, colorSchemeDarkBlue });
// <AgGridReact theme={macroTheme} ... />
```

---

## What's in `macro.css`

1. **Fonts** — `@import` from Google Fonts (Roboto + IBM Plex Mono, all weights, italics, variable axes). Remove/replace if your CSP blocks third-party fonts.
2. **Design tokens** — every CSS custom property from the canonical token file: core palette, semantic colors (dark default + `.theme-light`), market directional colors, typography, spacing, radii, shadows, motion, density.
3. **Base element styles** — `body`, `h1`–`h4`, `.label`, `.caption`, `.mono`, `.num`, `.up`, `.down`, `.bid`, `.ask`, scrollbars, selection.
4. **PrimeNG / PrimeReact (Aura) overrides** — buttons (incl. `.macro-bid` / `.macro-ask`), inputs, DataTable, Dialog, Sidebar, Toast, Message, Tree/TreeTable, Chip/Tag/Badge, Checkbox/Radio/Switch, menus, density toggles (`data-density="tight" | "cozy"`).
5. **AG Grid companion styles** — tick-flash animations (`.mkt-flash-up`, `.mkt-flash-down`), directional cell classes (`.mkt-up`, `.mkt-down`, `.mkt-num`), pinned-row styling, density overrides.

The AG Grid v33 **Theming API** cannot be expressed in CSS (it's a JS object),
so the theme itself lives in `macro-ag-grid.js`. Nothing in `macro.css`
depends on the JS file — if you're using AG Grid's legacy CSS themes
(Alpine, Balham, pre-v33 Quartz), the CSS-only pieces still apply.

---

## Theme switching

```html
<html class="theme-dark"> <!-- default; tokens assume dark -->
<html class="theme-light"> <!-- override block flips surfaces + market colors -->
```

Or scoped:

```html
<div class="theme-light">…light island inside a dark app…</div>
```

Density:

```html
<div data-density="tight">…20px rows…</div>
<div data-density="cozy">…32px rows…</div>
<!-- default is our "standard" — 22px rows, 28px controls -->
```

---

## Customising per consumer

Every token is a CSS variable — override on `:root` (or any ancestor) in your
app's own stylesheet, loaded *after* `macro.css`:

```css
:root {
  --brand:       #0064d2;  /* pin to your firm's blue */
  --brand-hover: #0c78ea;
  --brand-press: #004fb0;
}
```

The AG Grid theme object reads from inlined JS constants (not CSS vars, by
design — it has to work in tree-shaken builds). If you need a firm-specific
AG Grid palette, call `buildMacroTheme(...).withParams({ accentColor: '#...' })`.

---

## Versioning

Pin to a tag in production:

```
https://cdn.jsdelivr.net/gh/your-org/macro-design-system@v1.0.0/dist/macro.min.css
```

`@v1` follows the latest `v1.x.x`. `@latest` is not recommended for prod.

---

## Files in this folder

```
dist/
├── macro.css              # readable, ~30 KB
├── macro.min.css          # minified,  ~19 KB
├── macro-ag-grid.js       # UMD + ESM, readable
├── macro-ag-grid.min.js   # UMD + ESM, minified
├── demo.html              # smoke test — proves both files work standalone
└── README.md              # this file
```
