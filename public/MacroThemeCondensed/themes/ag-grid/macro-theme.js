/* =========================================================================
   Macro Theme for AG Grid v33+ (new Theming API)
   -------------------------------------------------------------------------
   Usage (ESM):

     import { themeQuartz } from 'ag-grid-community';
     import { macroTheme }  from './macro-theme.js';
     <AgGridReact theme={macroTheme} rowData={...} columnDefs={...} />

   Usage (UMD / script tag):

     <script src="https://cdn.jsdelivr.net/npm/ag-grid-community@33/dist/ag-grid-community.min.js"></script>
     <script src="./macro-theme.js"></script>
     new agGrid.createGrid(gridDiv, { theme: window.macroTheme, ... });

   Pair with ./macro-overrides.css for tick-flash animations, pinned-row styles,
   and density-toggle support.
   ========================================================================= */

(function (global) {
  const { themeQuartz, iconSetMaterial, colorSchemeDarkBlue } = global.agGrid || {};
  if (!themeQuartz) {
    console.warn('[macro-theme] AG Grid v33 not detected — skipping theme registration.');
    return;
  }

  // Tokens must mirror colors_and_type.css. We inline them rather than reading
  // from getComputedStyle so the theme can be used in tree-shaken React builds.
  const T = {
    bg_app:     '#0b0d12',
    bg_canvas:  '#12141a',
    bg_panel:   '#181b22',
    bg_raised:  '#1e222a',
    bg_row:     '#12141a',
    bg_row_alt: '#181b22',
    bg_hover:   '#22262f',
    bg_sel:     '#1a2a3f',
    fg1:        '#e6e8ec',
    fg2:        '#a8afbd',
    fg3:        '#6f7687',
    fg4:        '#4a5060',
    border1:    '#2a2f39',
    border2:    '#363c48',
    border_grid:'#1c2029',
    brand:      '#2aa6e6',   // Macro Cerulean 400
    brand_soft: 'rgba(42,166,230,0.14)',
    mkt_up:     '#34d97a',
    mkt_down:   '#ff6b64',
  };

  const macroTheme = themeQuartz
    .withPart(iconSetMaterial)
    .withPart(colorSchemeDarkBlue)
    .withParams({
      // --- Surfaces ---
      backgroundColor:                T.bg_canvas,
      foregroundColor:                T.fg1,
      chromeBackgroundColor:          T.bg_panel,
      headerBackgroundColor:          T.bg_panel,
      headerTextColor:                T.fg3,
      borderColor:                    T.border_grid,
      wrapperBorderRadius:            0,
      rowBorder:                      { style: 'solid', width: 1, color: T.border_grid },
      columnBorder:                   { style: 'none' },

      // --- Row states ---
      oddRowBackgroundColor:          T.bg_row_alt,
      rowHoverColor:                  T.bg_hover,
      selectedRowBackgroundColor:     T.bg_sel,

      // --- Typography ---
      fontFamily:                     "'IBM Plex Mono', 'JetBrains Mono', ui-monospace, monospace",
      fontSize:                       12,
      headerFontFamily:               "'Roboto', system-ui, sans-serif",
      headerFontSize:                 10,
      headerFontWeight:               500,
      cellHorizontalPadding:          10,

      // --- Density (default = dense; overrides via data-density below) ---
      rowHeight:                      22,
      headerHeight:                   28,
      listItemHeight:                 22,

      // --- Brand accents ---
      accentColor:                    T.brand,
      focusShadow:                    `0 0 0 2px ${T.bg_canvas}, 0 0 0 4px ${T.brand}`,
      rangeSelectionBackgroundColor:  T.brand_soft,
      rangeSelectionBorderColor:      T.brand,

      // --- Checkbox / input ---
      checkboxCheckedBackgroundColor: T.brand,
      checkboxCheckedBorderColor:     T.brand,
      inputBackgroundColor:           T.bg_canvas,
      inputBorder:                    { style: 'solid', width: 1, color: T.border2 },
      inputFocusBorder:               { style: 'solid', width: 1, color: T.brand },

      // --- Tooltip / menu / popover ---
      menuBackgroundColor:            T.bg_raised,
      menuBorder:                     { style: 'solid', width: 1, color: T.border2 },
      menuShadow:                     '0 4px 16px rgba(0,0,0,0.40)',
      tooltipBackgroundColor:         T.bg_raised,
      tooltipTextColor:               T.fg1,
      tooltipBorder:                  { style: 'solid', width: 1, color: T.border2 },

      // --- Directional column helpers (see macro-overrides.css for cellClass usage) ---
    });

  global.macroTheme = macroTheme;
  if (typeof module !== 'undefined' && module.exports) module.exports = { macroTheme };
})(typeof window !== 'undefined' ? window : globalThis);
