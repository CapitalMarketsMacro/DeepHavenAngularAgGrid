# Macro fonts (self-hosted)

This folder ships the **minimum useful weights** for the Macro design system:

| Family | Weights | Files |
|---|---|---|
| Roboto Condensed | 400, 500, 700 | `RobotoCondensed-Regular.ttf`, `RobotoCondensed-Medium.ttf`, `RobotoCondensed-Bold.ttf` |
| IBM Plex Mono | 400, 500, 700 | `IBMPlexMono-Regular.ttf`, `IBMPlexMono-Medium.ttf`, `IBMPlexMono-Bold.ttf` |

Pair these with `dist/macro.self-hosted.css` (or its minified sibling). The CSS expects this folder to live at `./fonts/` relative to the stylesheet — drop `dist/` and `dist/fonts/` together on your CDN and you're done.

## Adding more weights or italics

Need ExtraLight, Light, SemiBold, Black, italics, or a variable Roboto? Drop the additional `.ttf` (or `.woff2`) files in this folder and append `@font-face` declarations to your copy of `macro.self-hosted.css`. Pattern:

```css
@font-face {
  font-family: "Roboto";
  src: url("./fonts/Roboto-Light.ttf") format("truetype");
  font-weight: 300;
  font-style: normal;
  font-display: swap;
}
```

## WOFF2 (recommended for production)

TTFs are fine for desktop apps and OpenFin. For browsers, convert to WOFF2 to drop bundle size ~70%:

```bash
# requires woff2 package: brew install woff2 / apt install woff2
woff2_compress dist/fonts/Roboto-Regular.ttf
woff2_compress dist/fonts/IBMPlexMono-Regular.ttf
# ...etc
```

Then change the `format("truetype")` declarations to `format("woff2")` and the file extensions in your CSS.

## Licensing

Both families are SIL Open Font License 1.1 — free for commercial use, including embedding in proprietary trading apps. The OFL.txt files from the original distributions live in the project at `uploads/IBM_Plex_Mono,Roboto/`.
