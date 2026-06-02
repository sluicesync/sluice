# sluice — brand assets

Logo concept: a canal **sluice gate** — a lift-gate raised on its handwheel
over regulated water flow ("regulates flow, it doesn't generate it").

## Palette

| Role | Hex |
|------|-----|
| Primary (mark) | `#F35815` |
| Flow / accent | `#FFDCC6` |
| Wordmark on light | `#C0410A` |
| Wordmark on dark | `#FCE3D6` |
| Mark ink on dark (mono) | `#FF7A3C` |

Design: hard-edge gate (square structure), rounded water lines, connected
T-handle, flat fill.

## Primary assets

| File | Use |
|------|-----|
| `sluice-mark.svg` | App/avatar icon (orange square) |
| `sluice-logo.svg` | Horizontal lockup, light backgrounds |
| `sluice-logo-dark.svg` | Horizontal lockup, dark backgrounds |
| `sluice-wordmark.svg` | "sluice" wordmark only (`fill: currentColor`) |
| `sluice-mark-mono.svg` | Single-ink gate glyph, transparent (recolorable) |
| `sluice-avatar-1024/512/64.png` | Raster avatars (512 = GitHub org avatar) |

The wordmark is **outlined to paths** from **Inter SemiBold** (SIL OFL 1.1) —
no font dependency, renders identically everywhere.

## Favicons

| File | Use |
|------|-----|
| `favicon.ico` | Multi-res 16/32/48, classic `/favicon.ico` |
| `favicon.svg` | Modern scalable favicon |
| `favicon-32x32.png`, `favicon-16x16.png` | PNG fallbacks |
| `apple-touch-icon.png` | 180×180, iOS home screen |

Drop them at the site root and reference:

```html
<link rel="icon" href="/favicon.ico" sizes="any">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="icon" href="/favicon-32x32.png" sizes="32x32" type="image/png">
<link rel="icon" href="/favicon-16x16.png" sizes="16x16" type="image/png">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
```

