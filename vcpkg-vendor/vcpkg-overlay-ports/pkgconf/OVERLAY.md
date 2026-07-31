# pkgconf Overlay

## Why this overlay exists

Kart's Linux CI sets `PKG_CONFIG` (job-wide) to the vcpkg-installed pkgconf:
`.../vcpkg/installed/<triplet>/tools/pkgconf/pkgconf`.

`vcpkg_fixup_pkgconfig()`'s validation check honors `$PKG_CONFIG` — but when
pkgconf itself is being built, that path doesn't exist yet, so the check fails
with "no such file or directory". Upstream CI doesn't set `PKG_CONFIG`, so
upstream never hits this chicken-and-egg problem.

## Changes from upstream

- `vcpkg_fixup_pkgconfig()` → `vcpkg_fixup_pkgconfig(SKIP_CHECK)` in `portfile.cmake`
- All other content is identical to the upstream vcpkg port

Note: this overlay previously also set `VCPKG_FIXUP_ELF_RPATH OFF` to work
around a patchelf segfault (microsoft/vcpkg#41576). That was dropped to test
whether patchelf 0.19 (bundled since microsoft/vcpkg#52638) fixes it — if
pkgconf starts segfaulting during other ports' builds on x64 Linux, restore
`set(VCPKG_FIXUP_ELF_RPATH OFF)` at the top of `portfile.cmake`.
