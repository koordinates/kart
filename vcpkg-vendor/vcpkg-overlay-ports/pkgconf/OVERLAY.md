# pkgconf Overlay

## Why this overlay exists

When `VCPKG_FIXUP_ELF_RPATH` is enabled (as it is in `vcpkg-overlay-triplets/x64-linux.cmake`), the built pkgconf binary segfaults on x64 Linux.

see https://github.com/microsoft/vcpkg/issues/41576

This overlay disables `VCPKG_FIXUP_ELF_RPATH` specifically for pkgconf while keeping it enabled for all other packages.

## Changes from upstream

- Added `set(VCPKG_FIXUP_ELF_RPATH OFF)` at the top of `portfile.cmake`
- All other content is identical to upstream vcpkg port
