# curl Overlay

## Why this overlay exists

curl is pinned at 8.16.0. With curl 8.21.0 (from the 2026-07-30 vcpkg baseline),
Windows deadlocks during threaded raster tile imports (linked-storage datasets):
a native thread wedges holding the Windows DLL loader lock, which blocks all new
thread creation in the process — `Thread.start()` never returns, and CI hangs
until the job timeout. curl 8.20 rewrote its async DNS resolver (asyn-thrdd),
which is the prime suspect.

Bisected on kart CI (branch `cds-win-hang-probe`, 2026-08-02): with only curl
pinned back to 8.16.0, `tests/linked_storage/` passes threaded in ~1 minute;
with curl 8.21.0 it deadlocks every time. macOS/Linux are unaffected.

## Changes from upstream

None — this is the upstream vcpkg port as of baseline a62ce77d56 (curl 8.16.0),
kept while newer curl deadlocks on Windows. When bumping, re-test with:
`pytest tests/linked_storage/ -p no:xdist -o faulthandler_timeout=120` on Windows.
