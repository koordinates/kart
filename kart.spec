# -*- mode: python ; coding: utf-8 -*-

#
# Run this via cmake --build build --target bundle
#

# pyinstaller injects globals into here
# https://pyinstaller.readthedocs.io/en/stable/spec-files.html#globals-available-to-the-spec-file
# flake8: noqa E13

import os
import re
import shutil
import subprocess
import stat
import sys
from pathlib import Path

from PyInstaller.compat import is_win, is_darwin, is_linux
from PyInstaller.utils.hooks import collect_all, collect_submodules
from PyInstaller.depend import dylib
from PyInstaller.utils.hooks import collect_data_files

BINARY_DIR = os.environ.get("BINARY_DIR", "build")
USE_CLI_HELPER = os.environ.get("USE_CLI_HELPER", "ON") == "ON"

if is_win:
    lib_suffix_glob = 'dll'
    exe_suffix = '.exe'
elif is_darwin:
    lib_suffix_glob = 'dylib'
    exe_suffix = ''
elif is_linux:
    lib_suffix_glob = 'so*'
    exe_suffix = ''

if 'KART_VERSION' in os.environ:
    kart_version = os.environ["KART_VERSION"]
else:
    with open(os.path.join('kart', 'VERSION')) as version_file:
        kart_version = version_file.read().strip()

if is_win:
    with open(os.path.join('platforms', 'windows', 'version_info.rc')) as vr_template:
        vr_doc = vr_template.read()
        match = re.match(r'(\d+\.\d+(?:\.\d+)?)', kart_version)
        if not match:
            raise RuntimeError(f'Bad kart version: "{kart_version}"')
        kart_version_nums = match.group(1)
        kart_file_version = tuple(
            ([int(_v) for _v in kart_version_nums.split('.')] + [0, 0])[:4]
        )
        vr_doc = vr_doc.replace('%VERSION%', kart_version)
        vr_doc = vr_doc.replace('%VERTUPLE%', str(kart_file_version))

        with open(
            os.path.join(workpath, 'kart_version_info.rc'), 'w', encoding='utf-8'
        ) as vr:
            vr.write(vr_doc)

if is_linux:
    # TODO - these don't actually exclude these files as the check is run in
    #  a separate process
    # This needs to match the OS dependencies in platforms/linux/fpm.sh
    # We want to treat libstdc++ as a system dependency
    dylib._excludes |= {r'libstdc\+\+\.so(\..*)?'}
    # We want to treat libgcc1 as a system dependency
    dylib._excludes |= {r'libgcc_s\.so(\..*)?'}

    dylib.exclude_list = dylib.ExcludeList()

    print(
        "🏎️  Configured binary exclude-list overrides for libstdc++ & libgcc1",
        file=sys.stderr,
    )
    assert dylib.exclude_list.search('libstdc++.so.6.0.20')
    assert dylib.exclude_list.search('libgcc_s.so.1')


if is_linux or is_darwin:
    # TODO - these don't actually exclude these files as the check is run in
    #  a separate process
    # We want to treat unixODBC (libodbc) as a system dependency, since the MSSQL
    # drivers depend on it, and we don't want two different versions imported
    # in the same process.
    dylib._excludes |= {r'libodbc(\..*)?\.(dylib|so)(\..*)?'}

    dylib.exclude_list = dylib.ExcludeList()
    if is_darwin:
        dylib.exclude_list = dylib.MacExcludeList(dylib.exclude_list)

    print(
        "🏎️  Configured binary exclude-list overrides for libodbc",
        file=sys.stderr,
    )
    assert dylib.exclude_list.search('libodbc.2.dylib')
    assert dylib.exclude_list.search('libodbc.so.1')
    assert dylib.exclude_list.search('libodbc.so.2')

VENV_BIN_DIR = "Scripts" if is_win else "bin"

# Handled specially to avoid copies
symlinks = []

# Binaries: these are signed, and put in the correct place on macOS
binaries = [
    (f'{BINARY_DIR}/venv/lib/*.{lib_suffix_glob}', '.'),
    (f'{BINARY_DIR}/venv/lib/mod_spatialite.{lib_suffix_glob}', '.'),
    (f'{BINARY_DIR}/venv/{VENV_BIN_DIR}/git-lfs{exe_suffix}', '.'),
    (f'{BINARY_DIR}/venv/{VENV_BIN_DIR}/pdal{exe_suffix}', '.'),
]
if not is_win:
    binaries += [
        (f'{BINARY_DIR}/venv/bin/git', '.'),
    ]
    if USE_CLI_HELPER:
        binaries += [
            (f'{BINARY_DIR}/cli_helper/kart', '.'),
        ]


kart_version_file = os.environ.get("KART_VERSION_FILE", "kart/VERSION")

# Data files — these are copied in as-is
datas = [
    (kart_version_file, 'share/kart'),
    ('kart/diff-view.html', 'share/kart'),
    ('README.md', '.'),
    ('COPYING', '.'),
    (f'{BINARY_DIR}/venv/share/gdal', 'share/gdal'),
    (f'{BINARY_DIR}/venv/share/proj', 'share/proj'),
    (f'{BINARY_DIR}/venv/pyodbc.pyi', '.'),
    (f'{BINARY_DIR}/venv/help', 'help'),
]

if is_win:
    # entire MinGit folder
    datas += [
        (f'{BINARY_DIR}/venv/git', 'git'),
    ]
else:
    # find git binaries
    datas += [
        (f'{BINARY_DIR}/venv/share/git-core', 'share/git-core'),
    ]

    # add elf/macho binaries from git to binaries, symlinks to symlinks,
    # and the rest to datas
    git_libexec_core_root = f'{BINARY_DIR}/venv/libexec/git-core'
    for r, dl, fl in os.walk(git_libexec_core_root):
        for fn in fl:
            fp = Path(r) / fn
            rr = Path(r).relative_to(git_libexec_core_root)
            dr = Path('libexec') / 'git-core' / rr
            if fp.is_symlink():
                fp.resolve(strict=True)
                symlinks.append((str(fp), dr))
                continue

            if fp.stat().st_mode & stat.S_IXUSR:
                # shell scripts are executable, but can't be codesigned on macOS
                proc_output = subprocess.check_output(
                    ['file', '-b', str(fp)], text=True
                )
                typ = proc_output.split(' ', maxsplit=1)[0]
                if typ in ('ELF', 'Mach-O'):
                    binaries.append((str(fp), dr))
                    continue

            datas.append((str(fp), dr))


pyi_analysis = Analysis(
    ['platforms/kart_cli.py'],
    pathex=[],
    # only set kart_cli_helper as a binary for Linux or MacOS, need to
    # do here as modifying after the Analysis instance is created fails
    binaries=binaries,
    datas=datas,
    hiddenimports=[
        # TODO: improve this somehow
        *collect_submodules('kart'),
        *collect_submodules('kart.annotations'),
        *collect_submodules('kart.lfs_commands'),
        *collect_submodules('kart.point_cloud'),
        *collect_submodules('kart.sqlalchemy'),
        *collect_submodules('kart.sqlalchemy.adapter'),
        *collect_submodules('kart.tabular'),
        *collect_submodules('kart.upgrade'),
        # via pygit2
        '_cffi_backend',
        # via a cython module ???
        'csv',
        # `logging.config` is useful for ext-run modules
        *collect_submodules("logging"),
        'shellingham.posix',
        'shellingham.nt',
        *collect_submodules('sqlalchemy'),
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=[
        'ipdb',
        "_kart_env",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)

if is_linux or is_darwin:
    pyi_analysis.exclude_system_libraries(
        list_of_exceptions=['libffi*', 'libreadline*']
    )

pyi_pyz = PYZ(pyi_analysis.pure, pyi_analysis.zipped_data, cipher=None)

if is_win:
    exe_icon = 'platforms/windows/kart.ico'
    exe_name = 'kart'
else:
    exe_icon = 'platforms/macos/kart.icns'
    exe_name = 'kart_cli' if USE_CLI_HELPER else 'kart'

pyi_exe = EXE(
    pyi_pyz,
    pyi_analysis.scripts,
    [],
    exclude_binaries=True,
    name=exe_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    icon=exe_icon,
    version=os.path.join(workpath, 'kart_version_info.rc'),
    entitlements_file="platforms/macos/entitlements.plist",
)
pyi_coll = COLLECT(
    pyi_exe,
    pyi_analysis.binaries,
    pyi_analysis.zipfiles,
    pyi_analysis.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='kart',
)
pyi_app = BUNDLE(
    pyi_coll,
    name='Kart.app',
    icon='platforms/macos/kart.icns',
    bundle_identifier='com.koordinates.Sno.SnoCore',
    version=kart_version,
    info_plist={
        'NSPrincipalClass': 'NSApplication',
        'NSAppleScriptEnabled': False,
    },
)

# Ideally we'd do this before BUNDLE so it could sign it on macOS, but we
# can do that ourselves later.
if symlinks:
    if is_darwin:
        dist_bin_root = Path(DISTPATH) / 'Kart.app' / 'Contents' / 'MacOS'
    elif is_linux:
        dist_bin_root = Path(DISTPATH) / 'kart'
        print("⚠️⚠️⚠️⚠️ 'symlinks' haven't been tested on Linux")
    else:
        raise RuntimeError("Symlinks don't work well on Windows!")

    for sl, td in symlinks:
        sl, td = Path(sl), Path(td)
        tp = dist_bin_root / td

        st = sl.readlink()

        if sl.name == "git":  # git itself
            (tp / sl.name).symlink_to("../../git")
        elif str(st) == "../../bin/git":  # git-foo -> git
            (tp / sl.name).symlink_to("../../git")
        elif str(st) == st.name:  # git-foo -> git-bar
            (tp / sl.name).symlink_to(st.name)
        else:
            raise ValueError(
                "Found symlink I don't know how to handle: source={sl} -> {st}; dest={td}/{sl.name} -> ???"
            )
