# -*- mode: python ; coding: utf-8 -*-

# pyinstaller injects globals into here
# https://pyinstaller.readthedocs.io/en/stable/spec-files.html#globals-available-to-the-spec-file
# flake8: noqa E13

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from PyInstaller.compat import is_win, is_darwin, is_linux
from PyInstaller.utils.hooks import collect_submodules
from PyInstaller.depend import dylib


with open(os.path.join('sno', 'VERSION')) as version_file:
    kart_version = version_file.read().strip()

if is_win:
    with open(os.path.join('platforms', 'windows', 'version_info.rc')) as vr_template:
        vr_doc = vr_template.read()
        kart_version_nums = re.match(r'(\d+\.\d+(?:\.\d+)?)', kart_version).group(1)
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

if is_darwin:
    # on macOS every dylib dependency path gets rewritten to @loader_path/...,
    # which isn't much use wrt unixODBC. And PyInstaller has no useful hooks.
    # TODO: when we upgrade PyInstaller this probably needs redoing
    import macholib.util

    macholib.util._orig_in_system_path = macholib.util.in_system_path

    def kart__in_system_path(filename):
        if re.match(r'/usr/local(/opt/unixodbc)?/lib/libodbc\.\d+\.dylib$', filename):
            print(f"🏎️  Treating {filename} as a system library", file=sys.stderr)
            return True
        else:
            return macholib.util._orig_in_system_path(filename)

    macholib.util.in_system_path = kart__in_system_path


pyi_analysis = Analysis(
    ['platforms/kart_cli.py'],
    pathex=[],
    binaries=[
        ('vendor/dist/env/lib/*', '.'),
    ],
    datas=[
        ('sno/VERSION', 'sno'),
        ('sno/diff-view.html', 'sno'),
        ('README.md', '.'),
        ('COPYING', '.'),
    ],
    hiddenimports=[
        # via pygit2
        '_cffi_backend',
        # via a cython module ???
        'csv',
        *collect_submodules('sno'),
        *collect_submodules('sqlalchemy'),
    ],
    hookspath=[
        'platforms/pyi-hooks',
    ],
    runtime_hooks=[],
    excludes=[
        'ipdb',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)

# Git
if is_win:
    pyi_analysis.datas += Tree('vendor/dist/git', prefix='git')
    # GDAL/osgeo hook doesn't include Proj
    pyi_analysis.datas += Tree(
        'venv/Lib/site-packages/osgeo/data/proj',
        prefix=os.path.join('osgeo', 'data', 'proj'),
    )
else:
    pyi_analysis.binaries += [('git', 'vendor/dist/env/bin/git', 'BINARY')]
    libexec_root = 'vendor/dist/env/libexec'
    pyi_analysis.datas += Tree('vendor/dist/env/share', prefix='share')

pyi_pyz = PYZ(pyi_analysis.pure, pyi_analysis.zipped_data, cipher=None)

if is_win:
    exe_name = 'kart'
    exe_icon = 'platforms/windows/kart.ico'
else:
    exe_name = 'kart_cli'
    exe_icon = 'platforms/macos/kart.icns'

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

# PyInstaller by defaults dereferences symlinks in data directories
# Git has about 200 so it's a big size loss
# Fix it
if is_darwin:

    # fix symlinks/binaries in libexec/git-core/
    dist_bin_root = os.path.join(DISTPATH, 'Kart.app', 'Contents', 'MacOS')
    dist_resources_root = os.path.join(DISTPATH, 'Kart.app', 'Contents', 'Resources')
    dist_libexec_root = os.path.join(dist_resources_root, 'libexec')

    shutil.move(os.path.join(dist_bin_root, 'base_library.zip'), dist_resources_root)
    os.symlink(
        '../Resources/base_library.zip', os.path.join(dist_bin_root, 'base_library.zip')
    )

    os.makedirs(os.path.join(dist_libexec_root, 'git-core'))
    os.symlink('../Resources/libexec', os.path.join(dist_bin_root, 'libexec'))
    os.symlink('../../../MacOS/git', os.path.join(dist_libexec_root, 'git-core', 'git'))

    for (dir_, dirs, files) in os.walk(libexec_root):
        reldir = os.path.relpath(dir_, libexec_root)
        for f in files:
            fpath = os.path.join(dir_, f)
            relpath = os.path.join(reldir, f)
            if os.path.islink(fpath):
                link_path = os.readlink(fpath)
                if f == 'git':
                    continue  # we already created it
                if os.path.split(link_path)[1] == 'git':
                    # this symlinks to git: rewrite it
                    os.symlink(
                        os.path.split(link_path)[1],
                        os.path.join(dist_libexec_root, relpath),
                    )
                    continue
                if re.search(r'\.dylib$', f):
                    os.symlink(
                        os.path.join('../../../MacOS', os.path.split(link_path)[1]),
                        os.path.join(dist_libexec_root, relpath),
                    )
                    continue
                if not os.path.exists(fpath) and not os.path.exists(
                    os.path.join(dist_bin_root, link_path)
                ):
                    print(
                        f"🏎️  ignoring broken link {relpath} -> {link_path}",
                        file=sys.stderr,
                    )
                    # ignore broken symlinks (git-csvserver/git-shell)
                    continue
            elif subprocess.check_output(['file', '-b', fpath], text=True).startswith(
                'Mach-O'
            ):
                print(f"🏎️  relocating {relpath} to MacOS/", file=sys.stderr)
                shutil.move(fpath, dist_bin_root)
                os.symlink(
                    os.path.join('../../../MacOS', f),
                    os.path.join(dist_libexec_root, relpath),
                )
                continue

            os.makedirs(os.path.join(dist_libexec_root, reldir), exist_ok=True)
            # copy anything else (keeps symlinks too)
            shutil.copy(
                fpath, os.path.join(dist_libexec_root, relpath), follow_symlinks=False
            )

elif is_linux:
    # fix symlinks/binaries in libexec/git-core/
    dist_libexec_root = os.path.join(DISTPATH, 'kart', 'libexec')

    dist_bin_root = os.path.join(DISTPATH, 'kart')
    os.makedirs(os.path.join(dist_libexec_root, 'git-core'))
    for (dir_, dirs, files) in os.walk(libexec_root):
        reldir = os.path.relpath(dir_, libexec_root)
        for f in files:
            fpath = os.path.join(dir_, f)
            relpath = os.path.join(reldir, f)
            if os.path.islink(fpath):
                link_path = os.readlink(fpath)
                if link_path == "../../bin/git":
                    os.symlink(
                        os.path.join('../../git'),
                        os.path.join(dist_libexec_root, relpath),
                    )
                    continue
                if not os.path.exists(fpath) and not os.path.exists(
                    os.path.join(dist_bin_root, link_path)
                ):
                    print(
                        f"🏎️  ignoring broken link {relpath} -> {link_path}",
                        file=sys.stderr,
                    )
                    # ignore broken symlinks (git-csvserver/git-shell)
                    continue

            os.makedirs(os.path.join(dist_libexec_root, reldir), exist_ok=True)
            # copy anything else (keeps symlinks too)
            shutil.copy(
                fpath, os.path.join(dist_libexec_root, relpath), follow_symlinks=False
            )
