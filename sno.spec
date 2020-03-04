# -*- mode: python ; coding: utf-8 -*-

import os
import platform
import shutil
import subprocess

share_toc = Tree('vendor/dist/env/share', prefix='share')
libexec_root = 'vendor/dist/env/libexec'

with open(os.path.join('sno', 'VERSION')) as version_file:
    sno_version = version_file.read().strip()

a = Analysis(
    ['platforms/sno_cli.py'],
    pathex=[],
    binaries=[
        ('vendor/dist/env/bin/git', '.'),
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
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)
a.datas += share_toc

pyz = PYZ(a.pure, a.zipped_data, cipher=None)
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='sno_cli',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='sno',
)
app = BUNDLE(
    coll,
    name='Sno.app',
    icon='platforms/macos/sno.icns',
    bundle_identifier='com.koordinates.Sno.SnoCore',
    version=sno_version,
    info_plist={
        'NSPrincipalClass': 'NSApplication',
        'NSAppleScriptEnabled': False,
    },
)

# PyInstaller by defaults dereferences symlinks in data directories
# Git has about 200 so it's a big size loss
# Fix it
if platform.system() == "Darwin":

    # fix symlinks/binaries in libexec/git-core/
    dist_bin_root = os.path.join(DISTPATH, 'Sno.app', 'Contents', 'MacOS')
    dist_resources_root = os.path.join(DISTPATH, 'Sno.app', 'Contents', 'Resources')
    dist_libexec_root = os.path.join(dist_resources_root, 'libexec')

    shutil.move(os.path.join(dist_bin_root, 'base_library.zip'), dist_resources_root)
    os.symlink('../Resources/base_library.zip', os.path.join(dist_bin_root, 'base_library.zip'))

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
                    os.symlink('git', os.path.join(dist_libexec_root, relpath))
                    continue
                if not os.path.exists(fpath):
                    print(f"ignoring broken link {relpath}")
                    # ignore broken symlinks (git-csvserver/git-shell)
                    continue
            elif subprocess.check_output(['file', '-b', fpath], text=True).startswith('Mach-O'):
                print(f"relocating {relpath} to MacOS/")
                shutil.move(fpath, dist_bin_root)
                os.symlink(
                    os.path.join('../../../MacOS', f),
                    os.path.join(dist_libexec_root, relpath)
                )
                continue

            os.makedirs(os.path.join(dist_libexec_root, reldir), exist_ok=True)
            # copy anything else (keeps symlinks too)
            shutil.copy(fpath, os.path.join(dist_libexec_root, relpath), follow_symlinks=False)

elif platform.system() == "Linux":
    dist_libexec_root = os.path.join(DISTPATH, 'sno', 'libexec')
    shutil.copytree(libexec_root, dist_libexec_root, symlinks=True)
