# -*- mode: python ; coding: utf-8 -*-

import os
import platform
import shutil
import subprocess

with open(os.path.join('sno', 'VERSION')) as version_file:
    sno_version = version_file.read().strip()

if platform.system() == "Windows":
    with open(os.path.join('platforms', 'windows', 'version_info.rc')) as vr_template:
        vr_doc = vr_template.read()
        sno_version_nums = re.match(r'(\d+\.\d+(?:\.\d+)?)', sno_version).group(1)
        sno_file_version = tuple(([int(_v) for _v in sno_version_nums.split('.')] + [0, 0])[:4])
        vr_doc = vr_doc.replace('%VERSION%', sno_version)
        vr_doc = vr_doc.replace('%VERTUPLE%', str(sno_file_version))

        with open(os.path.join(workpath, 'sno_version_info.rc'), 'w', encoding='utf-8') as vr:
            vr.write(vr_doc)

a = Analysis(
    ['platforms/sno_cli.py'],
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
    ],
    hookspath=[
        'platforms/pyi-hooks',
    ],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)

if platform.system() == "Windows":
    a.datas += Tree('vendor/dist/git', prefix='git')
else:
    a.binaries += [('git', 'vendor/dist/env/bin/git', 'BINARY')]
    libexec_root = 'vendor/dist/env/libexec'
    a.datas += Tree('vendor/dist/env/share', prefix='share')

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

if platform.system() == "Windows":
    exe_name = 'sno'
    exe_icon = 'platforms/windows/sno.ico'
else:
    exe_name = 'sno_cli'
    exe_icon = 'platforms/macos/sno.icns'

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name=exe_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    icon=exe_icon,
    version=os.path.join(workpath, 'sno_version_info.rc'),
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
