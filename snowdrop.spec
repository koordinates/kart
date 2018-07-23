

block_cipher = None

import os

def Entrypoint(dist, group, name,
               scripts=None, pathex=None, hiddenimports=None,
               hookspath=None, excludes=None, runtime_hooks=None):
    import pkg_resources

    # get toplevel packages of distribution from metadata
    def get_toplevel(dist):
        distribution = pkg_resources.get_distribution(dist)
        if distribution.has_metadata('top_level.txt'):
            return list(distribution.get_metadata('top_level.txt').split())
        else:
            return []

    hiddenimports = hiddenimports or []
    packages = []
    for distribution in hiddenimports:
        packages += get_toplevel(distribution)

    scripts = scripts or []
    pathex = pathex or []
    # get the entry point
    ep = pkg_resources.get_entry_info(dist, group, name)
    # insert path of the egg at the verify front of the search path
    pathex = [ep.dist.location] + pathex
    # script name must not be a valid module name to avoid name clashes on import
    script_path = os.path.join(workpath, name + '-script.py')
    print ("creating script for entry point", dist, group, name)
    with open(script_path, 'w') as fh:
        print("import", ep.module_name, file=fh)
        print("%s.%s()" % (ep.module_name, '.'.join(ep.attrs)), file=fh)
        for package in packages:
            print ("import", package, file=fh)

    return Analysis([script_path] + scripts, pathex, hiddenimports, hookspath, excludes, runtime_hooks)

a = Entrypoint('snowdrop',
    'console_scripts',
    'kx-sync',
    pathex=[
      os.path.abspath('./snowdrop'),
      os.path.abspath('.'),
    ]
)

#Customisations
import importlib

# Package data files
data_package_imports = [
    ('yaspin', ('data/spinners.json',)),
]
for package, files in data_package_imports:
    proot = os.path.dirname(importlib.import_module(package).__file__)
    a.datas.extend([(os.path.join(package, f), os.path.join(proot, f), 'DATA') for f in files])

# Binary libraries
a.binaries.extend([
    # Spatialite
    ('libspatialite.7.dylib', './spatialite/libspatialite-4.3.0a/src/.libs/libspatialite.7.dylib', 'BINARY'),
    ('mod_spatialite.so', './spatialite/libspatialite-4.3.0a/src/.libs/mod_spatialite.so', 'BINARY'),
    ('mod_spatialite.7.so', './spatialite/libspatialite-4.3.0a/src/.libs/mod_spatialite.7.so', 'BINARY'),
    ('libfreexl.1.dylib', '/usr/local/opt/freexl/lib/libfreexl.1.dylib', 'BINARY'),
    ('libproj.13.dylib', '/usr/local/opt/proj/lib/libproj.13.dylib', 'BINARY'),
    ('libgeos_c.1.dylib', '/usr/local/opt/geos/lib/libgeos_c.1.dylib', 'BINARY'),
    # Already included by _sqlite extension
    # ('libsqlite3.0.dylib',  ('/usr/local/opt/sqlite/lib/libsqlite3.0.dylib', 'BINARY'),
])
#EndCustomisations

pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='kx-sync',
          debug=False,
          strip=False,
          upx=True,
          runtime_tmpdir=None,
          console=True )

# Build OSX .app bundle
# app = BUNDLE(exe,
#          name='kx-sync.app',
#          icon=None,
#          bundle_identifier="com.koordinates.kx-sync")
