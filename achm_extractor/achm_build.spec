# ACHM PyInstaller spec file
# Run: pyinstaller achm_build.spec

import os
from PyInstaller.utils.hooks import collect_dynamic_libs, collect_data_files

block_cipher = None

# Collect h5py dynamic libraries
h5py_binaries = collect_dynamic_libs('h5py')
numpy_binaries = collect_dynamic_libs('numpy')

a = Analysis(
    ['achm_batch_processor_portable.py'],
    pathex=['.'],
    binaries=h5py_binaries + numpy_binaries,
    datas=[
        # Embed the extractor inside the exe
        ('achm_extractor_v2.py', 'scripts'),
    ],
    hiddenimports=[
        'h5py',
        'h5py._hl',
        'h5py._errors',
        'h5py.h5ac',
        'h5py.defs',
        'h5py.utils',
        'h5py._proxy',
        'numpy',
        'pandas',
        'scipy',
        'scipy.spatial.transform._rotation_groups',
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=['matplotlib', 'jupyter', 'IPython'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(
    a.pure,
    a.zipped_data,
    cipher=block_cipher,
)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='ACHM_batch_processor',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)