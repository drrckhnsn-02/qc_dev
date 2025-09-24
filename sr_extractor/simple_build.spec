# Simple spec file - bundles everything into ONE file
# Put SR_data_extractor.py in same folder as this spec
# Run: pyinstaller simple_build.spec

block_cipher = None

a = Analysis(
    ['SR_batch_processor.py'],
    pathex=['.'],  # Add current directory to path
    binaries=[],
    datas=[
        # Embed the extractor inside the exe
        ('SR_data_extractor.py', 'scripts'),
    ],
    hiddenimports=[
        'pandas',
        'numpy', 
        'pyarrow',
        'pyarrow.parquet',
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=['matplotlib', 'scipy', 'jupyter'],
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
    name='SR_batch_processor',
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
