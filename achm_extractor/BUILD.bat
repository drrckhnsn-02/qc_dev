@echo off
setlocal
cls

echo ============================================================
echo            ACHM PROCESSOR - PORTABLE BUILDER V2
echo            No Python Installation Required!
echo ============================================================
echo.

:: Use embedded Python - no installation needed
set PATH=%~dp0python_embed;%~dp0python_embed\Scripts;%PATH%

echo [1/4] Using embedded Python...
python_embed\python.exe --version

:: Clean old builds
if exist "build" rmdir /s /q build
if exist "dist" rmdir /s /q dist

echo.
echo [2/4] Preparing build environment...

:: Build the executable
echo.
echo [3/4] Building ACHM_batch_processor.exe...
echo      This may take 3-5 minutes...
echo      Note: UPX disabled for antivirus compatibility
echo.

python_embed\python.exe -m PyInstaller achm_build.spec

:: Check success
if not exist "dist\ACHM_batch_processor.exe" (
    echo.
    echo ERROR: Build failed!
    pause
    exit /b 1
)

:: Test
echo.
echo [4/4] Testing executable...
dist\ACHM_batch_processor.exe --help >nul 2>&1
if errorlevel 1 (
    echo      Warning: Test had issues
) else (
    echo      Test passed!
)

echo.
echo ============================================================
echo                    BUILD COMPLETE!
echo ============================================================
echo.
echo Executable created: dist\ACHM_batch_processor.exe
echo Size: 
dir dist\*.exe | find "ACHM"
echo.
echo Copy this file to your qc_extractors folder
echo.
pause
endlocal