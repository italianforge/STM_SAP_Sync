@echo off
set ENV=%1
if "%ENV%"=="" set ENV=development

echo Running SAP Sync in %ENV% environment...

if "%ENV%"=="test" (
    set ENV=test && python sync.py
) else if "%ENV%"=="prod" (
    set ENV=production && python sync.py
) else if "%ENV%"=="production" (
    set ENV=production && python sync.py
) else (
    set ENV=development && python sync.py
)
