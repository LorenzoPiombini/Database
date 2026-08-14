@echo off
setlocal enabledelayedexpansion

:: Set the compiler and the final output executable name
set CC=gcc
set OUT=isam_db.exe
set dir=src\
set obj=obj\
:: List of all source files
set SOURCES=build.c journal.c common.c crud.c date.c debug.c endian.c file.c globals.c hash_tbl.c helper.c input.c  key.c lock.c main.c parse.c record.c sort.c string_utilities.c str_op.c

echo Starting compilation...

:: Compile each source file into an object file
for %%f in (%SOURCES%) do (
    echo Compiling %%f...
    gcc -g3 -c %dir%%%f -o %obj%%%~nf.o -Iinclude
    
    :: Stop if any compilation fails
    if !ERRORLEVEL! neq 0 (
        echo.
        echo Build failed during compilation of %%f.
        goto :error
    )
)
echo.
echo Linking object files into %OUT%...
%CC% -g3 .\obj\*.o -o %OUT%

copy %OUT% C:\Users\loren\bin\
if %ERRORLEVEL% neq 0 (
    echo.
    echo Build failed during linking.
    goto :error
)

echo.
echo Build successful: %OUT%
goto :EOF

:error
exit /b 1
