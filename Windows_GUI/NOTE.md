I want to use C, but give a modern look to the app.

so the compilation instructions:

windres resource.rc -o resource.o
gcc -o modern_gui.exe .\standard_windows_program_structure.c resource.o -mwindows -lcomctl32


and the app manifest will tell Windows to use version 6 of the Common Control library. the file will be attached to the executable.
