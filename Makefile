TARGET = /usr/local/bin/isam.db
SRC = $(wildcard src/*.c)
OBJ = $(patsubst src/%.c, obj/%.o, $(SRC))
OBJ_PROD = $(patsubst src/%.c, obj/%_prod.o, $(SRC))
OBJlibht = obj/debug.o  obj/hash_tbl.o
OBJlibhtPR = obj/debug_prod.o  obj/hash_tbl_prod.o
OBJlibf = obj/debug.o  obj/file.o  obj/endian.o 
OBJlibfPR = obj/debug_prod.o  obj/file_prod.o  obj/endian_prod.o 
OBJlibs = obj/debug.o  obj/str_op.o
OBJlibsPR = obj/debug_prod.o  obj/str_op_prod.o
OBJlibr = obj/debug.o  obj/record.o
OBJlibrPR = obj/debug_prod.o  obj/record_prod.o
OBJlibp = obj/debug.o  obj/sort.o obj/parse.o
OBJlibpPR = obj/debug_prod.o  obj/sort_prod.o obj/parse_prod.o
OBJlibl = obj/debug.o  obj/lock.o
OBJliblPR = obj/debug_prod.o  obj/lock_prod.o
OBJlibbst = obj/debug.o  obj/bst.o obj/str_op.o
OBJlibbstPR = obj/debug_prod.o  obj/bst_prod.o obj/str_op_prod.o
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

LIBNAMEht = ht
LIBDIR = /usr/local/lib
INCLUDEDIR = /usr/local/include
SHAREDLIBht = lib$(LIBNAMEht).so

LIBNAMEf = file
LIBDIR = /usr/local/lib
INCLUDEDIR = /usr/local/include
SHAREDLIBf = lib$(LIBNAMEf).so

LIBNAMEs = strOP
LIBDIR = /usr/local/lib
INCLUDEDIR = /usr/local/include
SHAREDLIBs = lib$(LIBNAMEs).so

LIBNAMEr = record
LIBDIR = /usr/local/lib
INCLUDEDIR = /usr/local/include
SHAREDLIBr = lib$(LIBNAMEr).so

LIBNAMEp = parse
LIBDIR = /usr/local/lib
INCLUDEDIR = /usr/local/include
SHAREDLIBp = lib$(LIBNAMEp).so

LIBNAMEl = lock
LIBDIR = /usr/local/lib
INCLUDEDIR = /usr/local/include
SHAREDLIBl = lib$(LIBNAMEl).so

LIBNAMEbst = bst
LIBDIR = /usr/local/lib
INCLUDEDIR = /usr/local/include
SHAREDLIBbst = lib$(LIBNAMEbst).so

SCRIPTS = SHOW FILE LIST WRITE UPDATE DEL DELa KEYS 

default: $(TARGET)


prod: $(TARGET)_prod

object-dir:
	@if [ ! -d ./obj ]; then\
		echo "creating object directory...";\
		mkdir -p obj ;\
	fi

check-linker-path:
	@if [ ! -f /etc/ld.so.conf.d/customtech.conf ]; then \
		echo "setting linker configuration..." ;\
		echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/customtech.conf >/dev/null ;\
		sudo ldconfig;\
	fi

library:
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBht) $(OBJlibht)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBs) $(OBJlibs)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBr) $(OBJlibr)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBf) $(OBJlibf)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBp) $(OBJlibp)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBl) $(OBJlibl)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBbst) $(OBJlibbst)

libraryPR:
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBht) $(OBJlibhtPR)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBs) $(OBJlibsPR)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBr) $(OBJlibrPR)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBf) $(OBJlibfPR)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBp) $(OBJlibpPR)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBl) $(OBJliblPR)
	sudo gcc -Wall -fPIC -shared -o $(SHAREDLIBbst) $(OBJlibbstPR)

clean:
	sudo rm -f $(BINDIR)/GET $(BINDIR)/LIST $(BINDIR)/FILE $(BINDIR)/KEYS $(BINDIR)/WRITE $(BINDIR)/UPDATE $(BINDIR)/DEL $(BINDIR)/DELa
	sudo rm -f $(INCLUDEDIR)/file.h $(INCLUDEDIR)/str_op.h $(INCLUDEDIR)/record.h $(INCLUDEDIR)/parse.h $(INCLUDEDIR)/bst.h $(INCLUDEDIR)/hash_tbl.h $(INCLUDEDIR)/lock.h 
	sudo rm -f $(LIBDIR)/$(SHAREDLIBf) $(LIBDIR)/$(SHAREDLIBs) $(LIBDIR)/$(SHAREDLIBr) $(LIBDIR)/$(SHAREDLIBp) $(LIBDIR)/$(SHAREDLIBht) $(LIBDIR)/$(SHAREDLIBl) $(LIBDIR)/$(SHAREDLIBbst) 
	sudo ldconfig
	rm -f obj/*.o 
	sudo rm -f *.so
	sudo rm -f $(TARGET)
	rm -f *.lock
	rm *.dat *.inx *.sch
	rm *core*
	 
$(TARGET): $(OBJ)
	sudo gcc -o $@ $? -fpie -pie -z relro -z now -z noexecstack -fsanitize=address 

obj/%.o : src/%.c
	sudo gcc  -Wall -Wextra -Walloca -Warray-bounds -Wnull-dereference -g3 -c $< -o $@ -Iinclude -fstack-protector-strong -D_FORTIFY_SOURCE=2 -fPIC -pie -fsanitize=address 
#	sudo gcc -Wall -g3 -c $< -o $@ -Iinclude

$(TARGET)_prod: $(OBJ_PROD)
	sudo gcc -o $@ $? -fpie -pie -z relro -z now -z noexecstack

obj/%_prod.o : src/%.c
	sudo gcc -Wall -g3 -c $< -o $@ -Iinclude -fstack-protector-strong -D_FORTIFY_SOURCE=2 -fPIC



$(BINDIR)/SHOW:
	@if [ !  -f $@ ]; then \
		echo "Creating $@ . . ."; \
		echo "#!/bin/bash" > $@; \
		echo "#Check if both arguments are provided" >> $@; \
		echo "if [ -z \"\$$1\" ] || [ -z \"\$$2\" ]; then" >> $@; \
		echo "echo \"Usage: SHOW [file name] [record_id]\"" >> $@; \
		echo "exit 1" >> $@; \
		echo "fi" >> $@; \
		echo "" >> $@; \
		echo "$(TARGET) -f \"\$$1\" -k \"\$$2\"" >> $@; \
		chmod +x $@; \
	fi
$(BINDIR)/LIST:
	@if [ !  -f $@ ]; then  \
		echo "Creating $@ . . ."; \
		echo "#!/bin/bash" > $@; \
		echo "#Check if the argument is provided" >> $@; \
		echo "if [ -z \"\$$1\" ]; then" >> $@; \
		echo "echo \"Usage: LIST [file name]\"" >> $@; \
		echo "exit 1" >> $@; \
		echo "fi" >> $@; \
		echo "" >> $@; \
		echo "$(TARGET) -lf \"\$$1\"" >> $@; \
		chmod +x $@; \
	fi

$(BINDIR)/FILE:
	@if [ ! -f $@ ]; then \
		echo "Creating $@ . . ."; \
		echo "#!/bin/bash" > $@; \
		echo "if [ -z \"\$$1\" ] || [ -z \"\$$2\" ]; then" >> $@; \
		echo "echo \"Usage: FILE [file name] [fields name and type]\"" >> $@; \
		echo "exit 1" >> $@; \
		echo "fi" >> $@; \
		echo "" >> $@; \
		echo "if [ -e \"\$$1.dat\" ]; then" >> $@; \
		echo "	 $(TARGET) -f \"\$$1\" -R \"\$$2\"" >> $@; \
		echo "else" >> $@; \
		echo "$(TARGET) -nf \"\$$1\" -R \"\$$2\"" >> $@; \
		echo "fi" >> $@; \
		chmod +x $@; \
	fi

$(BINDIR)/WRITE:
	@if [ ! -f $@ ]; then \
		echo "Creating $@ . . ."; \
		echo "#!/bin/bash" > $@; \
		echo "if [ -z \"\$$1\" ] || [ -z \"\$$2\" ] || [ -z \"\$$3\" ]; then" >> $@; \
		echo "echo \"Usage: WRITE [file name] [fields name and type] [key]\"" >> $@; \
		echo "exit 1" >> $@; \
		echo "fi" >> $@; \
		echo "$(TARGET) -f \"\$$1\" -a \"\$$2\" -k \"\$$3\" " >> $@; \
		chmod +x $@; \
	fi

$(BINDIR)/UPDATE:
	@if [ ! -f $@ ]; then \
		echo "Creating $@ . . ."; \
		echo "#!/bin/bash" > $@; \
		echo "if [ -z \"\$$1\" ] || [ -z \"\$$2\" ] || [ -z \"\$$3\" ]; then" >> $@; \
		echo "echo \"Usage: UPDATE [file name] [fields name and type] [key]\"" >> $@; \
		echo "exit 1" >> $@; \
		echo "fi" >> $@; \
		echo "$(TARGET) -uf \"\$$1\" -a \"\$$2\" -k \"\$$3\" " >> $@; \
		chmod +x $@; \
	fi
$(BINDIR)/KEYS:
	@if [ ! -f $@ ]; then \
		echo "Creating $@ . . ."; \
		echo "#!/bin/bash" > $@; \
		echo "if [ -z \"\$$1\" ]; then" >> $@; \
		echo "echo \"Usage: KEYS [file name]\"" >> $@; \
		echo "exit 1" >> $@; \
		echo "fi" >> $@; \
		echo "" >> $@; \
		echo "value=\"\$$2\"" >> $@; \
		echo "if [ -z \"\$$2\" ]; then" >> $@; \
		echo "value=0" >> $@; \
		echo "fi" >> $@; \
		echo "$(TARGET) -f \"\$$1\" -x \"\$$value\"" >> $@; \
		chmod +x $@; \
	fi
$(BINDIR)/DEL:
	@if [ ! -f $@ ]; then \
		echo "Creating $@ . . ."; \
		echo "#!/bin/bash" > $@; \
		echo "if [ -z \"\$$1\" ] || [ -z \"\$$2\" ]; then" >> $@; \
		echo "echo \"Usage: DEL [file name] [key] [index_number]\"" >> $@; \
		echo "echo \"index number is not mandatory\"" >> $@; \
		echo "echo \"if index_number is not specified index 0 is used\"" >> $@; \
		echo "exit 1" >> $@; \
		echo "fi" >> $@; \
		echo "" >> $@; \
		echo "value=\"\$$3\"" >> $@; \
		echo "if [ -z \"\$$3\" ]; then" >> $@; \
		echo "value=0" >> $@; \
		echo "fi" >> $@; \
		echo "" >> $@; \
		echo "$(TARGET) -f \"\$$1\" -k \"\$$2\" -D \"\$$value\"" >> $@; \
		chmod +x $@; \
	fi

$(BINDIR)/DELa:
	@if [ ! -f $@ ]; then \
		echo "Creating $@ . . ."; \
		echo "#!/bin/bash" > $@; \
		echo "if [ -z \"\$$1\" ]; then" >> $@; \
		echo "echo \"Usage: DELa [file name]\"" >> $@; \
   		echo "exit 1" >> $@; \
   		echo "fi" >> $@; \
		echo "$(TARGET) -f \"\$$1\" -D 0 -o all" >> $@; \
		chmod +x $@; \
	fi

install: $(TARGET) $(BINDIR)/SHOW $(BINDIR)/LIST $(BINDIR)/FILE $(BINDIR)/KEYS $(BINDIR)/WRITE $(BINDIR)/UPDATE $(BINDIR)/DEL $(BINDIR)/DELa check-linker-path
	install -d $(INCLUDEDIR)
	install -m 644 include/bst.h include/hash_tbl.h include/file.h include/str_op.h include/record.h include/parse.h include/lock.h $(INCLUDEDIR)/
	install -m 755 $(SHAREDLIBht) $(LIBDIR)
	install -m 755 $(SHAREDLIBf) $(LIBDIR)
	install -m 755 $(SHAREDLIBs) $(LIBDIR)
	install -m 755 $(SHAREDLIBr) $(LIBDIR)
	install -m 755 $(SHAREDLIBp) $(LIBDIR) 
	install -m 755 $(SHAREDLIBl) $(LIBDIR)
	install -m 755 $(SHAREDLIBbst) $(LIBDIR)
	ldconfig
	

install_prod: $(TARGET)_prod $(BINDIR)/SHOW $(BINDIR)/LIST $(BINDIR)/FILE $(BINDIR)/KEYS $(BINDIR)/WRITE $(BINDIR)/UPDATE $(BINDIR)/DEL $(BINDIR)/DELa check-linker-path
	install -d $(INCLUDEDIR)
	install -m 644 include/bst.h include/hash_tbl.h include/file.h include/str_op.h include/record.h include/parse.h include/lock.h $(INCLUDEDIR)/
	install -m 755 $(SHAREDLIBht) $(LIBDIR)
	install -m 755 $(SHAREDLIBf) $(LIBDIR)
	install -m 755 $(SHAREDLIBs) $(LIBDIR)
	install -m 755 $(SHAREDLIBr) $(LIBDIR)
	install -m 755 $(SHAREDLIBp) $(LIBDIR) 
	install -m 755 $(SHAREDLIBl) $(LIBDIR)
	install -m 755 $(SHAREDLIBbst) $(LIBDIR)
	ldconfig
build: object-dir default library install

build_prod: object-dir prod libraryPR install_prod 

.PHONY: default test memory clean install library check-linker-path object-dir prod $(BINDIR)/SHOW $(BINDIR)/LIST $(BINDIR)/FILE $(BINDIR)/KEYS $(BINDIR)/WRITE $(BINDIR)/UPDATE $(BINDIR)/DEL $(BINDIR)/DELa
