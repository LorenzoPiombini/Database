TARGET = bin/isam.db
SRC = $(wildcard src/*.c)
OBJ = $(patsubst src/%.c, obj/%.o, $(SRC))
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

SCRIPTS = GET FILE LIST 

default: $(TARGET)


test:	
	./$(TARGET) -nf test -d name:TYPE_STRING:ls:age:TYPE_BYTE:37:addr:TYPE_STRING:"Vattella a Pesca 122":city:TYPE_STRING:"Somerville":zip_code:TYPE_STRING:07921 -k pi90 
	./$(TARGET) -d code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0 -nf item -k ui7
	 ./$(TARGET) -nf test7789 -d name:TYPE_STRING:Lorenzo:age:TYPE_BYTE:23:addr:TYPE_STRING:somerville_rd_122:city:TYPE_STRING:Bedminster:zip_code:TYPE_STRING:07921 -k jj6
	./$(TARGET) -f test -D pi90
	./$(TARGET) -nf prova -d code:TYPE_STRING:"par45-Y-us":price:TYPE_FLOAT:33.56:discount:TYPE_INT:0.0 -k45rt
	
	./$(TARGET) -f item -a price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0:code:TYPE_STRING:"par45-Y-us" -k ui8
	./$(TARGET) -f item -a price:TYPE_FLOAT:67.56:discount:TYPE_FLOAT:0.0:code:TYPE_STRING:"met90-x-us":unit:TYPE_STRING:"each":weight:TYPE_DOUBLE:45.43 -k ui9
	./$(TARGET) -nf cmc 
	./$(TARGET) -f item -a code:TYPE_STRING:nhy-X-it -k ui10
	./$(TARGET) -f item -a code:TYPE_STRING:pio-u-ES:weight:TYPE_DOUBLE:10.9 -k ui11
	./$(TARGET) -f item -a code:TYPE_STRING:"par45-Y-us":price:TYPE_FLOAT:33.56:discount:TYPE_INT:0.0 -k ui10

pause:
	@bash -c 'read -p "Press any key to continue..." -n 1 -s'

memory:
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -nf test -d name:TYPE_STRING:ls:age:TYPE_BYTE:37:addr:TYPE_STRING:"Vattella a Pesca 122":city:TYPE_STRING:"Somerville":zip_code:TYPE_STRING:07921 -k pi90
	$(MAKE) pause
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -d code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0 -nf item -k ui7
	$(MAKE) pause
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -nf test7789 -d name:TYPE_STRING:Lorenzo:age:TYPE_BYTE:23:addr:TYPE_STRING:somerville_rd_122:city:TYPE_STRING:Bedminster:zip_code:TYPE_STRING:07921 -k jj6     
	$(MAKE) pause
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -f test -D pi90
	$(MAKE) pause
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -nf prova -d code:TYPE_STRING:"par45-Y-us":price:TYPE_FLOAT:33.56:discount:TYPE_INT:0.0 -k45rt
	$(MAKE) pause
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -f item -a price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0:code:TYPE_STRING:"par45-Y-us" -k ui8
	$(MAKE) pause
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -f item -a price:TYPE_FLOAT:67.56:discount:TYPE_FLOAT:0.0:code:TYPE_STRING:"met90-x-us":unit:TYPE_STRING:"each":weight:TYPE_DOUBLE:45.43 -k ui9
	$(MAKE) pause
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -nf cmc 
	$(MAKE) pause
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -f item -a code:TYPE_STRING:nhy-X-it -k ui10
	$(MAKE) pause
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -f item -a code:TYPE_STRING:pio-u-ES:weight:TYPE_DOUBLE:10.9 -k ui11
	$(MAKE) pause
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s $(TARGET) -f item -a code:TYPE_STRING:"par45-Y-us":price:TYPE_FLOAT:33.56:discount:TYPE_INT:0.0 -k ui10
	$(MAKE) pause

clean:
	sudo rm -f $(BINDIR)/GET $(BINDIR)/LIST $(BINDIR)/FILE
	rm -f obj/*.o 
	rm -f bin/*
	rm *.dat *.inx
	rm *core*
	 
$(TARGET): $(OBJ)
	gcc -o $@ $?

obj/%.o : src/%.c
	gcc -g -c $< -o $@ -Iinclude 



$(BINDIR)/GET:
	@if [ !  -f $@ ]; then \
		echo "Creating $@ . . ."; \
		echo "#!/bin/bash" > $@; \
		echo "#Check if both arguments are provided" >> $@; \
		echo "if [ -z \"\$$1\" ] || [ -z \"\$$2\" ]; then" >> $@; \
		echo "echo \"Usage: GET [file name] [record_id]\"" >> $@; \
		echo "exit 1" >> $@; \
		echo "fi" >> $@; \
		echo "" >> $@; \
		echo "$(TARGET) -f \"\$$1\" -r \"\$$2\"" >> $@; \
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
		echo "/home/lpiombini/Cprog/low_IO/$(TARGET) -lf \"\$$1\"" >> $@; \
		chmod +x $@; \
	fi

$(BINDIR)/FILE:
	@if [ ! -f $@ ]; then \
		echo "Creating $@ . . ."; \
		echo "#!/usr/bin/env python3" > $@; \
		echo "import sys" >> $@; \
		echo "import subprocess" >> $@; \
		echo "" >> $@; \
		echo "def transform_input(input_string):" >> $@; \
		echo "    # Dictionary of replacements" >> $@; \
		echo "    replacements = {" >> $@; \
		echo "        't_b': 'TYPE_BYTE'," >> $@; \
		echo "        't_d': 'TYPE_DOUBLE'," >> $@; \
		echo "        't_f': 'TYPE_FLOAT'," >> $@; \
		echo "        't_l': 'TYPE_LONG'," >> $@; \
		echo "        't_i': 'TYPE_INT'," >> $@; \
		echo "        't_s': 'TYPE_STRING'" >> $@; \
		echo "    }" >> $@; \
		echo "" >> $@; \
		echo "    parts = input_string.split(':')" >> $@; \
		echo "    transformed_parts = []" >> $@; \
		echo "" >> $@; \
		echo "    for part in parts:" >> $@; \
		echo "        if part in replacements:" >> $@; \
		echo "            part = replacements[part]" >> $@; \
		echo "            transformed_parts.append(part)" >> $@; \
		echo "            transformed_string = ':'.join(transformed_parts)" >> $@; \
		echo "" >> $@; \
		echo "    return transformed_string" >> $@; \
		echo "" >> $@; \
		echo "def execute_command(filename, transformed_string):" >> $@; \
		echo "    command = f\"/home/lpiombini/Cprog/low_IO/$(TARGET) -nf {filename} -R '{transformed_string}'\"" >> $@; \
		echo "    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)" >> $@; \
		echo "    if result.returncode == 0:" >> $@; \
		echo "        print(result.stdout.decode())" >> $@; \
		echo "    else:" >> $@; \
		echo "        print('Error:', result.stderr.decode())" >> $@; \
		echo "if __name__ == '__main__':" >> $@; \
		echo "    if len(sys.argv) != 3:" >> $@; \
		echo "        print('Usage: ./FILE \'<filename>\' \'<string>\'')" >> $@; \
		echo "        sys.exit(1)" >> $@; \
		echo "    filename = sys.argv[1]" >> $@; \
		echo "    input_string = sys.argv[2]" >> $@; \
		echo "    transformed_string = transform_input(input_string)" >> $@; \
		echo "    execute_command(filename, transformed_string)" >> $@; \
		chmod +x $@; \
	fi
install: $(TARGET) $(BINDIR)/GET $(BINDIR)/LIST $(BINDIR)/FILE
	sudo install -m 755 $(BINDIR)/GET $(BINDIR)/LIST $(BINDIR)/FILE $(BINDIR)

.PHONY: default test memory clean install 
