TARGET = bin/isam.db
SRC = $(wildcard src/*.c)
OBJ = $(patsubst src/%.c, obj/%.o, $(SRC))


default: $(TARGET)
	./$(TARGET) -nf test -d name:TYPE_STRING:ls:age:TYPE_BYTE:37:addr:TYPE_STRING:"Vattella a Pesca 122":city:TYPE_STRING:"Somerville":zip_code:TYPE_STRING:07921 -k pi90 
	./$(TARGET) -d code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0 -nf item -k ui7
#valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes --undef-value-errors=yes -s bin/isam.db -nf test7789 -d name:TYPE_STRING:Lorenzo:age:TYPE_BYTE:23:addr:TYPE_STRING:somerville_rd_122:city:TYPE_STRING:Bedminster:zip_code:TYPE_STRING:07921 -k jj6
	./$(TARGET) -f test -D pi90
	./$(TARGET) -nf prova -d code:TYPE_STRING:"par45-Y-us":price:TYPE_FLOAT:33.56:discount:TYPE_INT:0.0 -k45rt
	
	./$(TARGET) -f item -a price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0:code:TYPE_STRING:"par45-Y-us" -k ui8
	./$(TARGET) -f item -a code:TYPE_STRING:"par45-Y-us":price:TYPE_FLOAT:33.56:discount:TYPE_INT:0.0 -k ui8
clean:
	rm -f obj/*.o
	rm -f bin/*
	rm *.dat *.inx
	rm *core*

$(TARGET): $(OBJ)
	gcc -o $@ $?

obj/%.o : src/%.c
	gcc -c $< -o $@ -Iinclude -g

