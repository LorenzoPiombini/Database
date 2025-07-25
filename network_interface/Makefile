TARGET = db_listener
SRC = $(wildcard src/*.c)
OBJ = $(patsubst src/%.c, obj/%.o, $(SRC))



default: $(TARGET)

clean:
	rm obj/*.*
	rm $(TARGET)

$(TARGET): $(OBJ)
	sudo gcc -o $@ $? -z relro -z now -z noexecstack -fsanitize=address 

obj/%.o : src/%.c
	sudo gcc  -Wall -Wextra -Walloca -Warray-bounds -Wnull-dereference -g3 -c $< -o $@ -Iinclude -fstack-protector-strong -D_FORTIFY_SOURCE=2 -fPIC -fsanitize=address 
