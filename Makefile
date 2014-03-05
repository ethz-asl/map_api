all: mapapi

mapapi:
	mkdir -p build
	cd build && cmake  -G"Eclipse CDT4 - Unix Makefiles" ../src

