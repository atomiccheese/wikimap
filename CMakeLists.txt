cmake_minimum_required(VERSION 2.7)
project(wikimap)

#set(CMAKE_C_COMPILER "/usr/bin/clang")
#set(CMAKE_CXX_COMPILER "/usr/bin/clang")
#set(CMAKE_LINKER "/usr/bin/llvm-ld")
set(CMAKE_CXX_FLAGS "--std=c++11 ${CMAKE_CXX_FLAGS}")

find_package(Boost REQUIRED COMPONENTS system thread chrono atomic regex
	iostreams)
find_package(LibXml2 REQUIRED)

include_directories(${LIBXML2_INCLUDE_DIR}
	${BOOST_INCLUDE_DIR})
set(COMMON_SRC
	src/strtree.cpp
	src/bytes.cpp
	)

add_executable(preprocess src/preprocess.cpp ${COMMON_SRC})
add_executable(search src/search.cpp ${COMMON_SRC})

target_link_libraries(preprocess ${Boost_LIBRARIES} ${LIBXML2_LIBRARIES})
target_link_libraries(search ${Boost_LIBRARIES} pthread)
