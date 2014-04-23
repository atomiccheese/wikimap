cmake_minimum_required(VERSION 2.7)
project(wikimap)

# set(CMAKE_C_COMPILER "/usr/bin/clang")
# set(CMAKE_CXX_COMPILER "/usr/bin/clang")
# set(CMAKE_LINKER "/usr/bin/llvm-ld")
set(CMAKE_CXX_FLAGS "-ggdb -O0")

find_package(Boost REQUIRED COMPONENTS system thread regex chrono atomic)
find_package(LibXml2 REQUIRED)

include_directories(${LIBXML2_INCLUDE_DIR} ${BOOST_INCLUDE_DIR})
add_executable(preprocess src/preprocess.cpp)
add_executable(search src/search.cpp src/bag.cpp)
target_link_libraries(preprocess ${Boost_LIBRARIES})
target_link_libraries(search ${Boost_LIBRARIES})
