cmake_minimum_required(VERSION 3.5)
# No idea what the true minimum version is. I use 3.27.4.
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
project(extract)

# This project uses the experimental header <osmium/io/pbf_input_randomaccess.hpp>
# See also https://github.com/osmcode/libosmium/pull/367
set(OSMIUM_INCLUDE_DIR ../../libosmium/include)
find_package(Osmium REQUIRED COMPONENTS pbf)
include_directories(SYSTEM ${OSMIUM_INCLUDE_DIRS})

set(PROG extract)
add_executable(${PROG} ${PROG}.cpp ${SOURCES})
add_definitions(${OSMIUM_WARNING_OPTIONS})
target_compile_options(${PROG} PRIVATE -O2 -g2)
target_link_libraries(${PROG} ${Boost_LIBRARIES} ${OSMIUM_LIBRARIES})
set_pthread_on_target(${PROG})
