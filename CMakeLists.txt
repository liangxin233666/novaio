cmake_minimum_required(VERSION 3.25)

project(NovaIO VERSION 1.0.0 LANGUAGES CXX)

option(NOVAIO_BUILD_EXAMPLES "Build NovaIO example binaries" ON)
option(NOVAIO_ENABLE_METRICS "Enable per-scheduler runtime metrics" ON)
option(NOVAIO_ENABLE_LIBCXX "Build with LLVM libc++" ON)
option(NOVAIO_ENABLE_X86_64_V3 "Enable x86-64-v3 code generation for benchmark builds" ON)
option(NOVAIO_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "NovaIO requires LLVM/Clang because it uses coroutine and libc++ oriented extensions")
endif()

if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 22.0)
    message(FATAL_ERROR "NovaIO requires Clang 22.0+")
endif()

find_package(Threads REQUIRED)

find_path(URING_INCLUDE_DIR NAMES liburing.h PATHS /usr/local/include /usr/include)
find_library(URING_LIBRARY NAMES uring PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu)
find_path(MIMALLOC_INCLUDE_DIR NAMES mimalloc.h PATHS /usr/local/include/mimalloc-3.3 /usr/local/include /usr/include)
find_library(MIMALLOC_LIBRARY NAMES mimalloc PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu)
find_library(NUMA_LIBRARY NAMES numa PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu)

foreach(required_var URING_INCLUDE_DIR URING_LIBRARY MIMALLOC_INCLUDE_DIR MIMALLOC_LIBRARY NUMA_LIBRARY)
    if(NOT ${required_var})
        message(FATAL_ERROR "Required dependency not found: ${required_var}")
    endif()
endforeach()

add_library(novaio_core STATIC
    src/comutex.cpp
    src/io_engine.cpp
    src/runtime.cpp
    src/scheduler.cpp
    src/time_wheel.cpp
)

target_include_directories(novaio_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${URING_INCLUDE_DIR}
        ${MIMALLOC_INCLUDE_DIR}
)

target_compile_definitions(novaio_core
    PUBLIC
        NOVAIO_ENABLE_METRICS=$<IF:$<BOOL:${NOVAIO_ENABLE_METRICS}>,1,0>
)

target_compile_options(novaio_core
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -fno-omit-frame-pointer
)

if(NOVAIO_ENABLE_X86_64_V3)
    target_compile_options(novaio_core PRIVATE -march=x86-64-v3)
endif()

if(NOVAIO_ENABLE_LIBCXX)
    target_compile_options(novaio_core PUBLIC -stdlib=libc++)
    target_link_options(novaio_core PUBLIC -stdlib=libc++)
    find_path(LIBCXX_RUNTIME_DIR
        NAMES libc++.so.1
        PATHS
            /opt/llvm-22/lib/x86_64-unknown-linux-gnu
            /opt/llvm-22/lib
            /usr/local/lib
            /usr/lib
            /usr/lib/x86_64-linux-gnu
    )
    if(LIBCXX_RUNTIME_DIR)
        target_link_options(novaio_core PUBLIC "-Wl,-rpath,${LIBCXX_RUNTIME_DIR}")
    endif()
endif()

find_program(MOLD_LINKER mold)
if(MOLD_LINKER)
    include(CheckLinkerFlag)
    check_linker_flag(CXX "-fuse-ld=mold" NOVAIO_HAS_MOLD_LINKER_FLAG)
    if(NOVAIO_HAS_MOLD_LINKER_FLAG)
        target_link_options(novaio_core PUBLIC -fuse-ld=mold)
    endif()
endif()

if(NOVAIO_ENABLE_SANITIZERS)
    target_compile_options(novaio_core PUBLIC -fsanitize=address,undefined -fno-sanitize-recover=all)
    target_link_options(novaio_core PUBLIC -fsanitize=address,undefined)
endif()

target_link_libraries(novaio_core
    PUBLIC
        Threads::Threads
        ${URING_LIBRARY}
        ${MIMALLOC_LIBRARY}
        ${NUMA_LIBRARY}
)

function(novaio_add_binary target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE novaio_core)
endfunction()

include(CTest)

if(BUILD_TESTING)
    novaio_add_binary(test_framework examples/test_framework.cpp)
    novaio_add_binary(comprehensive_test examples/comprehensive_test.cpp)

    add_test(NAME novaio.test_framework COMMAND test_framework)
    add_test(NAME novaio.comprehensive COMMAND comprehensive_test)
    set_tests_properties(novaio.test_framework novaio.comprehensive PROPERTIES TIMEOUT 30)
endif()

if(NOVAIO_BUILD_EXAMPLES)
    novaio_add_binary(http_server examples/http_server.cpp)
    novaio_add_binary(http_server_multirecv examples/http_server_multirecv.cpp)
    novaio_add_binary(production_benchmark_test examples/production_benchmark_test.cpp)
endif()
