cmake_minimum_required(VERSION 4.0.0)
project(NovaIO VERSION 1.0.0 LANGUAGES CXX)


set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "NovaIO requires LLVM/Clang 22.1.3+")
endif()

add_compile_options(-march=x86-64-v3 -O3 -fno-omit-frame-pointer -stdlib=libc++)
add_link_options("-fuse-ld=mold" "-Wl,--zero-to-bss" "-stdlib=libc++")


add_library(novaio_core INTERFACE)
target_include_directories(novaio_core INTERFACE 
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    /usr/local/include
    /usr/local/include/liburing
    /usr/local/include/liburing/io_uring
    /usr/local/include/mimalloc-3.3
)


target_link_libraries(novaio_core INTERFACE
    pthread
    /usr/local/lib/liburing.so
    /usr/local/lib/libmimalloc.so
)