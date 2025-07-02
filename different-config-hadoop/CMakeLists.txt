cmake_minimum_required(VERSION 3.10)
project(Benchmarking)

# Set the C standard and optimization level
set(CMAKE_C_STANDARD 11)

# Find OpenSSL, ensure it provides hints if CMake has difficulty finding it
set(OPENSSL_ROOT_DIR /usr/local/opt/openssl@3)
set(OPENSSL_LIBRARIES ${OPENSSL_ROOT_DIR}/lib)
find_package(OpenSSL REQUIRED)

# Include directories
include_directories(${PROJECT_SOURCE_DIR}/c ${OPENSSL_INCLUDE_DIR})

# Source files
set(SOURCE_FILES
    hashgen.c
    c/blake3.c
    c/blake3_dispatch.c
    c/blake3_portable.c
)

# Conditionally add files and flags based on architecture
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w -O3 -mavx2 -pthread")
    
    # Assembly files
    set(ASSEMBLY_FILES
        c/blake3_sse2_x86-64_unix.S
        c/blake3_sse41_x86-64_unix.S
        c/blake3_avx2_x86-64_unix.S
        c/blake3_avx512_x86-64_unix.S
    )

elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w -O3 -pthread")  # Adjust flags suitable for ARM

    # ARM-specific assembly or sources would be added here
    set(ASSEMBLY_FILES
        c/blake3_sse2_x86-64_unix.S
        c/blake3_sse41_x86-64_unix.S
        c/blake3_avx2_x86-64_unix.S
        c/blake3_avx512_x86-64_unix.S
    )
endif()

# Set the language for assembly files
set_source_files_properties(${ASSEMBLY_FILES} PROPERTIES LANGUAGE C)

# Executable
add_executable(hashgen ${SOURCE_FILES} ${ASSEMBLY_FILES})

# Set the output directory for the executable to be one level up
set_target_properties(hashgen PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link with the crypto library
target_link_libraries(hashgen OpenSSL::Crypto)
