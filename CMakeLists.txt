cmake_minimum_required(VERSION 3.10)
project(AQP_middleware)

set(CMAKE_CXX_STANDARD 14)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# DuckDB
set(DUCKDB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../duckdb" CACHE PATH "DuckDB source path")
set(HAVE_DUCKDB FALSE)
if(EXISTS ${DUCKDB_ROOT})
    message(STATUS "Found DuckDB source at: ${DUCKDB_ROOT}")

    include_directories(${DUCKDB_ROOT}/src/include)

    find_library(DUCKDB_LIB
            NAMES duckdb libduckdb
            PATHS ${DUCKDB_ROOT}/build/release/src ${DUCKDB_ROOT}/build/debug/src
            NO_DEFAULT_PATH
    )

    if(DUCKDB_LIB)
        set(HAVE_DUCKDB TRUE)
        add_library(duckdb_adapter STATIC src/adapters/duckdb_adapter.cpp)
        target_include_directories(duckdb_adapter PUBLIC
                ${CMAKE_CURRENT_SOURCE_DIR}/include
                ${DUCKDB_ROOT}/src/include
        )

        target_link_libraries(duckdb_adapter PUBLIC ${DUCKDB_LIB})

        add_executable(test_duckdb examples/test_duckdb.cpp)
        target_link_libraries(test_duckdb PRIVATE duckdb_adapter)

        message(STATUS "DuckDB adapter: ENABLED")
    endif()
else()
    message(WARNING "DuckDB source not found at ${IR_CONVERTER_ROOT}")
endif()

# PostgreSQL
set(POSTGRES_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../PostgreSQL-12.3" CACHE PATH "PostgreSQL source path")
set(HAVE_POSTGRES FALSE)
if(EXISTS ${POSTGRES_ROOT})
    message(STATUS "Found PostgreSQL source at: ${POSTGRES_ROOT}")

    include_directories(
            ${POSTGRES_ROOT}/src/include
            ${POSTGRES_ROOT}/src/interfaces/libpq)

    # Find libpq library (check custom build and system locations)
    find_library(LIBPQ_LIB
            NAMES pq libpq
            PATHS ${POSTGRES_ROOT}/src/interfaces/libpq ${POSTGRES_ROOT}/lib
            NO_DEFAULT_PATH
    )

    if(LIBPQ_LIB)
        set(HAVE_POSTGRES TRUE)
        add_library(postgres_adapter STATIC src/adapters/postgres_adapter.cpp)
        target_include_directories(postgres_adapter PUBLIC
                ${CMAKE_CURRENT_SOURCE_DIR}/include
                ${POSTGRES_ROOT}/src/include
                ${POSTGRES_ROOT}/src/interfaces/libpq
        )

        target_link_libraries(postgres_adapter PUBLIC ${LIBPQ_LIB})

        add_executable(test_postgres examples/test_postgres.cpp)
        target_link_libraries(test_postgres PRIVATE postgres_adapter)

        message(STATUS "PostgreSQL adapter: ENABLED")
    endif ()
else()
    message(WARNING "PostgreSQL source not found at ${IR_CONVERTER_ROOT}")
endif()

# IR_SQL_Converter
set(IR_CONVERTER_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../IR_SQL_Converter" CACHE PATH "IR_SQL_Converter source path")
if(EXISTS ${IR_CONVERTER_ROOT})
    message(STATUS "Found IR_SQL_Converter at: ${IR_CONVERTER_ROOT}")

    # Include IR_SQL_Converter headers
    include_directories(
            ${IR_CONVERTER_ROOT}/inc
            ${IR_CONVERTER_ROOT}/third_party/libpg_query/include
    )

    # Find IR_SQL_Converter library
    find_library(IR_CONVERTER_LIB
            NAMES IR_SQL_Converter libIR_SQL_Converter IR_SQL_Converter_static
            PATHS
            ${IR_CONVERTER_ROOT}/lib
            ${IR_CONVERTER_ROOT}/build/lib
            NO_DEFAULT_PATH
    )

    if(HAVE_DUCKDB)
        target_link_libraries(test_duckdb PUBLIC ${IR_CONVERTER_LIB})
    endif()
    if(HAVE_POSTGRES)
        target_link_libraries(test_postgres PUBLIC ${IR_CONVERTER_LIB})
    endif()
else()
    message(WARNING "IR_SQL_Converter source not found at ${IR_CONVERTER_ROOT}")
endif()