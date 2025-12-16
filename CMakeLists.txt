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
    else()
        message(WARNING "DUCKDB_LIB not found")
    endif()
else()
    message(WARNING "DuckDB source not found")
endif()

# PostgreSQL
set(POSTGRES_ROOT "/home/pei/Project/project_bins" CACHE PATH "PostgreSQL lib/inc path")
set(HAVE_POSTGRES FALSE)
if(EXISTS ${POSTGRES_ROOT})
    message(STATUS "Found PostgreSQL lib/inc at: ${POSTGRES_ROOT}")

    include_directories(
            ${POSTGRES_ROOT}/include)

    # Find libpq library (check custom build and system locations)
    find_library(LIBPQ_LIB
            NAMES pq libpq
            PATHS ${POSTGRES_ROOT}/lib
            NO_DEFAULT_PATH
    )

    if(LIBPQ_LIB)
        set(HAVE_POSTGRES TRUE)
        add_library(postgres_adapter STATIC src/adapters/postgres_adapter.cpp)
        target_include_directories(postgres_adapter PUBLIC
                ${POSTGRES_ROOT}/include
        )

        target_link_libraries(postgres_adapter PUBLIC ${LIBPQ_LIB})

        add_executable(test_postgres examples/test_postgres.cpp)
        target_link_libraries(test_postgres PRIVATE postgres_adapter)

        message(STATUS "PostgreSQL adapter: ENABLED")
    else()
        message(WARNING "LIBPQ_LIB not found")
    endif ()
else()
    message(WARNING "PostgreSQL source not found")
endif()

# libpg_query
set(LIBPG_QUERY_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../libpg_query" CACHE PATH "libpg_query source path")
if(EXISTS ${LIBPG_QUERY_ROOT})
    message(STATUS "Found libpg_query at: ${LIBPG_QUERY_ROOT}")

    include_directories(${LIBPG_QUERY_ROOT})

    find_library(LIBPG_QUERY_LIB
            NAMES pg_query libpg_query
            PATHS ${LIBPG_QUERY_ROOT}
            NO_DEFAULT_PATH
    )

    if(LIBPG_QUERY_LIB AND HAVE_POSTGRES)
        target_link_libraries(postgres_adapter PUBLIC ${LIBPG_QUERY_LIB})
        message(STATUS "libpg_query: ENABLED")
    else()
        message(WARNING "LIBPG_QUERY_LIB not found or not have PostgreSQL")
    endif()
else()
    message(WARNING "libpg_query not found at ${LIBPG_QUERY_ROOT}")
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