#!/bin/bash

cd ../../IR_SQL_Converter/build_duckdb_010/ && make clean && make -j32
cd ../../duckdb_010/ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make
cd ../PostgreSQL-12.3/build/ && make clean && make -j32 && sudo make install
cd ../../AQP_middleware

if [ -d "build" ]; then
  cd build/ && make -j32
else
  mkdir build/ && cd build/ && cmake .. && make -j32
fi