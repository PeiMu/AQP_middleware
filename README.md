## Clone Project
```bash
git clone --recurse-submodules git@github.com:PeiMu/AQP_middleware.git
```

## How to compile
```bash
mkdir -p build_debug && cd build_debug/
cmake -DCMAKE_BUILD_TYPE=Debug .. # requires CMake 4.0 or higher
make -j32

mkdir -p build_release && cd build_release/
cmake -DCMAKE_BUILD_TYPE=Release .. # requires CMake 4.0 or higher
make -j32
```

## Configuration

It can select different engines and split strategies.

E.g., PostgreSQL
```bash
./build_release/aqp_middleware
--engine=postgresql \
--db="host=localhost port=5432 dbname=imdb user=pei" \
--schema=/home/pei/Project/benchmarks/imdb_job-postgres/schema.sql \
--fkeys=/home/pei/Project/benchmarks/imdb_job-postgres/fkeys.sql \
--split=relationshipcenter \
--check-correctness \
--debug \
/home/pei/Project/benchmarks/imdb_job-postgres/queries/1a.sql
```

Or DuckDB
```bash
./build_release/aqp_middleware
--engine=duckdb \
--db="/home/pei/Project/duckdb_132/measure/imdb.db" \
--schema=/home/pei/Project/benchmarks/imdb_job-postgres/schema.sql \
--fkeys=/home/pei/Project/benchmarks/imdb_job-postgres/fkeys.sql \
--split=top_down \
--check-correctness \
--debug \
/home/pei/Project/benchmarks/imdb_job-postgres/queries/1a.sql
```

Or Umbra
```bash
# start the docker
docker run \
--name umbra_middleware \
--network=host \
-v umbra-db:/var/db \
-v /tmp:/tmp \
--ulimit nofile=1048576:1048576 \
--ulimit memlock=8388608:8388608 \
umbradb/umbra:latest \
umbra-server --address 0.0.0.0 /var/db/imdb.db

# run the aqp_middleware
../build/aqp_middleware \
--engine=umbra \
--db="host=localhost port=5432 user=postgres password=postgres" \
--schema=/home/pei/Project/benchmarks/imdb_job-postgres/schema.sql \
--fkeys=/home/pei/Project/benchmarks/imdb_job-postgres/fkeys.sql \
--split=relationshipcenter \
--check-correctness \
--debug \
/home/pei/Project/benchmarks/imdb_job-postgres/queries/1a.sql
```

Or MariaDB
```bash
../build/aqp_middleware \
--engine=mariadb \
--db="host=localhost dbname=imdb user=imdb" \
--schema=/home/pei/Project/benchmarks/imdb_job-postgres/schema.sql \
--fkeys=/home/pei/Project/benchmarks/imdb_job-postgres/fkeys.sql \
--split=relationshipcenter \
--check-correctness \
--debug \
/home/pei/Project/benchmarks/imdb_job-postgres/queries/1a.sql
```

It can also run the whole benchmark.

E.g.,
```bash
--engine=postgresql
--db="host=localhost port=5432 dbname=imdb user=pei"
--schema=/home/pei/Project/benchmarks/imdb_job-postgres/schema.sql
--split=relationshipcenter
--check-correctness
--benchmark
--debug
/home/pei/Project/benchmarks/imdb_job-postgres/queries
```

## Measurement Scripts
Go to directory `measure/`, there is script to run with either engine/split_strategy

```bash
bash ./run_job.sh duckdb
```

Or measure the performance.

```bash
bash ./measure_job.sh duckdb
```


We also provide scripts for running the native Umbra and MariaDB.

```bash
bash ./run_umbra.sh
bash ./run_mariadb.sh
```

Or measure their performance.

```bash
bash ./measure_umbra.sh
bash ./measure_mariadb.sh
```