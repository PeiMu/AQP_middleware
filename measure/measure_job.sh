#!/bin/bash

mkdir -p job_result/
rm -rf compile.log

Project_path=/home/pei/Project/project_bins
pg_start() {
  pg_ctl start -l $Project_path/logfile -D $Project_path/data
}
pg_stop() {
  pg_ctl stop -D $Project_path/data -m smart -s
}
rm_pg_log() {
  rm $Project_path/logfile
}


bash ./compile.sh >> compile.log 2>&1

pg_start

# run ANALYZE
psql -U pei -d imdb -c "ANALYZE;"

cd ../measure && bash ./hyperfine_job.sh $1

pg_stop

mv compile.log job_result/.
