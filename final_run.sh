#!/bin/bash

single_cell hmmcopy --input_yaml inputs.yaml \
  --library_id A97318A --maxjobs 4 --nocleanup --sentinel_only  \
  --submit lsf --loglevel DEBUG \
  --config_file config.yaml \
   --tmpdir temp \
  --pipelinedir pipeline --submit lsf --out_dir output
