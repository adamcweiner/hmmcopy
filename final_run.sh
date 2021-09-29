#!/bin/bash

single_cell hmmcopy --input_yaml inputs.yaml \
  --library_id A90554B --maxjobs 4 --nocleanup --sentinel_only  \
  --submit lsf --loglevel DEBUG \
  --config_file config.yaml \
  --tmpdir temp/A90554B_test \
  --pipelinedir pipeline/A90554B_test --submit lsf --out_dir output/A90554B_test
