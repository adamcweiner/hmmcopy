#!/bin/bash

single_cell hmmcopy --input_yaml input/A73044B_inputs.yaml \
  --library_id A73044B --maxjobs 4 --nocleanup --sentinel_only  \
  --submit lsf --loglevel DEBUG \
  --config_file config.yaml \
  --tmpdir temp/A73044B \
  --pipelinedir pipeline/A73044B --submit lsf --out_dir output/A73044B
