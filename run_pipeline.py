import subprocess
import sys
import pandas as pd
from argparse import ArgumentParser


def get_args():
	p = ArgumentParser()

	p.add_argument('-s', '--samples', type=str, help='tsv file of dataset information (sample_id, library_id, ticket_id, etc)')
	p.add_argument('-i', '--input_dir', type=str, nargs='?', default='input', help='folder for storing yaml files used as pipeline input')
	p.add_argument('-o', '--output_dir', type=str, nargs='?', default='output', help='folder for storing pipeline output')
	p.add_argument('-t', '--temp_dir', type=str, nargs='?', default='temp', help='folder for storing pipeline temp')
	p.add_argument('-p', '--pipeline_dir', type=str, nargs='?', default='pipeline', help='folder for storing pipeline')

	return p.parse_args()



def main():
	argv = get_args()
	df = pd.read_csv(argv.samples, sep='\t', index_col=False, dtype=str)
	print(sys.version)
	subprocess.call('sh final_run.sh', shell=True)

	# for i, row in df.iterrows():
	# 	subprocess.check_call('single_cell hmmcopy --input_yaml {input_dir}/{library_id}_inputs.yaml \
	# 		--library_id {library_id} --maxjobs 4 --nocleanup --sentinel_only  \
	# 		--submit lsf --loglevel DEBUG \
	# 		--config_file config.yaml \
	# 		--tmpdir {temp_dir}/{library_id} \
	# 		--pipelinedir {pipeline_dir}/{library_id} --submit lsf --out_dir {output_dir}/{library_id}'.format(
	# 			library_id=row['library_id'], input_dir=argv.input_dir, output_dir=argv.output_dir,
	# 			temp_dir=argv.temp_dir, pipeline_dir=argv.pipeline_dir),
	# 		shell=True)