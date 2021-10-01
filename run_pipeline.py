import subprocess
import os
import multiprocessing
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


def run(chunk):
	argv = get_args()

	for i, row in chunk.iterrows():
		file = "{input_dir}/{ticket_id}/{library_id}/{library_id}_inputs.yaml".format(
				library_id=row['library_id'], ticket_id=row['ticket_id'], input_dir=argv.input_dir)

		# this line requests 6 GB by default which isn't enough
		# --nativespec \' -n {{ncpus}} -R "rusage[mem={{mem}}]span[ptile={{ncpus}}]select[type==CentOS7]"\' \

		# this line allows me to specify memory & walltime but it reaches memory limit at 16 GB
		# --nativespec \' -n {ncpus} -W {walltime} -R "rusage[mem={mem}]span[ptile={ncpus}]select[type==CentOS7]"\' \
		if os.path.exists(file):
			subprocess.call('single_cell hmmcopy --input_yaml {input_dir}/{ticket_id}/{library_id}/{library_id}_inputs.yaml \
				--library_id {library_id} --maxjobs 4 --nocleanup --sentinel_only  \
				--submit lsf \
				--config_file config.yaml \
				--nativespec \' -n {ncpus} -W {walltime} -R "rusage[mem={mem}]span[ptile={ncpus}]select[type==CentOS7]"\' \
				--tmpdir {temp_dir}/{ticket_id}/{library_id} \
				--pipelinedir {pipeline_dir}/{ticket_id}/{library_id} --submit lsf --out_dir {output_dir}/{ticket_id}/{library_id}'.format(
					library_id=row['library_id'], ticket_id=row['ticket_id'],
					input_dir=argv.input_dir, output_dir=argv.output_dir,
					temp_dir=argv.temp_dir, pipeline_dir=argv.pipeline_dir, 
					ncpus="1", walltime="6:00", mem="64"
					),
				shell=True)


def main():
	argv = get_args()
	whole_df = pd.read_csv(argv.samples, sep='\t', index_col=False, dtype=str)

	# nohit_libraries are the contaminated libraries of interest that might give insight to S-phase progress
	# nohit_libraries = ['A96180B', 'A96184A', 'A96149B', 'A96228B', 'A96240A']

	# remove rows from df if the input file is missing or output file is already present
	bad_rows = []
	for i, row in whole_df.iterrows():
		input_file = "{input_dir}/{ticket_id}/{library_id}/{library_id}_inputs.yaml".format(
				library_id=row['library_id'], ticket_id=row['ticket_id'], input_dir=argv.input_dir)
		output_file = "{output_dir}/{ticket_id}/{library_id}/{library_id}_reads.csv.gz".format(
				library_id=row['library_id'], ticket_id=row['ticket_id'], output_dir=argv.output_dir)
		if not os.path.exists(input_file) or os.path.exists(output_file):
			bad_rows.append(i)

	df = whole_df.drop(bad_rows)
	df.reset_index(inplace=True)
	df.drop(columns=['index'], inplace=True)

	# create as many processes as there are CPUs on your machine
	num_processes = min(multiprocessing.cpu_count(), df.shape[0])

	# calculate the chunk size as an integer
	chunk_size = int(df.shape[0]/num_processes)

	# separate df into chunks
	chunks = [df.iloc[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)]

	# create our pool with `num_processes` processes
	pool = multiprocessing.Pool(processes=num_processes)

	# apply our function to each chunk in the list
	result = pool.map(run, chunks)


if __name__ == '__main__':
	main()
