import os
import sys
import logging
import yaml
import scgenome.db.search
import dbclients.tantalus
import pandas as pd
from argparse import ArgumentParser
from tantalus_credentials import load_os_environ


LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)


def get_args():
	p = ArgumentParser()

	p.add_argument('samples', type=str, help='tsv file of dataset information (sample_id, library_id, ticket_id, etc)')
	p.add_argument('dir', type=str, nargs='?', default='input', help='output folder for storing yaml files')

	return p.parse_args()


def get_BAM_paths(sample_id, library_id, jira_ticket):
	tantalus_api = dbclients.tantalus.TantalusApi()
	bam_list = []

	# get all datasets that match the library ID
	bam_datasets = list(tantalus_api.list('sequencedataset', library__library_id=library_id, dataset_type='BAM'))


	results_datasets = list(tantalus_api.list('resultsdataset',
					libraries__library_id=library_id,
					samples__sample_id=sample_id,
					results_type='alignment'
					))
	analysis_id = ""
	for x in results_datasets:
		current_jira_ticket = x["name"].split("_")[0]
		if current_jira_ticket == jira_ticket:
			analysis_id = x["analysis"]
			break


	for item in bam_datasets:
		current_analysis_id = item["analysis"]
		if item["sample"]["sample_id"] == sample_id and item["analysis"] == analysis_id:
			current_id = item["id"]  # store tantalus dataset id
			current_bams = list(tantalus_api.list('file_resource_detail',
							sequencedataset__id=current_id,
							filename__endswith='.bam'))
			file_locations = set()
			for file in current_bams:  # for each cell in the dataset
				for location in file["fileinstance_set"]:  # for every location where that cell's BAM exists
					file_locations.add(location["storage"]["name"])
					if location["storage"]["name"] == "juno":
						bam_list.append(location["filepath"])  # append path located on juno
			print("file_locations:", file_locations)

	return bam_list


def main():
	argv = get_args()
	df = pd.read_csv(argv.samples, sep='\t', index_col=False)

	for i, row in df.iterrows():
		sub_dir = '/' + str(row['ticket_id']) + '/' + str(row['library_id']) + '/'
		full_prefix = str(argv.dir) + sub_dir
		file_name = full_prefix + str(row['library_id']) + '_inputs.yaml'

		# only look for BAM files if the input file doesn't exist yet
		if not os.path.exists(file_name):
			print(row['sample_id'], row['library_id'], row['ticket_id'])
			bam_paths = get_BAM_paths(row['sample_id'], row['library_id'], row['ticket_id'])
			print(len(bam_paths), "number of bam files found")

			inputs_yaml = {}
			for path in bam_paths:
				cell_id = os.path.basename(path).split('.')[0]
				r = cell_id.split('-')[2].replace('R', '')
				c = cell_id.split('-')[3].replace('C', '')
				temp_dict = {
					'bam': path,
					'column': int(c),
					'condition': 'B',
					'img_col': int(c),
					'index_i5': 'i5-' + str(r),
					'index_i7': 'i7-' + str(c),
					'pick_met': 'C1',
					'primer_i5': 'AAAAAA',
					'primer_i7': 'TTTTTT',
					'row': int(r),
					'sample_id': str(row['sample_id']),
					'library_id': str(row['library_id'])
					}
				inputs_yaml[cell_id] = temp_dict

			# only write non-empty dicts to input yaml file
			if inputs_yaml:
				if not os.path.exists(full_prefix):
					os.makedirs(full_prefix)

				with open(file_name, 'w') as f:
					yaml.dump(inputs_yaml, f, default_flow_style=False, sort_keys=False)


if __name__ == "__main__":
	load_os_environ()
	main()
