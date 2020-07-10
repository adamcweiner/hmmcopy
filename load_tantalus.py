import os
import sys
import logging
import scgenome.db.search
import dbclients.tantalus
from tantalus_credentials import load_os_environ


LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)


def get_BAM_paths(sample_name, jira_ticket):
	tantalus_api = dbclients.tantalus.TantalusApi()
	[sample_id, library_id] = sample_name.split("-")
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


def get_annotation_metrics_path(sample_name, jira_ticket):
	""" Use this function to find annotation metrics CSV file path on juno """
	tantalus_api = dbclients.tantalus.TantalusApi()
	[sample_id, library_id] = sample_name.split("-")
	file_path = None

	# find all annotation resultsdatasets that correspond to this sample+library
	results_datasets = list(tantalus_api.list('resultsdataset',
						libraries__library_id=library_id,
						samples__sample_id=sample_id,
						results_type='annotation'))
	# for each resultsdataset
	for x in results_datasets:
		current_jira_ticket = x["name"].split("_")[0]
		# check to see if jira_ticket matches
		if current_jira_ticket == jira_ticket:
			# query file resources with the id of this resultsdataset and the specific annotation metrics end of filename
			current_results = list(tantalus_api.list('file_resource_detail',
								   resultsdataset__id=x["id"],
								   filename__endswith='_metrics.csv.gz'))
			for file in current_results:  # for each match in current_results query
				for location in file["fileinstance_set"]:  # for every storage location where that file exists
					if location["storage"]["name"] == "juno":
						file_path = location["filepath"]  # store path located on juno

	return file_path

def find_jira_tickets(sample_name):
	""" Use this function to find jira-tickets that are common to all 3 result_types. """
	tantalus_api = dbclients.tantalus.TantalusApi()
	[sample_id, library_id] = sample_name.split("-")

	results_types = ["alignment", "annotation", "hmmcopy"]

	common_jira_tickets = []  # list of jira tickets common to all results types

	for i, typee in enumerate(results_types):
		results_datasets = list(tantalus_api.list('resultsdataset',
							libraries__library_id=library_id,
							samples__sample_id=sample_id,
							results_type=typee))
		print("length of results_datasets:", len(results_datasets))
		current_jira_tickets = []
		for x in results_datasets:
			current_jira_tickets.append(x["name"].split("_")[0])
		print('\n', typee, ': \n', current_jira_tickets, '\n')
		if i == 0:
			common_jira_tickets = current_jira_tickets
		else:
			common_jira_tickets = list(set(common_jira_tickets) & set(current_jira_tickets))  # find intersection of current and common jira_tickets

	return common_jira_tickets


def find_normal_BAMs(sample_name):
	tantalus_api = dbclients.tantalus.TantalusApi()
	[sample_id, library_id] = sample_name.split("-")

	# get all datasets that match the library ID
	bam_datasets = list(tantalus_api.list('sequencedataset', library__library_id=library_id, dataset_type='BAM'))
	bam_list = []

	for item in bam_datasets:
		current_analysis_id = item["analysis"]
		if item["sample"]["sample_id"] == sample_id:
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


# do some basic testing to make sure stuff is working
if __name__ == "__main__":
	load_os_environ()

	samp_lib_ids = [
					# "SA1255RA-A98177B",
					# "SA1256IO-A108867A",
					# "SA1049N-A78166",
					# "SA1202N-09443_I",
					# "SA1257IO-A108761B",
					# "SA1257IO-A108762B",
					# "SA1257LA-A108762A",
					"SA1049AX1XB01417-A95652B",
					"SA1049AX1XB01417-A96113A",
					"SA1049AX1XB01417-A96205B"
					]
	for i in range(len(samp_lib_ids)):
		print("current sample:", samp_lib_ids[i])
		commons = find_jira_tickets(samp_lib_ids[i])
		print("commons:", commons)
		if len(commons) > 0:
			bams = get_BAM_paths(samp_lib_ids[i], commons[0])
			print(len(bams), "number of bam files found")
