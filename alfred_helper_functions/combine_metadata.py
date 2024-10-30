meta_21='manuscript_metadata_attempted_samples_2021.csv'
meta_22='MSMT_2022_METADATA.csv'
meta_23='KAGERA_MSMT_2023.csv'

path_dict={'2021': meta_21, '2022': meta_22, '2023': meta_23}
output_file=open('merged_metadata.csv', 'w')

def compare_headers(path_dict):
	'''
	used primarily to tweak the input metadata headers until congruent columns
	have identical names
	'''
	header_dict={}
	for year in path_dict:
		path=path_dict[year]
		header=open(path).readline().strip().split(',')
		header_set=set(header)
		if len(header_set)!=len(header):
			print('duplicates exist in', year)
			print('comparison is', header_set, header, year)
		header_dict[year]=header_set
	one_only=header_dict['2021']-(header_dict['2022']|header_dict['2023'])
	two_only=header_dict['2022']-(header_dict['2021']|header_dict['2023'])
	three_only=header_dict['2023']-(header_dict['2021']|header_dict['2022'])
	one_share=header_dict['2021']&(header_dict['2022']|header_dict['2023'])
	two_share=header_dict['2022']&(header_dict['2021']|header_dict['2023'])
	three_share=header_dict['2023']&(header_dict['2021']|header_dict['2022'])
	all_union=header_dict['2021']|header_dict['2022']|header_dict['2023']
	print('one only', one_only)
	print('two only', two_only)
	print('three only', three_only)
	print('one share', one_share)
	print('two share', two_share)
	print('three share', three_share)
	return list(all_union)

def merge_sheets(path_dict, all_union, output_file):
	'''
	iterates through the input metadata files and for each line iterates through
	the union of all header values, printing any values of the current line that
	are in the current header, and printing a blank placeholder otherwise.
	'''
	output_file.write(','.join(all_union)+'\n')
	for year in path_dict:
		path=path_dict[year]
		for line_number, line in enumerate(open(path)):
			line=line.strip().split(',')
			if line_number==0:
				h_dict={}
				for column_number, column in enumerate(line):
					h_dict[column]=column_number
			else:
				output_line=[]
				for entry in all_union:
					if entry in h_dict:
						output_line.append(line[h_dict[entry]])
					else:
						output_line.append('')
				output_file.write(','.join(output_line)+'\n')

all_union=compare_headers(path_dict)
merge_sheets(path_dict, all_union, output_file)