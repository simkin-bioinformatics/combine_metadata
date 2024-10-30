'''
checks that lower levels of heirarchical data that are the same as each other
share the same prefixes
'''
#metadata_path='preprint_metadata-all_Dativa_metadata.csv'
#metadata_path='NEW_MIP_2021_Revised_Metadata_Alfred_combined_preprint_metada.csv'
metadata_path='merged_metadata_edited.csv'

pairings={'Region': ['Region','Region_code'], 'District': ['District', 'District_code'], 'HF_lat': ['HFname', 'latitude'], 'HF_long': ['HFname', 'longitude'], 'coordinates': ['latitude', 'longitude'], 'HF_codes': ['HF_code', 'HFname']}
heirarchies=['Region', 'District', 'HFname', 'latitude', 'longitude']
pairing_file=open('pairing_file.csv', 'w')
heirarchy_file=open('heirarchy_file.csv', 'w')
repeating_file=open('repeat_file.csv', 'w')
h={}
equivalence_dict={}
pairing_file.write('sample,pairing_type,1st_value,obs_value,exp_value\n')
for line_number, line in enumerate(open(metadata_path)):
	line=line.strip().split(',')
	if line_number==0:
		for column_number, column in enumerate(line):
			h[column]=column_number
	else:
		sample=line[h['Sample_ID']]
		for pairing_type in pairings:
			pairing_columns=pairings[pairing_type]
			first, second=line[h[pairing_columns[0]]], line[h[pairing_columns[1]]]
			equivalence_dict.setdefault(pairing_type, {})
			if first not in equivalence_dict[pairing_type]:
				equivalence_dict[pairing_type][first]=second
			elif equivalence_dict[pairing_type][first]!=second:
				print('pairing type is', pairing_type, 'first is', first, 'second is', second, 'expected is', equivalence_dict[pairing_type][first], 'line is', line)
				pairing_file.write(','.join([sample, pairing_type, first, second, equivalence_dict[pairing_type][first]])+'\n')

print('here are all expected pairings')
for pairing_type in equivalence_dict:
	for first in equivalence_dict[pairing_type]:
		second=equivalence_dict[pairing_type][first]
		print(pairing_type, first, second)

suffix_dict={}
heirarchy_file.write('sample,current,prefix,exp_prefix\n')
for line_number, line in enumerate(open(metadata_path)):
	line=line.strip().split(',')
	if line_number==0:
		for column_number, column in enumerate(line):
			h[column]=column_number
	else:
		prefix=''
		sample=line[h['Sample_ID']]
		for layer in heirarchies:
			current_string=layer+': '+line[h[layer]]
			suffix_dict.setdefault(current_string, prefix)
			if prefix!=suffix_dict[current_string]:
				print('current is', current_string, 'prefix is', prefix, 'expected prefix is', suffix_dict[current_string])
				heirarchy_file.write(','.join([sample, current_string, prefix, suffix_dict[current_string]])+'\n')
			prefix+=current_string+'.  '

samples=set([])
for line_number, line in enumerate(open(metadata_path)):
	split_line=line.strip().split(',')
	if line_number==0:
		for column_number, column in enumerate(split_line):
			h[column]=column_number
	else:
		prefix=''
		sample=split_line[h['Sample_ID']]
		if sample in samples:
			print('repeated:', line)
			repeating_file.write(line)
		samples.add(sample)
