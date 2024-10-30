'''
unlike previous attempts to validate metadata, this one is a pairwise comparison
of every column against every other column. The idea is that most columns should
either have a single value when compared against another column (e.g. a given
latitude should always pair with a single longitude) or many values (e.g. a
given region will have many samples). When there are only a few values (e.g.
one health facility abbreviation that has a couple different health facility
names) this is an indicator of a possible issue.

The thinking behind this turned out to be flawed - there are a very large number
of first column values the have a very small number of second column values, and
even if we subset to the first_column:second_column pairings that are
comparatively rare (relative to other first_column:second column pairings) we
still have a very large number of events. It is slightly useful for finding
typos. Leaving it here in case it turns out to be useful one day.
'''

metadata_file='/nfs/jbailey5/baileyweb/asimkin/msmt_projects/msmt_longitudinal_DR/v1_2024-01-31/combined_for_hackathon/merged_metadata.csv'
meta_dict={}
for line_number, line in enumerate(open(metadata_file)):
	line=line.strip().split(',')
	if line_number==0:
		hr_dict={}
		for column_number, column in enumerate(line):
			hr_dict[column_number]=column
	else:
		for first_number, first_column in enumerate(line):
			for second_number, second_column in enumerate(line):
				if second_number!=first_number:
					first_pair=(first_column, first_number)
					second_pair=(second_column, second_number)
					meta_dict.setdefault(first_pair, {})
					meta_dict[first_pair].setdefault(second_number, {})
					meta_dict[first_pair][second_number].setdefault(second_column, 0)
					meta_dict[first_pair][second_number][second_column]+=1

output_file=open('rare_events.tsv', 'w')
output_file.write('first_header\tfirst_value\tsecond_header\tsecond_value\tcount\tfrac\n')

for first_pair in meta_dict:
	pairings=len(meta_dict[first_pair])
	for second_number in meta_dict[first_pair]:
		pairings=len(meta_dict[first_pair][second_number])
		if pairings>1 and pairings<10:
			total=0
			for second_column in meta_dict[first_pair][second_number]:
				count=meta_dict[first_pair][second_number][second_column]
				total+=count
			for second_column in meta_dict[first_pair][second_number]:
				count=meta_dict[first_pair][second_number][second_column]
				frac=count/total
				#print('frac is', frac)
				if frac<0.2 and count<30:
					first_header=hr_dict[first_pair[1]]
					first_name=first_pair[0]
					second_header=hr_dict[second_number]
					print('when column', first_header, 'takes the value', first_name, 'and column', second_header, 'takes the value', second_column, 'this happens', count, 'times, which is', frac)
					output_file.write(f'{first_header}\t{first_name}\t{second_header}\t{second_column}\t{count}\t{frac}\n')
			
