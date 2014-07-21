"""
PROBLEM 5

Consider a set of key-value pairs where each key is a sequence id and
each value is a string of nucleotides, e.g., GCTTCCGAAATGCTCGAA

Write a MapReduce query to remove the last 10 characters from each
string of nucleotides, then remove any duplicates generated.

The output from the reduce function should be the unique trimmed
nucleotide strings.

"""

import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    seq_id = record[0]
    nucleotide = record[1]
    
    ##trim the nucleotide
    trimmed = nucleotide[:len(nucleotide)-10]
    
    ##Emit the trimmed nucleotide so reducer will have unique values
    mr.emit_intermediate(trimmed, seq_id)
    
def reducer(key, list_of_values):
    ##emits the nucleotide
    mr.emit(key)
    
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)