"""
PROBLEM 1

Create an Inverted index. Given a set of documents, an inverted index
is a dictionary where each word is associated with a list of the document 
identifiers in which that word appears.
"""

import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    key = record[0]
    value = record[1]
    terms = value.split()
    
    #emit the intermediary pair (term, docid)
    map(lambda term: mr.emit_intermediate(term, key), terms)
    
def reducer(key, list_of_values):
    ##Takes the list of docids (i.e. list)
    ##Remove the duplicates then emit the index for the term key
    mr.emit((key, list(set(list_of_values))))
    
    if __name__ == '__main__':
        inputdata = open(sys.argv[1])
        mr.execute(inputdata, mapper, reducer)