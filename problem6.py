"""
PROBLEM 6

Assume you have two matrices A and B in a sparse matrix format,
where each record is of the form: i, j, value. Design a MapReduce
algorithm to compute matrix multiplication: A %*% B.

The input to the map function will be a row of a matrix represented
as a list. Each list will be of the form [matrix, i, j, value] where
matrix is a string and i, j, and value are integers. The first item,
matrix is a string that identifies which matrix the record originates
from. This field is either "a or b".

The output from the reduce function will also be a row of the result
matrix represented as a tuple. Each tuple will be of the form (i, j, value)
where each element is an integer.

to run: python problem6.py matrix.json
"""

import MapReduce
import sys

mr = MapReduce.MapReduce()
a_row_max = 5 ##max rows in mat. A is 5
b_col_max = 5 ##max cols in mat. B is 5

def mapper(record):
    matrix = record[0]
    row = record[1]
    col = record[2]
    value = record[3]
    
    if matrix == 'a':
        ##for all A(i,j) emit key (j, k) for k=1 for num cols in B
        for k in range(0, b_col_max):
            mr. emit_intermediate((row, k), [matrix, col, value])
    else:
        ##for all B(j,k) emit key (j, i) for i=1 to # of rows in B
        for i in range(0, a_row_max):
            mr.emit_intermediate((i, col), [matrix, row, value])
            
def reducer(key, list_of_values):
    #sort the values
    a_values = filter(lambda cell: cell[0] == 'a', list_of_values)
    b_values = filter(lambda cell: cell[0] == 'b', list_of_values)
    
    ##generate sets and take the intersection of indices from 
    ##row vectors (A) and column vectors(B)
    a_set = set(map(lambda s: s[1], a_values))
    b_set = set(map(lambda s: s[1], b_values))
    a_b_set = a_set & b_set
    
    ##filter on a[j] == b[j]
    b_rows = filter(lambda row: row[1] in a_b_set, b_values)
    a_cols = filter(lambda row: row[1] in a_b_set, a_values)
    
    ##Multiply the matching pairs
    a_b_mult = map(lambda x: x[0][2] * x[1][2], zip(b_rows, a_cols))
    
    ##emit the sum of pairs for cell (j, k)
    mr.emit((key[0], key[1], sum(a_b_mult)))
    
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)