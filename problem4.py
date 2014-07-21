"""
PROBLEM 4

The relationship "friend" is often symmetric, meaning that if I am your 
friend, you are my friend. Implement a MapReduce algorithm to check whether 
this property holds. Generate a list of all non-symmetric friend relationships.

The output should be a full record of symmetric relation. For every pair
(person, friend), you will emit BOTH (person, friend) AND (friend, person).

**However, be aware that (friend, person) may already appear in the 
dataset...duplications should not appear.
"""

import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    person = record[0]
    friend = record[1]
    
    """emit two keys so that a duplicate will be created for a 
    symmetric friendship. Creates a filter for symmetric friends.
    """
    mr.emit_intermediate(person, [person, friend])
    mr.emit_intermediate(friend, [friend, person])
    
def reducer(key, list_of_values):
    pairs = [tuple(value) for value in list_of_values]
    all_set = set(pairs)
    
    ##Find duplicates in the list of friend pairs
    dups_set = set([pair for pair in pairs if pairs.count(pair) >1])
    
    ##to get the asymetric friend list, remove dupplicates by difference
    assymetric_friends = all_set ^ dups_set
    map(lambda pair: mr.emit(pair), assymetric_friends)
    
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)