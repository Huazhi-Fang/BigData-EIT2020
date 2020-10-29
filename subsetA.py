# Complete the 'subsetA' function below
#
# The function is expected to return an INTEGER_ARRAY.
# The function accepts INTEGER_ARRAY arr as parameter
#
# Return the subsetA in increasing order where the sum of A's element is greater than the sum of B's element. 
# If more than one subset exits, return the one with maximumn sum.

# Solution 1:

def subsetA(arr):
   
    # sort input arr ascendingly
    arr = sorted(arr, reverse=True)
    
    # subsetting arr into A and B and return subsetA until sum(subsetA)>sum(subsetB)
    for i in range(1,len(arr)+1):
        if(sum(arr[:i])>sum(arr[i:])):
            subsetA = sorted(arr[:i])
            return subsetA
    
arr = [3,7,5,6,2]
print(subsetA(arr))



# Solution 2:

from itertools import combinations

def subsetA(arr):
    for i in range(1,len(arr)+1):
        # find all possible combinations of arr elements in size i
        listA=list(combinations(arr,i))
        
        # sort the items of the combination list by the sum of elements in the item
        listA=sorted(listA, key=lambda x: sum(x), reverse=True)
        
        # compare and return the subsetA where sum(subsetA)>sum(subsetB)
        for item in listA:
            if (sum(item)>(sum(arr)-sum(item))):
                return sorted(item)
                
arr = [3,7,5,6,2]
print(subsetA(arr))