l=[10,20,30,40,50,60,70,80,90,100,110,120]
x=0
y=12
for i in range(x,y,4):
    x=i
    print (l[x:x+4])

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

import pprint
pprint.pprint(list(chunks(l, 2)))