This data set was provided to us by Prof. David Franke and relates to visitors at a website looking to purchase automobile. 

Reduce side join implementation 

1. sessions data has data related to each user session.
2. impressions data has vin impression related data.
3. this code reads in both the file formats and performs reduce side join 
4. to run code via command line : cat <"filepath">/data* | <"filepath">session\_map.py | sort -k1,1 | <"filepath">session_reduce.py


