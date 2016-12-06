This code parses the two text files to index all emails associated with a message type as key and email ids as values in a list.

1. to run code via command line : cat <"filepath">/data* | <"filepath">session\_map.py | sort -k1,1 | <"filepath">session_reduce.py
