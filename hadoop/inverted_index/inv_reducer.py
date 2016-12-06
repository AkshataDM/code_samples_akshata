#!/usr/bin/python
import sys
import re
from collections import defaultdict
import operator

current_email = None
current_msg = None
email = None
message = None

dict_email = defaultdict(list)
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    message_list = None
    # parse the input we got from mapper.py
    email, message_id = line.split('\t', 1)
    # creating a list of message id if more than one message id is associated with the email
    if email in dict_email.keys():
       message_list = dict_email[email]
       message_list = message_list + "," + message_id
       dict_email[email] = message_list
    
    else: 
       dict_email[email] = message_id

# sorting the output by list of message id for each email
for a,b in dict_email.iteritems():
    message_id_split = re.split(",",b)
    sorted_message_id = sorted(message_id_split)
    print a," ".join(sorted_message_id)
    



   

