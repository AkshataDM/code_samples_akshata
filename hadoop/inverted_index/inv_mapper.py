#!/usr/bin/python
import sys
import re
data = []
#--- get all lines from stdin ---
for line in sys.stdin:
    #--- remove leading and trailing whitespace---
    line = line.strip()

    #--- split the document into chunks [ each chunk is a message ] 
    chunks = line.split('\n')   
    dict_keys = {}
    # iterate through each chunk
    for lne in chunks:
        # split each chunk by tab
        words = re.split(r'(\t)',lne)   

        for word in words:
            dict_type = {}
            # choosing the message id field
            if re.match('Message-ID: ', word):   
        # splitting the string on the space and taking only the value of message id
               message_id = re.split(" ",word)
               value = message_id[1]
            # regular expresion to match on whitespace character
            b = r'(\s|^|$)'
            # if the keyword x-filename is encountered, break from the loop - you don't want to consider emails in the body of the message
            if re.match('X-FileName: ',word):
                break
            # match on the email reference type
            if re.match(b + "To:" + b,word) or re.match(b + "From:" + b,word) or re.match(b + "Cc:" + b,word) or re.match(b + "Bcc:" + b,word):
                
                # email regular expression matcher
              
    #split on the : and , and concatenate the the reference type and the email
                n = re.split(":",word)
                x = re.split(",",n[1])
                  
                concat_str = []
                for i in range(len(x)):
                      
                    if re.match('[^"](.*)@(.*).*',x[i]):
                        
                        concat_str.append(n[0] +":"+ x[i]) 
                    if len(concat_str) > 1:
                         for i in range(len(concat_str)):
                                
#making sure the message ID for an email address has no duplicates. We could do this or use a set() for the list of message id in the reducer 
                             if concat_str[i] not in dict_keys.keys():      
                                 dict_keys[concat_str[i]] = value
                                 print "%s\t%s" %(concat_str[i],dict_keys[concat_str[i]])

                    else:
                        
                        dict_keys[concat_str[0]] = value
                        print "%s\t%s" %(concat_str[0],dict_keys[concat_str[0]])
                             


           

           
