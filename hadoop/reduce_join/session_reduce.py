#!/usr/bin/python

import sys

def read_key_value(file):
    for line in file:
        # split the line into components, before and after the tab
        yield line.strip().split('\t')

def main():
    current_vin = None
    vin_list = []
    session_count_d = 0
    click_event_count_d = 0
    event_bool_count_d = 0
    count_d_s = 0
    count_d_v = 0
    vin_list = []
    join_count = 0

    for vin, session_count, click_event_count, event_bool_count, count_s,count_v in read_key_value(sys.stdin):
        # eval() converts a data structure described on a string
        # into that internal data structure (for example, a dictionary).
        session_count = int(session_count)
        click_event_count = int(click_event_count)
        event_bool_count = int(event_bool_count)
        count_s = int(count_s)
        count_v = int(count_v)

        # Assemble
        if vin == current_vin:
            if session_count == 0:
               count_d_s+=count_s
               count_d_v+=count_v         
               continue
            else:               
               session_count_d+=session_count
               click_event_count_d+=click_event_count
               event_bool_count_d+=event_bool_count
               continue       
        else:
            if current_vin and session_count_d!=0:
                    join_count +=1
                    print '{}\t{}\t{}\t{}\t{}\t{}'.format(current_vin, session_count_d, click_event_count_d, event_bool_count_d, count_d_s,count_d_v)
            current_vin=vin
           
            count_d_s = count_s
            count_d_v = count_v
     
            session_count_d = session_count
            click_event_count_d = click_event_count
            event_bool_count_d = event_bool_count
#for last record
    if vin == current_vin:
        if session_count_d!=0:
           join_count+=1
           print '{}\t{}\t{}\t{}\t{}\t{}'.format(current_vin, session_count_d,click_event_count_d,event_bool_count_d,count_d_s,count_d_v)

    print '{}\t'.format(join_count)
if __name__ == "__main__":
    main()
