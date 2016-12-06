#!/usr/bin/python

# Read session

import sys

event_field_list = [ 'timestamp', 'event_action', 'event_target', 'vin' ]
vin_field_list = [ 'condition', 'year', 'make', 'model', 'price', 'mileage' ]


def read_session(file):
    for line in file:
        # split the line into components: user_id:sessions_type, event_list, in dictionary
        yield line.strip().split('\t')


def main(): 
  
        data = read_session(sys.stdin)
        for info in data:
            session_count = 1
            click_event_count = {}
            event_bool = {}
            count = 0
            if len(info) == 3:
               user_id,session_type = info[0].split(':')
               event_list = eval(info[1])
               vin_dict = eval(info[2])
               for i in event_list:
                   if i['event_action'] == 'click':
                      if i['vin'] not in click_event_count.keys():
                          click_event_count[i['vin']] = 1
                      else:
                          click_event_count[i['vin']] +=1
                   
                   if i['event_target'] == 'contact form':
                      if i['vin'] not in event_bool.keys():
                         event_bool[i['vin']] = 1
              
               vin_keys = vin_dict.keys()
               for vin in vin_keys:
                   if vin not in click_event_count.keys():
                      click_event_count[vin] = 0
                   if vin not in event_bool.keys():
                      event_bool[vin] = 0
               for vin in vin_keys:  
                   
                   print '{}\t{}\t{}\t{}\t{}\t{}'.format(vin, session_count, click_event_count[vin], event_bool[vin],0,0)
            if len(info) == 1:
               flag = 0
               vin,s_v,count = info[0].split(',')
               if s_v == 'SRP':            
                  print '{}\t{}\t{}\t{}\t{}\t{}'.format(vin,0, 0, 0,count,0)
               else:
                  print '{}\t{}\t{}\t{}\t{}\t{}'.format(vin,0, 0, 0,0,count)

if __name__ == "__main__":
    main()
