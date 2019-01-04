#!/usr/bin/python

from operator import itemgetter
import sys

current_disaster_type = ""
current_deaths = 0
current_injuries = 0
current_demage = 0

word = ""

# Input takes from standard input
for myline in sys.stdin:
    # Remove whitespace either side
    line = myline.strip()
    elements = line.split(",")
    # Split the input
    disaster_type = elements[0]
    # Convert variables to numbers
    try:
        injuries = int(elements[1])
    except:
        injuries = 0
    try:
        deaths = int(elements[2])
    except:
        deaths = 0
    try:
        demage = float(elements[3])
    except:
        demage = 0.0
    
    if current_disaster_type == disaster_type:
        current_deaths += deaths
        current_injuries += injuries
        current_demage += demage
    else:
        if current_disaster_type:
            # Write result to standard output
            print '%s,%s,%s,%s' % (current_disaster_type, current_injuries, current_deaths, current_demage)
        current_disaster_type = disaster_type
        current_deaths = deaths
        current_injuries = injuries
        current_demage = demage

# Do not forget to output the last word if needed!
if current_disaster_type == disaster_type:
    print '%s,%s,%s,%s' % (current_disaster_type, current_injuries, current_deaths, current_demage)
