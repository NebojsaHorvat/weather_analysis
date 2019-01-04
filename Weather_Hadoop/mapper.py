#!/usr/bin/python

import sys

def mapByDisasterType(line):
    columns = line.split(",")
    try:
        injuries = int(columns[20]) + int(columns[21])
    except:
        injuries = 0
    try:
        deaths = int(columns[22]) + int(columns[23])
    except:
        deaths = 0
    try:
        matherial_demage = float(columns[24][:-1])
        matherial_demage_scal = columns[24][-1]
        if matherial_demage_scal == 'K' :
            matherial_demage *= 1000
        elif matherial_demage_scal == 'M' :
            matherial_demage *= 1000000
    except:
        matherial_demage = 0.0
    try:
        typeOfDisaster = columns[12]
    except:
        typeOfDisaster = "ERROR"

#     return (typeOfDisaster , ( injuries, deaths, matherial_demage) )
    return '%s,%s,%s,%s' % (typeOfDisaster, injuries, deaths, matherial_demage)

# Input takes from standard input
for myline in sys.stdin:
    # Remove whitespace either side
    myline = myline.strip()
    # Map by disaster type
    mapped = mapByDisasterType(myline)
    # Print mapped 
    print mapped
    
