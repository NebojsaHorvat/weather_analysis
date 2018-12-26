
import sys

def mapByUSStateTemplate(line):
    columns = line.split(",")
    return (columns[8], ( (columns[20],columns[21]),(columns[22],columns[23]),columns[24] ) )

def mapByUSState(line):
    columns = line.split(",")
    injuries = int(columns[20]) + int(columns[21])
    deaths = int(columns[22]) + int(columns[23])
    matherial_demage = float(columns[24][:-1])
    matherial_demage_scal = columns[24][-1]
    if matherial_demage_scal == 'K' :
        matherial_demage *= 1000
    elif matherial_demage_scal == 'M' :
        matherial_demage *= 1000000
    
    return (columns[8], ( injuries, deaths, matherial_demage) )

if __name__ == "__main__":
    with open("../storm_data/Stormdata_1996_red.csv") as f:
        content = f.readlines()
        maped = mapByUSState(content[6])
        print(maped)