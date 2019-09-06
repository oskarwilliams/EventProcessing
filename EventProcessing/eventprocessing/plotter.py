import json
import seaborn
import logging
import pandas
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import matplotlib.animation as animation

outputAverageName = 'outputAverageValue.json'

def importFiles():
    averages = {}
    locationsDict = []
    with open('locations.json', 'r') as infile:
        locationsDict = json.load(infile)
    for location in locationsDict:
        with open(f"outputs/{outputAverageName}AtID{location['id']}.json", "r") as infile:
            averages[location['id']] = json.load(infile)
    return locationsDict, averages

def plotter(locationsDict, averages):
    data = [{
        'X' : [],
        'Y' : [],
        'Z' : []
    } for _ in range(40)]
    for location in locationsDict:
        for i in range(len(averages[location['id']])):
            data[i]['X'] += [location['x']]
            data[i]['Y'] += [location['y']]
            data[i]['Z'] += [averages[location['id']][i]['averageValue']]

    # for values in averages['25a7bbac-8b5d-425d-9d18-bf6079777f67']:
    #     otherData = { [values['startTime']]}


    # otherData = {}
    # for location in locationsDict:
    #     for values in averages[location['id']]:
    #         otherData[values['startTime']]['X'] += [location['x']]
    #         otherData[values['startTime']]['Y'] += [location['y']]
    #         otherData[values['startTime']]['Z'] += [values['averageValue']]

    chosenIndex = 2
    fig = plt.figure()
    ax = fig.gca(projection='3d')
    ax.plot_trisurf(data[chosenIndex]['X'],data[chosenIndex]['Y'],data[chosenIndex]['Z'], cmap=plt.cm.jet, linewidth=0.2)
    plt.show()

    # for values in averages['25a7bbac-8b5d-425d-9d18-bf6079777f67']:
    #         print(values['startTime'])

    # chosenTime = '1567782188.3859286'
    # fig = plt.figure()
    # ax = fig.gca(projection='3d')
    # ax.plot_trisurf(otherData[chosenTime]['X'],otherData[chosenTime]['Y'],otherData[chosenTime]['Z'], cmap=plt.cm.jet, linewidth=0.2)
    
def runProg():
    locationsDict, averages = importFiles()
    plotter(locationsDict, averages)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runProg()