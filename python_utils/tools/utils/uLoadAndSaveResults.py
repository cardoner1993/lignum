import pandas as pd
from os import path, makedirs
from lib.python.tools.utils.uDate import from_interval_to_date_range, sanitize_to_datestr


def generate_file_path(resultsPath, resultsPrefix, currentClient, currentLocal, currentDate):
    dateString = sanitize_to_datestr(currentDate)
    filePath = resultsPath + '/' + currentClient + '/' + currentLocal + '/' + resultsPrefix + dateString + '_' + currentLocal + '.csv'
    return path.abspath(filePath)


def store_results(resultsPath, resultsPrefix, resultsDF, currentClient, currentLocal, currentDate):
    filePath = generate_file_path(resultsPath, resultsPrefix, currentClient, currentLocal, currentDate)
    store_data_frame(resultsDF, filePath)
    return filePath


def read_results(resultsPath, resultsPrefix, currentClient, currentLocal, currentDate):
    filePath = generate_file_path(resultsPath, resultsPrefix, currentClient, currentLocal, currentDate)
    resultsDF = read_data_frame(filePath)
    return resultsDF


def store_data_frame(resultsDF, filePath, sep=';', mode='w', header=True, index=False, floatFormat='%.2f'):
    # mode = 'a' append, 'w' write.
    # header = True or False

    folderPath = path.dirname(path.abspath(filePath))
    if not path.exists(folderPath):
        makedirs(folderPath)

    if mode == 'a':
        if not path.exists(filePath):
            headerA = header
        else:
            headerA = False

    if not resultsDF.empty:
        if mode == 'a':
            resultsDF.to_csv(filePath, sep=sep, encoding='utf-8', mode=mode, header=headerA, index=index,
                             float_format=floatFormat)
        else:
            resultsDF.to_csv(filePath, sep=sep, encoding='utf-8', mode=mode, header=header, index=index,
                             float_format=floatFormat)

    else:  # creem el fitxer buit si no hi ha dades
        file = open(filePath, "w")
        file.close()


def read_data_frame(filePath, sep=';', header=0):
    if path.isfile(filePath):
        try:
            resultsDF = pd.read_csv(filePath, header=header, sep=sep)  # , dtype={'key': object})
        except ValueError:
            resultsDF = pd.DataFrame()
    else:
        resultsDF = pd.DataFrame()

    return resultsDF


def concatenate_results(resultsPath, resultsPrefix, currentClient, currentLocal, firstDate, lastDate):

    datesRange = from_interval_to_date_range(firstDate, lastDate, left=True, right=True)

    resultsDFList = []
    for currentDate in datesRange:
        resultsDFList.append(read_results(resultsPath, resultsPrefix, currentClient, currentLocal, currentDate))

    resultsDF = pd.concat(resultsDFList, axis=0, ignore_index=True)

    return resultsDF


if __name__ == "__main__":
    # dummy prova per store_data_frame
    import numpy as np
    df3 = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])
    store_data_frame(df3, 'kk.csv', sep=';', mode='w')
    store_data_frame(df3, 'kk.csv', sep=';', mode='a')

    store_data_frame(df3, 'kkAppend.csv', sep=';', mode='a')
    store_data_frame(df3, 'kkWNoHeader.csv', sep=';', mode='w', header=False)
