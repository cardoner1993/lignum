import json
import logging

from os import path, listdir, makedirs


def read_json(filePath, returnError=False):
    jsonContent = {}  # jsonContent can be a DICTIONARY or a LIST or ...

    errorReading = False
    if path.exists(filePath):
        try:
            with open(filePath) as fp:
                jsonContent = json.load(fp)
        except:
            errorReading = True
            logging.error(' Error reading json ' + filePath + '\n\n', exc_info=True)
    else:
        errorReading = True
        logging.warning('File ' + filePath + ' does not exist')

    if returnError == True:
        return jsonContent, errorReading
    else:
        return jsonContent  # jsonContent can be a DICTIONARY or a LIST or ...


def write_json(jsonContent, filePath, mode='w'):


    # jsonContent can be a DICTIONARY or a LIST or ...
    # mode can be append too, mode='a'

    errorWriting = False

    folderPath = path.dirname(path.abspath(filePath))
    if not path.exists(folderPath):
        makedirs(folderPath)

    try:
        with open(filePath, mode) as f:
            aux = json.dumps(jsonContent, sort_keys=True, indent=4)
            f.write(aux)
    except:
        errorWriting = True
        logging.error(' problem writing file' + filePath + ' from json\n', exc_info=True)

    return errorWriting


def read_and_write_json(filePath):
    jsonContent, errorReading = read_json(filePath, returnError=True)
    wrongFile = ''
    if not errorReading:
        write_json(jsonContent, filePath)
    else:
        wrongFile = filePath
    return wrongFile


def check_jsons_of_a_folder(folderPath):
    wrongList = []
    for fileName in listdir(folderPath):
        filePath = folderPath + '/' + fileName
        if path.isfile(filePath):
            if read_and_write_json(filePath):
                wrongList.append(filePath)

        elif path.isdir(filePath):
            check_jsons_of_a_folder(filePath)
    if wrongList:
        logging.info('WrongFiles are\n' + ';'.join(wrongList))

