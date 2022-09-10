import pandas as pd
import requests
import csv
import numpy as np
import time
import glob, os, os.path


def extracParseSave(numberOfUsers, numberOfFiles):
    for t in range(numberOfFiles):
        numOfUsers = getUsers(numberOfUsers)
        dataParsed = parseUsers(numOfUsers)
        saveToCsv(dataParsed)


def deleteColumn(dataFrame, column):
    del dataFrame[column]
    return dataFrame


def creatingGeralDF():
    csvFileList = glob.glob(os.path.join("*.csv"))
    geralDataFrame = pd.DataFrame()
    for file in csvFileList:
        tempDf = pd.read_csv(file)
        geralDataFrame = pd.concat([geralDataFrame, tempDf])
    return geralDataFrame 


def getUsers(numberOfUsers):
        csvFileList = glob.glob(os.path.join("*.csv"))
        csvFilesCouter = int(len(csvFileList))
        print(f'=============================={csvFilesCouter + 1}º file processing==============================')
        r = requests.get(f"https://randomuser.me/api/?results={numberOfUsers}")
        s = r.json()
        usersListTemp = s['results']
        print('All user data has been successfully extracted')
        s.clear()
        return usersListTemp


def parseUsers(usersList):
    def getFields(user):
        userDict = {'title': user['name']['title'], 
                    'fName': user['name']['first'],
                    'lName': user['name']['last'],
                    'city': user['location']['city'],
                    'country': user['location']['country'],
                    'email': user['email'],
                    'phone': user['phone'],
                    'dob': user['dob']['date']
                    }        
        return userDict    
    parseUsersListTemp = list(map(lambda user: getFields(user), usersList))
    
    return parseUsersListTemp


def saveToCsv(dataParsed):
    csvFileList = glob.glob(os.path.join("*.csv"))
    csvFilesCouter = int(len(csvFileList))
    print (f'Saving the {csvFilesCouter + 1}º file...')
    time.sleep(1)
    print('The date have been saved')
    df = pd.DataFrame(dataParsed)
    df.to_csv(f'users{csvFilesCouter + 1}.csv')


def createDfToParse():
    csvFileList = glob.glob(os.path.join("*.csv"))
    csvFilesCouter = int(len(csvFileList))
    if csvFilesCouter == 0:
        print('Parse data function failed! There are no csv files in the directory.')
    else:
        all_files = glob.glob(os.path.join("*.csv")) 
        listOfusers = []
        for file in all_files:
            temp = pd.read_csv(file)
            listOfusers.append(temp)
        df = pd.concat(listOfusers)
    return df


def renameColumns(df):
    del df['Unnamed: 0']
    del df['0']
    df.columns = ['Title','Name','Surname','City','Country','Email','Phone','Birthdate']
    return df


def getColumnPercentage(df, column):
    preserveData = df['Title'].copy() 
    df['Title'] = df['Title'].apply(lambda x: 'man' if x in ['Monsieur', 'Mr'] else 'woman')
    percentage = df[column].value_counts(normalize = True)*100
    df['Title'] = preserveData
    percentage = pd.DataFrame(percentage)
    percentage = percentage.reset_index()
    percentage.columns = ['Title', 'Percentage']
    pd.set_option("display.precision", 1)
    return percentage

def getGenderPercentage(df):
    genderPercentage = df.to_dict()
    malePercentage = genderPercentage['man']
    femalePercentage = genderPercentage['woman']
    return {'malePercentage': malePercentage, 'femalePercentage': femalePercentage}


def isolatingDF(df):
    new = df.groupby(['year', 'Title']).count()
    new.reset_index(inplace=True)
    new = new[['year', 'Title', 'Name']]
    return new


def creatingWomanDf(df):
    womanDf = pd.DataFrame()
    womanDf = df.query("Title == 'woman'")
    womanDf = womanDf[['year', 'Name']]
    womanDf = womanDf.set_index('year')
    womanDf = womanDf.rename(columns = {'Name': 'woman'})
    pd.set_option("display.precision", 1)
    return womanDf


def creatingManDf(df):
    manDf = pd.DataFrame()
    manDf = df.query("Title == 'man'")
    manDf = manDf[['year', 'Name']]
    manDf = manDf.rename(columns = {'Name': 'man'})
    manDf = manDf.set_index('year')
    pd.set_option("display.precision", 1)
    return manDf


def creatingGenderGeral(woman, man):
    genderGeral = woman
    genderGeral['man'] = man
    genderGeral = genderGeral.fillna(0)
    genderGeral['manPorcetage'] = (genderGeral['man'] / (genderGeral['man'] + genderGeral['woman']))*100
    genderGeral['womanPorcetage'] = (genderGeral['woman'] / (genderGeral['man'] + genderGeral['woman']))*100
    genderGeral['Borns'] = genderGeral['man'] + genderGeral['woman']
    genderGeral = genderGeral[['Borns', 'manPorcetage', 'womanPorcetage']]
    return genderGeral


def countingBirths(data):
    data['Title'] = data['Title'].apply(lambda x: 'man' if x in ['Monsieur', 'Mr'] else 'woman')
    data['year'] = pd.to_datetime(data['Birthdate']).dt.year
    #Functions call
    newDf = isolatingDF(data)    
    womanDf = creatingWomanDf(newDf)    
    manDf = creatingManDf(newDf)    
    genderGeral = creatingGenderGeral(womanDf, manDf)
    pd.set_option("display.precision", 1) 
    return genderGeral
