import os
import sys
import sqlparse
from sqlparse.tokens import Keyword, DML
from parsedQuery import parsedQuery
import csv
from collections import defaultdict

def printError(error):
    print(error)
    exit()

class Table:
    def __init__(self,name):
        self.name = name
        self.attributes = []

def parseMetadataFile(path):
    tables = []
    if not os.path.exists(path):
        printError("metadata file does not exists")
    else:
        print("meta file exists ")
        metaFile = open(path,'r')
        while True:
            line = metaFile.readline().strip()
            if not line:
                break
            if(line == "<begin_table>"):
                tableName = metaFile.readline().strip()
                if not tableName:
                    printError("metadata file format is wrong")
                currTable = Table(tableName)
                while(True):
                    attribute = metaFile.readline().strip()
                    if not attribute:
                        printError("metadata file format is wrong")
                    if(attribute == "<end_table>"):
                        break
                    currTable.attributes.append(attribute)
                tables.append(currTable)
            else:
                printError("metadata file format is wrong")
        metaFile.close()
        return tables

def tablesExistInMeta(tableNames, tablesFromMeta ):
    for tab in tableNames:
        found = False
        for t in tablesFromMeta:
            if(t.name == tab):
                found = True
                break
        if(found == False):
            printError("Didnt find table " + tab )
            return False
    return True

def colExistInMeta(colNamesQuery,tablesFromMeta):
    for colName in colNamesQuery:
        found = False
        for t in tablesFromMeta:
            if colName in t.attributes:
                found = True
                break
        if(found == False):
            printError("didn't find col " + colName)
            return False
    return True

def getColums(t,tableFromMetaData):
    for table in tableFromMetaData:
        if(table.name == t):
            return table.attributes
    printError("error in getting col data of table "+ t)

def joinTablesHelper(dic1,dic2):
    keys1 = list(dic1.keys())
    print(keys1)
    keys2 = list(dic2.keys())
    print(keys2)
    len1 = len(dic1[keys1[0]])
    len2 = len(dic2[keys2[0]])
    ans = defaultdict(list)
    for i in range(len1):
        for j in range(len2):
            for k1 in range(len(keys1)):
                ans[keys1[k1]].append(dic1[keys1[k1]][i])
            for k2 in range(len(keys2)):
                ans[keys2[k2]].append(dic2[keys2[k2]][j])
    return ans



def joinTables(tableNames,tableFromMetaData):
    folderPath = "files/"
    tableDictLists = []
    for t in tableNames:
        if not os.path.exists(folderPath+t+'.csv'):
            printError("table csv does not exists of "+ t)
        colums = getColums(t,tableFromMetaData)
        tableDict = defaultdict(list)
        csvFile = open(folderPath+t+'.csv','r')
        csvReader = csv.reader(csvFile)
        for line in csvReader:
            line = [int(l) for l in line]
            if(len(line) != len(colums)):
                printError("number of csv colums does not match with metadata colums")
            for i in range(len(colums)):
                tableDict[colums[i]].append(line[i])
        csvFile.close()
        tableDictLists.append(tableDict)
    print(tableDictLists[0])
    print(tableDictLists[1])
    #ans = joinTablesHelper(tableDictLists[0],tableDictLists[1])
    ans = tableDictLists[0]
    for i in range(1,len(tableDictLists)):
        ans = joinTablesHelper(ans,tableDictLists[i])


def main():
    tablesFromMetaData = parseMetadataFile("files/metadata.txt")

    #sqlQuery = input()
    sqlQuery = "select A, D from a, b,c where a=10 AND b=20 order by a ASC group by c"

    pq = parsedQuery(sqlQuery)

    if not tablesExistInMeta(pq.tables,tablesFromMetaData):
        printError("One of the table does not exists ")
    
    if not colExistInMeta(pq.colums,tablesFromMetaData):
        printError("One of the col does not exists ")
    
    tablesAfterJoin = joinTables(pq.tables,tablesFromMetaData)
    


    
if __name__ == "__main__":
    main()