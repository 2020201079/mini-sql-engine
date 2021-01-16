import os
import sys
import sqlparse
from sqlparse.tokens import Keyword, DML
from parsedQuery import parsedQuery

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

def tablesExist(tableNames, tablesFromMeta ):
    for tab in tableNames:
        found = False
        for t in tablesFromMeta:
            if(t.name == tab):
                found = True
                break
        if(found == False):
            printError("Didnt find table " + t.name)
            return False
    return True

def colExist(colNamesQuery,tablesFromMeta):
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

def main():
    tables = parseMetadataFile("files/metadata.txt")
    #sqlQuery = input()
    sqlQuery = "select A, D from table1, table2 where a=10 AND b=20 order by a ASC group by c"

    pq = parsedQuery(sqlQuery)
    print("printing tables :")
    for tab in pq.tables:
        print(tab)
    
    print("printing colums :")
    for col in pq.colums:
        print(col)

    if not tablesExist(pq.tables,tables):
        printError("One of the table does not exists ")
    else:
        print("All tables exists ")
    
    if not colExist(pq.colums,tables):
        printError("One of the col does not exists ")
    else:
        print("All cols exist")

    
if __name__ == "__main__":
    main()