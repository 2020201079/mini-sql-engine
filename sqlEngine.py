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
    keys2 = list(dic2.keys())
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
    ans = tableDictLists[0]
    for i in range(1,len(tableDictLists)):
        ans = joinTablesHelper(ans,tableDictLists[i])
    return ans

def selectColsFromTable(table,cols):
    ans = defaultdict(list)
    for c in cols:
        ans[c] = table[c]
    return ans


def parseCondition(condition):
    ans = []
    operators = ['>','<','<=','>=','=']
    for op in operators:
        if(condition.find(op) != -1):
            col1 = condition[0:condition.find(op)]
            col2 = condition[condition.find(op)+len(op):]
            ans.append(col1.strip())
            ans.append(op.strip())
            ans.append(col2.strip())
            break
    print(ans)
    return ans

def checkValid(currVal,operator,value):
    value = int(value)
    if(operator == "<"):
        return currVal < value
    elif(operator == ">"):
        return currVal > value
    elif(operator == "<="):
        return currVal <= value
    elif(operator == ">="):
        return currVal >= value
    elif(operator == "="):
        return currVal == value
    else:
        printError("Operator is not valid : " + operator)

def applyWhereCondition(pq,table):
    cols = list(table.keys())
    tableLen = len(table[cols[0]])
    print(cols)
    conditionsListAsString = pq.comparisonsInWhere
    logicOperator = pq.LogicOperatorInWhere
    ans = defaultdict(list)
    if(logicOperator == ""):
        if(len(conditionsListAsString) != 1):
            printError("Where condition syntax error")
        conditionList = parseCondition(conditionsListAsString[0])
        colName = conditionList[0].strip()
        operator = conditionList[1]
        value = conditionList[2]
        if not colName in cols:
            printError("Col name in where is not in the joined table col name : "+colName)
        for i in range(tableLen):
            currVal = table[colName][i]
            if(checkValid(currVal,operator,value)):
                for c in cols:
                    ans[c].append(table[c][i])
        return ans

    elif(logicOperator.upper() == "AND"):
        if(len(conditionsListAsString) != 2):
            printError("Where condition syntax error")
        conditionList1 = parseCondition(conditionsListAsString[0])
        colName1 = conditionList1[0].strip()
        operator1 = conditionList1[1]
        value1 = conditionList1[2]
        if not colName1 in cols:
            printError("Col name in where is not in the joined table col name : "+colName1)

        conditionList2 = parseCondition(conditionsListAsString[1])
        colName2 = conditionList2[0].strip()
        operator2 = conditionList2[1]
        value2 = conditionList2[2]
        if not colName2 in cols:
            printError("Col name in where is not in the joined table col name : "+colName2)

        for i in range(tableLen):
            currVal1 = table[colName1][i]
            currVal2 = table[colName2][i]
            if(checkValid(currVal1,operator1,value1) and checkValid(currVal2,operator2,value2)):
                for c in cols:
                    ans[c].append(table[c][i])
        return ans

    elif(logicOperator.upper() == "OR"):
        if(len(conditionsListAsString) != 2):
            printError("Where condition syntax error")
        conditionList1 = parseCondition(conditionsListAsString[0])
        colName1 = conditionList1[0].strip()
        operator1 = conditionList1[1]
        value1 = conditionList1[2]
        if not colName1 in cols:
            printError("Col name in where is not in the joined table col name : "+colName1)

        conditionList2 = parseCondition(conditionsListAsString[1])
        colName2 = conditionList2[0].strip()
        operator2 = conditionList2[1]
        value2 = conditionList2[2]
        if not colName2 in cols:
            printError("Col name in where is not in the joined table col name : "+colName2)

        for i in range(tableLen):
            currVal1 = table[colName1][i]
            currVal2 = table[colName2][i]
            if(checkValid(currVal1,operator1,value1) or checkValid(currVal2,operator2,value2)):
                for c in cols:
                    ans[c].append(table[c][i])
        return ans
    else:
        printError("Operator not supported " + logicOperator)
        


def printTable(table):
    keys = list(table.keys())
    if len(keys) == 0:
        print("Table is empty ")
        return
    length = len(table[keys[0]])
    for k in keys:
        print(k,end=" ")
    print()
    for i in range(length):
        for k in keys:
            print(table[k][i],end=" ")
        print()

def main():
    tablesFromMetaData = parseMetadataFile("files/metadata.txt")

    #sqlQuery = input()
    sqlQuery = "select A, D,G from a, b,c  where F = 15 and G = 16 order by a ASC group by c"

    pq = parsedQuery(sqlQuery)

    if not tablesExistInMeta(pq.tables,tablesFromMetaData):
        printError("One of the table does not exists ")
    
    if not colExistInMeta(pq.colums,tablesFromMetaData):
        printError("One of the col does not exists ")
    
    tablesAfterJoin = joinTables(pq.tables,tablesFromMetaData)

    if(pq.isWherePresent):
        tableAfterWhere = applyWhereCondition(pq,tablesAfterJoin)

    printTable(tableAfterWhere)

    tableAfterSelectingCols = selectColsFromTable(tableAfterWhere,pq.colums)
    
    printTable(tableAfterSelectingCols)
    
if __name__ == "__main__":
    main()