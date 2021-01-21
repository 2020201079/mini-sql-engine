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

def colExistInMeta(pq,tablesFromMeta):
    colNamesQuery = pq.colums
    for colName in colNamesQuery:
        if(colName.strip() == "*"):
            for t in tablesFromMeta:
                for colName in t.attributes:
                    pq.colToTableName[colName] = t.name
            continue
        found = False
        for t in tablesFromMeta:
            if colName in t.attributes:
                pq.colToTableName[colName] = t.name
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

def allColsHaveAggregate(pq):
    for col in pq.colums:
        if pq.colToFunc[col] == "":
            return False
    return True

def noneHaveAggregate(pq):
    for col in pq.colums:
        if pq.colToFunc[col] != "":
            return False
    return True

def evaluate(nums,funcName):
    if(funcName.upper() == "MAX"):
        return max(nums)
    if(funcName.upper() == "MIN"):
        return min(nums)
    if(funcName.upper() == "COUNT"):
        return len(nums)
    if(funcName.upper() == "SUM"):
        return sum(nums)
    if(funcName.upper() == "AVERAGE"):
        return sum(nums)/len(nums)
    else:
        printError("Invalid function name : "+ funcName)

def selectColsFromTable(table,pq):

    if "*" in pq.colums:
        if(pq.colToFunc['*'].upper() != "" and pq.colToFunc['*'].upper() != "COUNT" ):
            printError("with * no functions can be associated")
        pq.colums = list(table.keys())
        for k in table.keys():
            pq.colToFunc[k] = pq.colToFunc['*']
    if table is None:
        return
    if not pq.isGroupByPresent: 
        #group by is not present
        if allColsHaveAggregate(pq):
            ans = defaultdict(list)
            for c in pq.colums:
                ans[c] = [evaluate(table[c],pq.colToFunc[c])]
            return ans
        elif noneHaveAggregate(pq):
            ans = defaultdict(list)
            for c in pq.colums:
                ans[c] = table[c]
            return ans
        else:
            printError("without group by all should have aggregate func or none should have aggregate func")
    else:
        return table # group by function has already selected cols

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
    if(pq.isWherePresent == False):
        return table
    cols = list(table.keys())
    tableLen = len(table[cols[0]])
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
        if(value.isdigit()):
            if not colName in cols:
                printError("Col name in where is not in the joined table col name : "+colName)
            for i in range(tableLen):
                currVal = table[colName][i]
                if(checkValid(currVal,operator,value)):
                    for c in cols:
                        ans[c].append(table[c][i])
        else:
            if not colName in cols:
                printError("Col name in where is not in the joined table col name : "+colName)
            if not value in cols:
                printError("Col name in where is not in the joined table col name : "+value)
            for i in range(tableLen):
                currVal = table[colName][i]
                currVal2 = table[value][i]
                if(checkValid(currVal,operator,currVal2)):
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
        if not (value1.isdigit()):
            if not value1 in cols:
                printError("Col name in where is not in the joined table col name : "+value1)

        conditionList2 = parseCondition(conditionsListAsString[1])
        colName2 = conditionList2[0].strip()
        operator2 = conditionList2[1]
        value2 = conditionList2[2]
        if not colName2 in cols:
            printError("Col name in where is not in the joined table col name : "+colName2)
        if not (value2.isdigit()):
            if not value2 in cols:
                printError("Col name in where is not in the joined table col name : "+value2)

        for i in range(tableLen):
            currVal1 = table[colName1][i]
            currVal2 = table[colName2][i]
            if(value1.isdigit() and value2.isdigit()):
                if(checkValid(currVal1,operator1,value1) and checkValid(currVal2,operator2,value2)):
                    for c in cols:
                        ans[c].append(table[c][i])
            elif(value1.isdigit() and (not value2.isdigit()) ):
                if(checkValid(currVal1,operator1,value1) and checkValid(currVal2,operator2,table[value2][i])):
                    for c in cols:
                        ans[c].append(table[c][i])
            elif((not value1.isdigit()) and (not value2.isdigit()) ):
                if(checkValid(currVal1,operator1,table[value1][i]) and checkValid(currVal2,operator2,table[value2][i])):
                    for c in cols:
                        ans[c].append(table[c][i])
            elif((not value1.isdigit()) and (value2.isdigit()) ):
                if(checkValid(currVal1,operator1,table[value1][i]) and checkValid(currVal2,operator2,value2)):
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
        if not (value1.isdigit()):
            if not value1 in cols:
                printError("Col name in where is not in the joined table col name : "+value1)

        conditionList2 = parseCondition(conditionsListAsString[1])
        colName2 = conditionList2[0].strip()
        operator2 = conditionList2[1]
        value2 = conditionList2[2]
        if not colName2 in cols:
            printError("Col name in where is not in the joined table col name : "+colName2)
        if not (value2.isdigit()):
            if not value2 in cols:
                printError("Col name in where is not in the joined table col name : "+value2)

        for i in range(tableLen):
            currVal1 = table[colName1][i]
            currVal2 = table[colName2][i]
            if(value1.isdigit() and value2.isdigit()):
                if(checkValid(currVal1,operator1,value1) or checkValid(currVal2,operator2,value2)):
                    for c in cols:
                        ans[c].append(table[c][i])
            elif(value1.isdigit() and (not value2.isdigit()) ):
                if(checkValid(currVal1,operator1,value1) or checkValid(currVal2,operator2,table[value2][i])):
                    for c in cols:
                        ans[c].append(table[c][i])
            elif((not value1.isdigit()) and (not value2.isdigit()) ):
                if(checkValid(currVal1,operator1,table[value1][i]) or checkValid(currVal2,operator2,table[value2][i])):
                    for c in cols:
                        ans[c].append(table[c][i])
            elif((not value1.isdigit()) and (value2.isdigit()) ):
                if(checkValid(currVal1,operator1,table[value1][i]) or checkValid(currVal2,operator2,value2)):
                    for c in cols:
                        ans[c].append(table[c][i])
        return ans
    else:
        printError("Operator not supported " + logicOperator)

def applyGroupBy(pq,table):
    if(pq.isGroupByPresent == False):
        return table
    colToGroup = pq.groupByCol
    colToSelect = pq.colums
    if not (colToGroup in table.keys()):
        printError("Table does not contain col to group by : "+colToGroup)
    tableLen = len(table[colToGroup])
    for c in colToSelect: #cheching if all the colums have aggreagte function
        if c != colToGroup:
            if(pq.colToFunc[c] == ""):
                printError("With group by the colum should have an aggregate function "+ c)
    
    ans = defaultdict(dict)
    for i in range(tableLen):
        currValue = table[colToGroup][i]
        for k in colToSelect:
            currdic = ans[currValue]
            if k in currdic.keys():
                ans[currValue][k].append(table[k][i])
            else:
                ans[currValue][k] = [table[k][i]]
    finalAns = defaultdict(list)
    for k in ans.keys():
        dic = ans[k]
        for k1 in dic.keys():
            if(k1 == colToGroup):
                finalAns[k1].append(k)
            else:
                finalAns[k1].append(evaluate(dic[k1],pq.colToFunc[k1]))
    return finalAns

def applyOrderBy(table,pq):
    if(pq.isOrderByPresent == False):
        return table
    orderByCol = pq.orderByCol
    if orderByCol not in table.keys():
        printError("Order by colum is not selected : "+ orderByCol)
    listToSort = table[orderByCol]
    listOfTuple = []
    for i in range(len(listToSort)):
        listOfTuple.append((listToSort[i],i))
    if(pq.orderDir.upper() == "ASC"):
        listOfTuple.sort()
        ans = defaultdict(list)
        for t in listOfTuple:
            index = t[1]
            for k in table.keys():
                ans[k].append(table[k][index])
        return ans
    elif(pq.orderDir.upper() == "DESC"):
        listOfTuple.sort(reverse=True)
        ans = defaultdict(list)
        for t in listOfTuple:
            index = t[1]
            for k in table.keys():
                ans[k].append(table[k][index])
        return ans
    else:
        printError("Order by direction is not defined : "+ pq.orderDir)

def applyDistinct(table,pq):
    if (pq.isDistinctPresent == False):
        return table
    colNames = list(table.keys())
    if(len(colNames) <1):
        print("Table is empty")
        return
    tableLen = len(table[colNames[0]])
    s = set()
    ans = defaultdict(list)
    for i in range(tableLen):
        currlist = []
        for k in table.keys():
            currlist.append(table[k][i])
        currTuple = tuple(currlist)
        if currTuple not in s:
            s.add(currTuple)
            for k1 in table.keys():
                ans[k1].append(table[k1][i])
    return ans

def printTable(table,pq):
    if table is None:
        print("Table is empty ")
        return
    keys = list(table.keys())
    if len(keys) == 0:
        print("Table is empty ")
        return
    length = len(table[keys[0]])
    for k in keys:
        if( k != keys[len(keys)-1]):
            if(pq.colToFunc[k] == ""):
                print((pq.colToTableName[k]+"."+k).lower(),end=",")
            else:
                print((pq.colToFunc[k]+'('+pq.colToTableName[k]+"."+k+')'.lower()).lower(),end=",")
        else:
            if(pq.colToFunc[k] == ""):
                print((pq.colToTableName[k]+"."+k).lower())
            else:
                print((pq.colToFunc[k]+'('+pq.colToTableName[k]+"."+k+')').lower())
    for i in range(length):
        for k in keys:
            if( k != keys[len(keys)-1]):
                print(table[k][i],end=",")
            else:
                print(table[k][i])

def main():
    tablesFromMetaData = parseMetadataFile("files/metadata.txt")

    if(len(sys.argv) < 2):
        printError("Query not provided")
    query = sys.argv[1]
    #sqlQuery = "select * from a ;"

    pq = parsedQuery(query)

    if not tablesExistInMeta(pq.tables,tablesFromMetaData):
        printError("One of the table does not exists ")
    
    if not colExistInMeta(pq,tablesFromMetaData):
        printError("One of the col does not exists ")
    
    currTable = joinTables(pq.tables,tablesFromMetaData)

    currTable = applyWhereCondition(pq,currTable)

    currTable = applyGroupBy(pq,currTable)

    currTable = selectColsFromTable(currTable,pq)

    #add disticnt here 
    currTable = applyDistinct(currTable,pq)

    currTable = applyOrderBy(currTable,pq)
    
    printTable(currTable,pq)
    
if __name__ == "__main__":
    main()