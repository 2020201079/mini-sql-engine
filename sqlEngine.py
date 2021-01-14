import os
import sys
import sqlparse
from sqlparse.tokens import Keyword, DML

def printError(error):
    print(error)
    exit()

class Table:
    def __init__(self,name):
        self.name = name
        self.attributes = []

class parsedQuery:
    tables = []
    colums = []
    isWherePresent = False
    isSelectPresent = False
    isFromPresent = False
    isGroupByPresent = False
    isOrderByPresent = False
    columNames = ""
    tableNames = ""
    def __init__(self,query):
        tokenList = sqlparse.parse(query)[0].tokens
        self.__checkTokens(tokenList)
        if(tokenList[0].value.upper() != "SELECT" ):
            printError("sql query should start with select DML")

        if not self.isFromPresent:
            printError("there is no from keyword in the query")
        
        #######getting col names here
        i=1
        while i<len(tokenList):
            if(tokenList[i].ttype is sqlparse.tokens.Keyword and tokenList[i].value.upper() == "FROM"):
                break;
            elif(tokenList[i].ttype is not sqlparse.tokens.Text.Whitespace):
                self.columNames += tokenList[i].value
            i = i+1
        self.colums = self.columNames.split(",")
        self.colums = [x.strip() for x in self.colums]
        

        #######getting table names here
        i=1
        while not (tokenList[i].ttype is sqlparse.tokens.Keyword and tokenList[i].value.upper() == "FROM"):
            i = i+1
        i = i+1
        #currently i is pointing to next of from, table names are between i to next keyword or where instance
        while i<len(tokenList):
            if(tokenList[i].ttype is sqlparse.tokens.Keyword or self.__isWhereClass(tokenList[i])):
                break
            elif(tokenList[i].ttype is not sqlparse.tokens.Text.Whitespace):
                self.tableNames += tokenList[i].value
            i = i+1
        self.tables = self.tableNames.split(",")
        self.tables = [x.strip() for x in self.tables]

    def __checkTokens(self,tokenList):
        for token in tokenList:
            if(token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM"):
                self.isFromPresent = True
            if(token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "SELECT"):
                self.isSelectPresent = True
            if(token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "GROUP BY"):
                self.isGroupByPresent = True
            if self.__isWhereClass(token):
                self.isWherePresent = True
                self.__parseWhereCondition(token)
            if(token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "ORDER BY"):
                self.isOrderByPresent = True  
    def __isWhereClass(self,token):
        if isinstance(token,sqlparse.sql.Where):
            return True
        return False
    def __parseWhereCondition(self,token):
        print()
        #print("tokens in where class (still not implemented): ",token.tokens)

        
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
            return False
    return True

def main():
    tables = parseMetadataFile("files/metadata.txt")
    #sqlQuery = input()
    sqlQuery = "select a, b from table1, table2 where a=10 AND b=20 order by a ASC group by c"
    '''
    parsed = sqlparse.parse(sqlQuery)
    print(parsed[0].tokens) #0 for first statement. Our condition always has one statement
    tokenList = parsed[0].tokens
    for token in tokenList:
        print(token.ttype," ",token.value)
    '''
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

    
if __name__ == "__main__":
    main()