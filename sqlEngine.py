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
    def __init__(self,query):
        tokenList = sqlparse.parse(query)[0].tokens
        if(tokenList[0].value.upper() != "SELECT" ):
            printError("sql query should start with select DML")
        if not self.fromExistsInToken(tokenList):
            printError("there is no from keyword in the query")
        
    
    def fromExistsInToken(self,tokenList):
        for token in tokenList:
            if(token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM"):
                return True
        return False
        



def parseMetadataFile(path):
    tables = []
    if not os.path.exists(path):
        print("metadata file does not exists")
        exit()
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
                    print("metadata file format is wrong")
                    exit()
                currTable = Table(tableName)
                while(True):
                    attribute = metaFile.readline().strip()
                    if not attribute:
                        print("metadata file format is wrong")
                        exit()
                    if(attribute == "<end_table>"):
                        break
                    currTable.attributes.append(attribute)
                tables.append(currTable)
            else:
                print("metadata file format is wrong")
        metaFile.close()
        return tables

def main():
    tables = parseMetadataFile("files/metadata.txt")
    #sqlQuery = input()
    sqlQuery = "select max(a) from table1 order by a ASC"
    parsed = sqlparse.parse(sqlQuery)
    print(parsed[0].tokens) #0 for first statement. Our condition always has one statement
    tokenList = parsed[0].tokens
    for token in tokenList:
        print(token.ttype," ",token.value)
    print(sqlparse.sql.Statement(parsed[0]).get_type()) # outputs SELECT
    print(sqlparse.sql.Identifier(parsed[0]).get_array_indices())
    

    print(sqlparse.sql.IdentifierList(parsed[0]).get_identifiers())
    ids = sqlparse.sql.IdentifierList(parsed[0]).get_identifiers()
    for i in ids:
        print(i)
    
    pq = parsedQuery(sqlQuery)



    
if __name__ == "__main__":
    main()