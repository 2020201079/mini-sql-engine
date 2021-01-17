import sqlparse
import os
import sys
from sqlparse.tokens import Keyword, DML

def printError(error):
    print(error)
    exit()

class parsedQuery:
    tables = []
    colums = []
    isWherePresent = False
    isSelectPresent = False
    isFromPresent = False
    isGroupByPresent = False
    isOrderByPresent = False
    comparisonsInWhere = []
    LogicOperatorInWhere = ""
    __columNames = ""
    __tableNames = ""
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
                break
            elif(tokenList[i].ttype is not sqlparse.tokens.Text.Whitespace):
                self.__columNames += tokenList[i].value
            i = i+1
        self.colums = self.__columNames.split(",")
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
                self.__tableNames += tokenList[i].value
            i = i+1
        self.tables = self.__tableNames.split(",")
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
        isLogicOperatorPresent = False
        
        tokenList = token.tokens
        countCond = 0
        for t in tokenList:
            print(t.ttype,t.value)
            if(t.ttype == sqlparse.tokens.Keyword):
                if not (t.value.upper() == "AND" or t.value.upper() == "OR" or t.value.upper() == "WHERE"):
                    printError("syntax error is where "+ t.value)
            if(t.ttype == sqlparse.tokens.Keyword and t.value.upper() == "AND"):
                isLogicOperatorPresent = True
                self.LogicOperatorInWhere = "AND"
                countCond = countCond +1
            elif(t.ttype == sqlparse.tokens.Keyword and t.value.upper() == "OR"):
                isLogicOperatorPresent = True
                self.LogicOperatorInWhere = "OR"
                countCond = countCond +1
        
        if(countCond >1):
            printError("Only one OR or AND is supported")
        if(isLogicOperatorPresent):
            if len(tokenList)<7:
                printError("Error in syntax of where condition")
            if not isinstance(tokenList[2],sqlparse.sql.Comparison):
                printError("Error in syntax of where condition")
            self.comparisonsInWhere.append(tokenList[2].value)
            if not isinstance(tokenList[6],sqlparse.sql.Comparison):
                printError("Error in syntax of where condition")
            self.comparisonsInWhere.append(tokenList[6].value)
        else:
            if len(tokenList)<3:
                printError("Error in syntax of where condition")
            if not isinstance(tokenList[2],sqlparse.sql.Comparison):
                printError("Error in syntax of where condition")
            self.comparisonsInWhere.append(tokenList[2].value)
        
        for c in self.comparisonsInWhere:
            print(c)