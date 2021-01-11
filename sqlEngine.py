import os
import sys

class Table:
    def __init__(self,name):
        self.name = name
        self.attributes = []

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

if __name__ == "__main__":
    main()