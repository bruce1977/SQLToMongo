# -*-coding: utf-8 -*-
import pymongo
import pymssql
import sys
import json

def GetArgv():
    fin = open('Config.json','r')
    for eachLine in fin:
        line = eachLine.strip()               #去除每行首位可能的空格，并且转为Unicode进行处理
        line = line.strip(',')                                 #去除Json文件每行大括号后的逗号
        js = None
        js = json.loads(line)#加载Json文件
        print(js)




#清除Mongo数据库
def MongoClea(mgDB,mgCollection,conn):
    db = conn[mgDB]
    print(conn.database_names())
    collection = db[mgCollection]
    collection.remove()

'''
def MongoiInsert(mgPath='mgPath',mgPort='mgPort',mgDB='mgDB',mgCollection='mgCollection'):
    conn = pymongo.MongoClient(mgPath,mgPort)
    MongoClear(mgDB,mgCollection,conn)
    con = pymssql.connect(server='10.10.73.208:4399', user='sa', password='123456', database="PSPMS_Dev")
    cursor = con.cursor()
    cursor.execute('select * from company')
    for row in cursor:
        MongoCompanyInsert(row[1],row[2],row[0])
        print(row)
    con.close()
'''
class SQLtoMongo:
    sqlPath =''
    sqlUser = ''
    sqlPWD = ''
    sqlDB = ''
    con = None
    #构造函数创建于SQL连接
    def __init__(self,sqlServer,sqlUser,sqlPWD,sqlDB):
        self.sqlPath = sqlServer
        self.sqlUser = sqlUser
        self.sqlPWD = sqlPWD
        self.sqlDB = sqlDB
        self.con = pymssql.connect(server=self.sqlPath, user=self.sqlUser, password=self.sqlPWD, database=self.sqlDB)
        print('SQLConnected Successfully')

    #向MG数据库转换
    def StartTransPort(self,mgPath,mgPort,mgDB,mgCollection):
        conn = pymongo.MongoClient(mgPath,mgPort)
        MongoClea(mgDB,mgCollection,conn)
        cursor = self.con.cursor()
        column = {}
        rowDone = {}
        i1 = 0
        cursor.execute('select name from syscolumns where id=(select max(id) from sysobjects where xtype=\'u\' and name= \''+mgCollection+'\')')
        for r in cursor:
            column[i1] = str(r).strip(',').strip(')').strip('(')
            i1 = i1+1
        cursor.execute('select * from '+mgCollection)
        for row in cursor:
            i = 0
            rowDone = {}
            for things in row:
                rowDone[column[i]] = things
                i = i+1
            self.MongoInsert(rowDone,conn,mgDB,mgCollection)
            print(row)

    #向Mongo插入数据
    def MongoInsert(self,row,conn,mgDB,mgCollection):
        db = conn[mgDB]
        print(conn.database_names())
        collection = db[mgCollection]
        account = collection
        account.insert(row)



fin = open('Config.json','r')
ConfigString = fin.read()
decodejson = json.loads(ConfigString)
StM = SQLtoMongo(**decodejson[0])
for row in decodejson:
    if(row != decodejson[0]):
        StM.StartTransPort(**row)
        print(row)
