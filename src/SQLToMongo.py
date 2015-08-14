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




class SQLServer:
    sqlPath =''
    sqlUser = ''
    sqlPWD = ''
    sqlDB = ''
    con = None
    #构造函数创建于SQL连接
    def __init__(self,host,port,username,pwd,database,provider):
        self.sqlPath = host+':'+str(port)
        self.sqlUser = username
        self.sqlPWD = pwd
        self.sqlDB = database
        self.con = pymssql.connect(server=self.sqlPath, user=self.sqlUser, password=self.sqlPWD, database=self.sqlDB)
        print('SQLConnected Successfully')

class MongoServer:
    host = ''
    port = ''
    database = ''
    username = ''
    pwd = ''
    conn = None
    def __init__(self,host,port,username,pwd,database):
        self.host = host
        self.port = str(port)
        self.database = database
        self.username = username
        self.pwd = pwd
        self.conn = pymongo.MongoClient('mongodb://'+ username +':'+ pwd +'@' +host+':'+str(port))
        print(self.conn.server_info())
        print('Mongo Connected')

        #If you use this function it will clear the target mongo collection
    def TransFromSQL(self,sqlcon,table,mgcollection):
        mgPath = self.host
        mgPort = str(self.port)
        mgDB = self.database
        mgUser = self.username
        mgPWD = self.pwd
        sqlTable = table
        mgCollection = mgcollection
        self.MongoClear(mgDB,mgCollection,self.conn)
        cursor = sqlcon.cursor()
        column = {}
        i1 = 0
        db = self.conn[mgDB]
        collect = db[mgCollection]
        cursor.execute('select name from syscolumns where id=object_id(N\''+sqlTable+'\')')
        for r in cursor:
            column[i1] = str(r).strip(',').strip(')').strip('(').strip(',').strip('\'')#去除数组分割剩下的垃圾
            i1 = i1+1
        collect.drop_indexes()
        collect.create_index(column[0])
        cursor.execute('select * from '+sqlTable)
        for row in cursor:
            i = 0
            rowDone = {}
            for things in row:
                rowDone[column[i]] = things
                i = i+1
            try:
                self.MongoInsert(rowDone,self.conn,mgDB,mgCollection)
            except:
                print('Insert Failed')
                raise


    def TransArrayFromSQL(self,sqlcon,table,mgcollection,array):
        mgPath = self.host
        mgPort = str(self.port)
        mgDB = self.database
        mgUser = self.username
        mgPWD = self.pwd
        sqlTable = table
        mgCollection = mgcollection
        self.MongoClear(mgDB,mgCollection,self.conn)
        cursor = sqlcon.cursor()
        column = {}
        i1 = 0
        cursor.execute('select name from syscolumns where id=object_id(N\''+sqlTable+'\')')
        db = self.conn[mgDB]
        collect = db[mgCollection]
        for r in cursor:
            column[i1] = str(r).strip(',').strip(')').strip('(').strip(',').strip('\'')#去除数组分割剩下的垃圾
            i1 = i1+1
        collect.drop_indexes()
        collect.create_index(column[0])
        cursor.execute('select * from '+sqlTable)
        rr = {}


        i2 = 0
        for x in cursor:
            rr[i2] = x
            i2 =i2+1


        for row in range(len(rr)):
            i = 0
            rowDone = {}
            for things in rr[row]:
                rowDone[column[i]] = things
                i = i+1
            rowDone[array['arrayname']] = self.SQLArrayGet(sqlcon,array['table'],array['query'],str(rowDone[array['query']]),array['arrayname'])
            try:
                self.MongoInsert(rowDone,self.conn,mgDB,mgCollection)
            except:
                print('Insert Failed')
                raise



    def SQLArrayGet(self,sqlcon,sqltable,query,key,arrayname):#获取子表中的内容
        cursor = sqlcon.cursor()
        column = {}
        i1 = 0
        cursor.execute('select name from syscolumns where id=object_id(N\''+ sqltable+'\')')
        for r in cursor:
            column[i1] = str(r).strip(',').strip(')').strip('(').strip(',').strip('\'')#去除数组分割剩下的垃圾
            i1 = i1+1
        s='select * from '+sqltable +' where '+ query+' = ' +key+''
        cursor.execute('select * from '+sqltable +' where '+ query+' = '+ key)
        xxxx = []
        if(cursor.rowcount != 0):
            for row in cursor:
                i = 0
                rowDone = {}
                for things in row:
                    rowDone[column[i]] = things
                    i = i+1
                xxxx.append(rowDone)
        return xxxx

    def MongoInsert(self,row,conn,database,collection):
        db = conn[database]
        #print(conn.database_names())
        collection = db[collection]
        account = collection
        account.insert(row)


    def MongoInsertArray(self,row,conn,database,collection,array):
        db = conn[database]
        #print(conn.database_names())
        collection = db[collection]
        account = collection
        account.insert(row)
        #account.insert(row)


    def MongoClear(self,mgDB,mgCollection,conn):
        db = conn[mgDB]
        print(conn.database_names())
        collection = db[mgCollection]
        collection.remove()

fin = open('Config2.json','r')
ConfigString = fin.read()
decodejson = json.loads(ConfigString)
sql = SQLServer(**decodejson['server']['source'])
mg = MongoServer(**decodejson['server']['target'])
s = decodejson['tables'][0]['source']
for i in range(len(decodejson['tables'])):
    if decodejson['tables'][i].get('array') == None:
        mg.TransFromSQL(sql.con,decodejson['tables'][i]['source'],decodejson['tables'][i]['target'])
    else:
        mg.TransArrayFromSQL(sql.con,decodejson['tables'][i]['source'],decodejson['tables'][i]['target'],decodejson['tables'][i]['array'])

