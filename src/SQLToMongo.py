# -*-coding: utf-8 -*-
import pymongo
import pymssql
import sys
import json




class SQLServer:
    sqlPath =''
    sqlPort =0
    sqlUser = ''
    sqlPWD = ''
    sqlDB = ''
    con = None
    #构造函数创建于SQL连接
    def __init__(self,host,port,username,pwd,database,provider):
        if(port == 0):
            self.sqlPath = host
            self.sqlPort = port
            self.sqlUser = username
            self.sqlPWD = pwd
            self.sqlDB = database
            self.con = pymssql.connect(server=self.sqlPath, user=self.sqlUser, password=self.sqlPWD, database=self.sqlDB)
            print('SQLConnected Successfully')
        else:
            self.sqlPath = host
            self.sqlPort = port
            self.sqlUser = username
            self.sqlPWD = pwd
            self.sqlDB = database
            self.con = pymssql.connect(server=self.sqlPath,port = self.sqlPort,user=self.sqlUser, password=self.sqlPWD, database=self.sqlDB)
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
                things = self.NumberToFloat(things)
                rowDone[column[i]] = things
                i = i+1
            try:
                self.MongoInsert(rowDone,self.conn,mgDB,mgCollection)
            except:
                print('Insert Failed')
                raise

    def NumberToFloat(self,number):
        print(str(type(number)))
        if(str(type(number))=='<class \'decimal.Decimal\'>'):
            number = float(number)
            return number
        return number

    def TransArrayFromSQL(self,sqlcon,table,mgcollection,array):#
        mgPath = self.host
        mgPort = str(self.port)
        mgDB = self.database
        mgUser = self.username
        mgPWD = self.pwd
        sqlTable = table
        mgCollection = mgcollection
        self.MongoClear(mgDB,mgCollection,self.conn)#Clear the MongoDB Collection
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
            rowDone = self.getRowDone(sqlcon,array,rowDone)
            try:
                self.MongoInsert(rowDone,self.conn,mgDB,mgCollection)
            except:
                print('Insert Failed')
                raise


    def TransFromSQLwithMapping(self,sqlcon,table,mgcollection,array,mapping):
        mgPath = self.host
        mgPort = str(self.port)
        mgDB = self.database
        mgUser = self.username
        mgPWD = self.pwd
        sqlTable = table
        mgCollection = mgcollection
        mgArray = array
        mgMapping = mapping
        self.MongoClear(mgDB,mgCollection,self.conn)#Clear the MongoDB Collection
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
            rowDone = self.getRowDone(sqlcon,mgArray,rowDone)
            try:
                rowDone = self.MappingRow(rowDone,mgMapping)
                self.MongoInsert(rowDone,self.conn,mgDB,mgCollection)
            except:
                print('Insert Failed')
                raise


    def MappingRow(self,row,mapping):
        rowMapped = {}
        if(mapping.get('mapping')):
            for i in mapping['mapping']:
                rowMapped[i['target']] = row[i['name']]
        if(dict(mapping).get('array')):
            for i1 in range(len(mapping['array'])):
                if(row[mapping['array'][i1]['arrayname']] != []):
                    for i2 in range(len(row[mapping['array'][i1]['arrayname']])):
                        rowMapped[mapping['array'][i1]['arrayname']] = self.MappingRow(row[mapping['array'][i1]['arrayname']][i2],mapping['array'][i1])
        return rowMapped


    def getRowDone(self,sqlcon,array,rowDone):#Get the whole row,make row to rowdone
        for n in range(len(array)):
            rowDone[array[n]['arrayname']] = self.SQLArrayGet(sqlcon,array[n]['table'],array[n]['query'],str(rowDone[array[n]['query']]),array[n]['arrayname'])
            if( len(rowDone[array[n]['arrayname']])>=0):
                for bb in range(len(rowDone[array[n]['arrayname']])):
                    if(dict(array[n]).get('array')):
                        rowDone[array[n]['arrayname']][bb] = self.getRowDone(sqlcon,array[n]["array"],rowDone[array[n]['arrayname']][bb])
            return rowDone




    def SQLArrayGet(self,sqlcon,sqltable,query,key,arrayname):#Get the children table in SQL
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



    def MongoInsert(self,row,conn,database,collection):#Insert one row
        db = conn[database]
        collection = db[collection]
        account = collection
        account.insert(row)


    def MongoClear(self,mgDB,mgCollection,conn):#Remove each row in this mgCollection
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
        mg.TransFromSQLwithMapping(sql.con,decodejson['tables'][i]['source'],decodejson['tables'][i]['target'],decodejson['tables'][i]['array'],decodejson['tables'][i])


