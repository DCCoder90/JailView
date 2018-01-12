# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import hashlib
import string

class MySQLPipeline(object):

    collection_name = 'scrapy_items'

    def __init__(self, dbu, dbp, dbh, db, dbport):
        self.dbu = dbu
        self.dbp = dbp
        self.dbh = dbh
        self.db = db
        self.dbport = dbport

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            dbu=crawler.settings.get('DBU'),
            dbp=crawler.settings.get('DBP'),
            dbh=crawler.settings.get('DBH'),
            db=crawler.settings.get('DB'),
            dbport = crawler.settings.get('DBPORT')
        )

    def open_spider(self, spider):
        self.conn = MySQLdb.connect(
            host = self.dbh, 
            user = self.dbu, 
            passwd = self.dbp, 
            db = self.db, 
            port = self.dbport)
        self.c = self.conn.cursor()

    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):  
        
        rr=item['record']
        charges=rr['charges']
        
        dentifiers=self.parse_identifier(item['identifier'])

        bookingnum = rr['bookingnum'].encode('utf-8').strip()
        age = rr['ageonbooking'].encode('utf-8').strip()
        bookdate = rr['bookdate'].encode('utf-8').strip()
        bond=rr['bondamount'].encode('utf-8').strip()
        try:
            address=rr['address'].encode('utf-8').strip()
        except KeyError:
            address=""
        imageurl=rr['imageurl'].encode('utf-8').strip()
        
        pid=self.insert_person(dentifiers['FName'],dentifiers['MName'],dentifiers['LName'],dentifiers['Ethnicity'],dentifiers['Sex'],dentifiers['DOB'],rr['mninum'].encode('utf-8').strip())
        rid=self.insert_record(pid,bookingnum,age,bookdate,bond,address,imageurl)        
        
        if rid: #Only add charges if rid is a number and not False
            for charge in charges:
                statute=charge['statute'].encode('utf-8').strip()
                try:
                    casenum=charge['casenumber'].encode('utf-8').strip()
                except KeyError:
                    casenum=""
                chargee=charge['charge'].encode('utf-8').strip()
                degree=charge['degree'].encode('utf-8').strip()
                level=charge['level'].encode('utf-8').strip()
                bond=charge['bond'].encode('utf-8').strip()
                self.insert_charge(rid,statute,casenum,chargee,degree,level,bond)
        self.conn.commit()
        return item
    
    def parse_identifier(self,identifier):
        if not identifier:
            data={'FName':'','MName':'', 'LName':'','Ethnicity':'','Sex':'','DOB':''}
            return data
            
        name=string.split(identifier,'(',1)
        name=name[0].encode('utf-8').replace("\xc2\xa0","").strip()
        name=string.split(name," ",)
        
        fname=name[1]
        lname=name[0]
        
        try:
            mname=name[2]
        except IndexError:
            mname=""  
        
        try:
            ethnicity=string.split(identifier,'(')
            ethnicity=string.split(ethnicity[1],'/')
            ethnicity=ethnicity[0].encode('utf-8')
        except IndexError:
            ethnicity=""
        
        try:
            sex=string.split(identifier,'/',1)
            sex=string.split(sex[1],'/',1)
            sex=sex[0].encode('utf-8').replace(")","").strip()
        except IndexError:
            sex=""
        try:
            dob=string.split(identifier,'DOB:',1)
            dob=string.split(dob[1],')',0)
            dob=dob[0].encode('utf-8').replace(")","").strip()
        except IndexError:
            dob=""  
        data={'FName':fname,'MName':mname, 'LName':lname,'Ethnicity':ethnicity,'Sex':sex,'DOB':dob}
        return data
    
    def insert_person(self,fname,mname,lname,ethnicity,sex,dob,mni):

        hash=hashlib.sha256(lname+mni).hexdigest()

        self.c.execute("SELECT COUNT(*) FROM people WHERE hash ='"+hash+"'") #Check if person is already here by hash
        count = self.c.fetchone()
        if count[0] < 1:
            self.c.execute('''INSERT INTO people (hash,fname,mname,lname,ethnicity,sex,dob,mninum) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',[hash,fname,mname,lname,ethnicity,sex,dob,mni])
            return self.c.lastrowid
        else: #Person already exists, return their id
            self.c.execute("SELECT id FROM people WHERE hash='"+hash+"'")
            id=self.c.fetchone()
            return id[0]
        return False

    def insert_record(self,id,bookingnum,age,bookdate,bond,address,imageurl):
        hash=hashlib.sha256(bookingnum+bookdate).hexdigest()
      
        self.c.execute("SELECT COUNT(*) FROM records WHERE hash = '"+hash+"'") #check if record already here by hash
        count = self.c.fetchone()
        if count[0] < 1:
            self.c.execute('''INSERT INTO records (individualid,hash,bookingnum,ageonbooking,bookdate,bondamount,address,imageurl) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',[id,hash,bookingnum,age,bookdate,bond,address,imageurl])
        else: #Record already exists return false
            return False
        lastid = self.c.lastrowid
        return lastid

    def insert_charge(self,id,statute,casenum,charge,degree,level,bond):
        self.c.execute('''INSERT INTO charges (recordid,statute,casenum,charge,degree,level,bond) VALUES (%s,%s,%s,%s,%s,%s,%s)''',[id,statute,casenum,charge,degree,level,bond])
        lastid = self.c.lastrowid 
        return lastid
