#Name: spider.py
#Desc: Scrapes county jailview and stores data in json format
#Date: 12/12/2016
#Author: Ernest Mallett
from scrapy.spiders import CrawlSpider, Rule
from jailview.items import JailviewItem, ChargeItem, RecordItem
from scrapy import Request
import logging
import string
import json

class JvSpider(CrawlSpider):
    name = "jailview"
    start_urls=["http://inmatelookup.myescambia.com/smartwebclient/jail.aspx",
    "http://portal.srso.net/smartweb/jail.aspx",
    "http://bcsonline.co.baldwin.al.us/smartweb/jail.aspx",
    "http://jail.madisoncountyal.gov/smartwebclient/jail.aspx",
    "http://50.201.159.194/smartwebclient/jail.aspx",
    "http://smartweb.pcso.us/smartwebclient/jail.aspx",
    "http://www2.sjso.org/smartwebclient/jail.aspx",
    "http://173.186.83.202/smartwebclient/jail.aspx",
    "http://portal.sumtercountysheriff.org/smartwebclient/jail.aspx",
    "http://69.21.72.195/smartwebclient/Jail.aspx",
    "http://70.89.76.90/smartweb/jail.aspx",
    "http://www.rc13.mt.gov/smartwebclient/jail.aspx",
    "http://smartweb.bradfordsheriff.org/smartwebclient/Jail.aspx"]
    
    #TODO: Finish load_more function
    def load_more(self):
        if not url:
            url=0
        logging.debug("Running load_more")
        payload = {'FirstName': '', 'MiddleName': '', 'LastName': '', 'BeginBookDate':'','EndBookDate':'','BeginReleaseDate':'','EndReleaseDate':'','TypeJailSearch':2,'RecordsLoaded':10,'SortOption':0,'SortOrder':0,'IsDefault':False}
        requrl = str(start_urls[url])+'/AddMoreResults'
        yield Request(requrl, self.parse_data,method="POST",body=urllib.urlencode(payload))
        
    def parse_data(self, response):
        #data = json.loads(response.body)
        logging.debug(response.body)
        
    #Ensure's data isn't left null and strips out unneeded characters
    def fixdata(self, resp,xpth):
        if resp.xpath(xpth).extract():
            data=resp.xpath(xpth).extract()[0]
            d=data.replace("\t","").replace("\r","").replace("\n","")
            return d
        else:
            return ""
        
    #Ensure's data isn't left null and strips out unneeded characters
    def fixcssdata(self, resp,cssd):
        if resp.css(cssd).extract():
            data=resp.css(cssd).extract()[0]
            d=data.replace("\t","").replace("\r","").replace("\n","")
            return d
        else:
            return ""
        
    def parse(self,response):

        people = response.xpath('//tr[@class="InmateRecordSeperater"]/preceding-sibling::tr[1]')
        pcount = 1
        
        for person in people:
            
            logging.debug("Creating new record")
            record = JailviewItem()
            record['identifier'] = self.fixdata(person,"./td[2]/table/thead/tr/td/text()")
            recorddata = RecordItem()
            #TODO: Ensure that charge is added to both parties when two or more people have the same charge
            recorddata['bookingnum'] = self.fixdata(person,"./td/table/tbody/tr[2]/td[2]/text()")
            recorddata['mninum'] = self.fixdata(person,"./td/table/tbody/tr[2]/td[4]/text()")
            recorddata['ageonbooking'] = self.fixdata(person,"./td/table/tbody/tr[4]/td[2]/text()")
            recorddata['bookdate'] = self.fixdata(person,"./td/table/tbody/tr[3]/td[2]/text()")
            recorddata['bondamount'] = self.fixdata(person,"./td/table/tbody/tr[5]/td[2]/text()")
            recorddata['address'] = self.fixdata(person,"./td/table/tbody/tr[7]/td[2]/text()")
            imageurl = self.fixdata(person,"./td[1]/a/@href")
            imageurl = response.url.replace("jail.aspx","").replace("Jail.aspx","") + imageurl
            recorddata['imageurl'] = imageurl
            
            #Now that we have the record, let's populate it with the charges (should they be listed)
            charges = response.xpath('.//tr[@class="InmateRecordSeperater"]['+str(pcount)+']/following-sibling::tr[1]/td/table/tr[position()>2 and position() mod 2 = 1]')
            chrgarr = []
                      
            logging.debug("Pcount: "+str(pcount))
            logging.debug("Charges count: "+str(len(charges)))
            
            if(len(charges)>0):
                for charge in charges:
                    logging.debug("Creating new charge")
                    chrg = ChargeItem()
                    chrg['statute'] = self.fixdata(charge,"./td[2]/text()")
                    chrg['casenumber'] = self.fixdata(charge,"./td[3]/text()")
                    chrg['charge'] = self.fixdata(charge,"./td[4]/text()")
                    chrg['degree'] = self.fixdata(charge,"./td[5]/text()")
                    chrg['level'] = self.fixdata(charge,"./td[6]/text()")
                    chrg['bond'] = self.fixdata(charge,"./td[7]/text()")
                    chrgarr.append(chrg)
            recorddata['charges']=chrgarr
            record['record'] = recorddata
            logging.debug("Yeilding record")
            pcount=pcount+1
            yield record
        logging.debug("Total count: "+str(pcount))
        
        #logging.debug("Loading more records")
        
        #self.load_more()