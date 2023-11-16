import os
import requests
import pandas as pd
import re
from time import sleep
from bs4 import BeautifulSoup as bsoup
from typing import Tuple


startUrl = "https://nrlm.gov.in/shgOuterReports.do?methodName=showShgreport"
startDir = "/home/arpiku/pyScrape/shgMain"
encdPattern = r"'(\d+)'"
srtnmPattern = r"'([a-z]{2})'(?=,)"
shcdPattern = r",'(\d+)',"


def getBaseUrlAndPaths()->Tuple[str,str,str,bool]:
    return (startUrl,startDir,"shgMain",False)


def getStateUrlAndPaths(stateName:str,encd:str)->Tuple[str,str,str,bool]:
    return (f"https://nrlm.gov.in/shgOuterReports.do?methodName=showDistrictPage&encd={encd}&stateName={stateName}" ,f"{startDir}/{stateName}","_".join(f"{stateName}".lower().split(" ")),False)

def getDistrictUrlAndPaths(stateName:str,districtName:str,encd:str)->Tuple[str,str,str,bool]:
    return (f"https://nrlm.gov.in/shgOuterReports.do?methodName=showBlockPage&encd={encd}&stateName={stateName}&districtName={districtName}", f"{startDir}/{stateName}/{districtName}","_".join(f"{stateName}|{districtName}".lower().split(" ")),False)

def getBlockUrlAndPaths(stateName:str,districtName:str,blockName:str,encd:str)->Tuple[str,str,str,bool]:
    return (f"https://nrlm.gov.in/shgOuterReports.do?methodName=showGPPage&encd={encd}&stateName={stateName}&districtName={districtName}&blockName={blockName}",f"{startDir}/{stateName}/{districtName}/{blockName}","_".join(f"{stateName}|{districtName}|{blockName}".lower().split(" ")),False)

def getVillageUrlAndPaths(stateName:str,districtName:str,blockName:str,panchayatName:str,encd:str)->Tuple[str,str,str,bool]:
    return (f"https://nrlm.gov.in/shgOuterReports.do?methodName=showVillagePage&encd={encd}&stateName={stateName}&districtName={districtName}&blockName={blockName}&grampanchayatName={panchayatName}",f"{startDir}/{stateName}/{districtName}/{blockName}/{panchayatName}","_".join(f"{stateName}|{blockName}|{panchayatName}".lower().split(" ")),False)

def getGroupUrlAndPaths(stateName:str,districtName:str,blockName:str,panchayatName:str,villageName:str,srtnm:str,encd:str)->Tuple[str,str,str,bool]:
    return (f"https://nrlm.gov.in/shgOuterReports.do?methodName=showSHGPage&encd={encd}&srtnm={srtnm}&stateName={stateName}&districtName={districtName}&blockName={blockName}&grampanchayatName={panchayatName}&villageName={villageName}",f"{startDir}/{stateName}/{districtName}/{blockName}/{panchayatName}/{villageName}","_".join(f"{stateName}|{blockName}|{panchayatName}|{villageName}".lower().split(" ")),False)

def getFinalGroupUrlAndPaths(stateName:str,districtName:str,blockName:str,panchayatName:str,villageName:str,groupName:str,srtnm:str,encd:str,shgcd:str)->Tuple[str,str,str,bool]:
    return (f"https://nrlm.gov.in/shgOuterReports.do?methodName=showGroupPage&encd={encd}&srtnm={srtnm}&shgcd={shgcd}&stateName={stateName}&districtName={districtName}&blockName={blockName}&grampanchayatName={panchayatName}&villageName={groupName}",f"{startDir}/{stateName}/{districtName}/{blockName}/{panchayatName}/{villageName}/{groupName}","_".join(f"{stateName}|{blockName}|{panchayatName}|{villageName}|{groupName}".lower().split(" ")),False)

def getDataFrameFromSoup(table,level=0)->pd.DataFrame():    
    print(f"INO: Making DataFrame From SOUP")
    """Parses a html segment started with tag <table> followed 
    by multiple <tr> (table rows) and inner <td> (table data) tags. 
    It returns a list of rows with inner columns. 
    Accepts only one <th> (table header/data) in the first row.
    """
    def rowgetDataText(tr, level:int, coltag='td'): # td (data) or th (header)       
        text = [td.get_text(strip=True) for td in tr.find_all(coltag)]
        if level > 5:
            return text
        encd = [re.search(encdPattern,str(td["href"])).group(1) for td in tr.find_all("a")]
        if level == 5:
            srtn = [re.search(srtnmPattern ,str(td["href"])).group(1) for td in tr.find_all("a")]
            shc = [re.search(shcdPattern, str(td["href"])).group(1) for td in tr.find_all("a")]
            return text,encd,srtn,shc
        return text,encd
    rows = []
    encdr = []
    srtnm = []
    shcd= []
    trs = table.find_all('tr')
    #headerow = rowgetDataText(trs[0],level, 'th')

    for tr in trs: # for every table row
        if tr.find_all("a", href=True) or level > 5: #So that the garbage data dosen't get picked
            if level==5:
                r,encd,srtn,shc = rowgetDataText(tr, level,'td')
                rows.append(r) # data row       
                encdr.append(encd[0])
                srtnm.append(srtn[0])
                shcd.append(shc[0])
            elif level < 5:
                r,encd = rowgetDataText(tr, level,'td')
                rows.append(r) # data row       
                encdr.append(encd[0])
            else:
                r = rowgetDataText(tr,level,'td')
                rows.append(r)
    if level > 5:
        return pd.DataFrame(rows)
    if level == 5:
        return pd.DataFrame(rows),encdr,srtnm,shcd
    return pd.DataFrame(rows),encdr

def getTableSoup(url,level=0,retryCount=0):
    print(f"INFO: Making Soup at {level} with {url}")
    if retryCount > 80:
            print(f"Failed")
            return
    response = requests.get(url)
    print(response.status_code)
    if response.status_code == 200:
        soup = bsoup(response.text, 'html.parser')
      
    if level == 0:
        table = soup.find("table", {"id": "mainex"})
    else:
        table = soup.find("table", {"id": "example"})
    if table:
        return table
    else:
        sleep(1)
        getTableSoup(url,level,retryCount+1)



def makeDir(dirPath:str)->None:
    if not os.path.exists(dirPath):
        os.makedirs(dirPath)


if __name__ == "__main__":
    ssUrl = "https://nrlm.gov.in/shgOuterReports.do?methodName=showSHGPage&encd=0215001022003&srtnm=ap&stateName=ANDHRA%20PRADESH&districtName=ALLURI%20SITHARAMA%20RAJU&blockName=ADDATEEGALA&grampanchayatName=ADDATEEGALA&villageName=ADDATEEGALA"

    a0 = "https://nrlm.gov.in/shgOuterReports.do?methodName=showShgreport"
    a1 = "https://nrlm.gov.in/shgOuterReports.do?methodName=showDistrictPage&encd=33&stateName=CHHATTISGARH"
    a2 = "https://nrlm.gov.in/shgOuterReports.do?methodName=showBlockPage&encd=3321&stateName=CHHATTISGARH&districtName=BALOD"
    a3 = "https://nrlm.gov.in/shgOuterReports.do?methodName=showGPPage&encd=3321004&stateName=CHHATTISGARH&districtName=BALOD&blockName=BALOD"
    a4 = "https://nrlm.gov.in/shgOuterReports.do?methodName=showVillagePage&encd=3321004001&stateName=CHHATTISGARH&districtName=BALOD&blockName=BALOD&grampanchayatName=AMORA&reqtrack=79aPF4sJqddlo6t1008qrk5Hx"
    a5 = "https://nrlm.gov.in/shgOuterReports.do?methodName=showSHGPage&encd=3321004001001&srtnm=cg&stateName=CHHATTISGARH&districtName=BALOD&blockName=BALOD&grampanchayatName=AMORA&villageName=AMORA"
    a6 = "https://nrlm.gov.in/shgOuterReports.do?methodName=showGroupPage&encd=3321004001001&srtnm=cg&shgcd=228420&stateName=CHHATTISGARH&districtName=BALOD&blockName=BALOD&grampanchayatName=AMORA&villageName=AMORA&groupName=BHARAT%20MATA%20VAHINI%20SHG&reqtrack=KqJPjK9g5oNSzsYIxDG22cveY"

    #print(getStateUrlAndPaths("a"))
    #print(getDistrictUrlAndPaths("a","b"))0
    #print(getBlockUrlAndPaths("a","b","c"))
    #print(getVillageUrlAndPaths("a","b","c","d"))
    #print(getGroupUrlAndPaths ("a","b","c","d","e"))
    #table = getTableSoup(a5,5) 
    #print(table)
    #df,r,y,z = getDataFrameFromSoup(table,5)
    #print(df.head())
    #print(r)
    #print(y)
    #print(z)
    #print(df.head())
    tt = pd.DataFrame()
    tt.to_csv("/Users/arpitkumar/pyScrape/shgMain/ANDHRA PRADESH/ADDATEEGALA/ADDATEEGALA/ADDATEEGALA/ss.csv")
    makeDir("/Users/arpitkumar/pyScrape/shgMain/ANDHRA PRADESH/ADDATEEGALA/ADDATEEGALA/ADDATEEGALA")
