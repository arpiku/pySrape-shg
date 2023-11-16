import pandas as pd
import json
import os
import itertools
import pickle


from typing import List
from utils import *
from os.path import exists

queue=[]
#queue.append(getGroupUrlAndPaths("ANDHRA PRADESH","VISAKHAPATANAM","ADDATEEGALA","ADDATEEGALA","ADDATEEGALA",'ap', '0215001022003'))

history = {}
urlEncdr = {}
LEVEL = 0
def getQ():
    try:
        with open('queue', 'r') as fp:
            queue = pickle.load(fp)
        return queue
    except:
        print("Doesn't exists")
        queue.append(getBaseUrlAndPaths())

        return queue
def setQ(que)->None:
    with open('queue', 'w') as fp:
        pickle.dumps(que,fp)
    return

def getHistory()->dict:
    try:
        with open('history.json', 'r') as fp:
            history = json.load(fp)
        return history
    except:
        print("Doesn't exists")
        return {}
def setHistory(history:dict)->None:
    with open('history.json', 'w') as fp:
        json.dump(history,fp)
    return
def getEncdr()->dict:
    try:
        with open('encdr.json', 'r') as fp:
            history = json.load(fp)
        return history
    except:
        print("Doesn't exists")
        return {}
def setEncdr(encdr:dict)->None:
    with open('encdr.json', 'w') as fp:
        json.dump(history,fp)
    return


def storeCSV(df:pd.DataFrame,path:str,name:str)->None:
    makeDir(path)
    df.to_csv(path + f"/{name}.csv") 


def generateLinks(df:pd.DataFrame,level:int,path:str,encdr=[],srtnm=[],shcd=[]):
    if level == 0:
        states = df[1]
        params = list(zip(states,encdr))
        return map(lambda x:getStateUrlAndPaths(*x),params)
    if level == 1:
        districts=list(df[1])
        state = path.split("/")[-1]
        params = list(zip(itertools.repeat(state),districts,encdr))
        return map(lambda x:getDistrictUrlAndPaths(*x),params)
    if level == 2:
        blocks=list(df[1])
        splits=path.split("/")
        district=splits[-1]
        state=splits[-2]
        params = list(zip(itertools.repeat(state),itertools.repeat(district),blocks,encdr))
        return map(lambda x:getBlockUrlAndPaths(*x),params)
    if level == 3:
        panchayats=list(df[1])
        splits=path.split("/")
        block=splits[-1]
        district=splits[-2]
        state=splits[-3]
        params = list(zip(itertools.repeat(state),itertools.repeat(district),itertools.repeat(block),panchayats,encdr))
        return map(lambda x:getVillageUrlAndPaths(*x),params)
    if level == 4:
        villages = list(df[1])
        splits=path.split("/")
        panchayat=splits[-1]       
        block=splits[-2]
        district=splits[-3]
        state=splits[-4]
        params = list(zip(itertools.repeat(state),itertools.repeat(district),itertools.repeat(block),itertools.repeat(panchayat),villages,srtnm,encdr))
        return map(lambda x:getGroupUrlAndPaths(*x),params)
    if level == 5:
        groups = list(df[1])
        splits=path.split("/")
        village=splits[-1]
        panchayat=splits[-2]       
        block=splits[-3]
        district=splits[-4]
        state=splits[-5]
        params = list(zip(itertools.repeat(state),itertools.repeat(district),itertools.repeat(block),itertools.repeat(panchayat),itertools.repeat(village),groups,srtnm, encdr,shcd))
        return map(lambda x:getFinalGroupUrlAndPaths(*x),params)





def scrape(level:int = 0):
    while queue:
        size_t = len(queue)
        print(size_t,level,"\n\n")
        for i in range(size_t):
            url,path,name,_ = queue.pop(0)
            if url in history and url in urlEncdr:
                print(f"Already visited")
                df = pd.read_csv(path + f"/{name}.csv")
                encdr = urlEncdr[url]
            else:
                table = getTableSoup(url,level)
                df,encdr = getDataFrameFromSoup(table)

                storeCSV(df,path,name)
                history[url]=True
                setHistory(history)
                setEncdr(encdr)
            for urlAndPath in generateLinks(df,level,path,encdr):
                print(urlAndPath)
                queue.append(urlAndPath)
        level = level + 1 
def scrape2(level:int = 0):
    if exists('queue'):
        queue = getQ()
    while queue:
        size_t = len(queue)
        print(size_t,level,"\n\n")
        for i in range(size_t):
            print(len(queue), level)
            url,path,name,_ = queue.pop(0)
            if url in history and url in urlEncdr:
                print(f"Already visited")
                df = pd.read_csv(path + f"/{name}.csv")
                encdr = urlEncdr[url]
            else:
                table = getTableSoup(url,level)
                if not table:
                    table = getTableSoup(url,level)
                if level == 5:
                    df, encdr,srtnm, shcd = getDataFrameFromSoup(table,level)
                elif level < 5:
                    df,encdr = getDataFrameFromSoup(table,level)
                        
                else:
                    df = getDataFrameFromSoup(table,level)
                storeCSV(df,path,name)
                history[url]=True
            if level == 5:
                for urlAndPath in generateLinks(df,level,path,encdr,srtnm,shcd):
                    queue.append(urlAndPath)
            elif level < 5:
                for urlAndPath in generateLinks(df,level,path,encdr):
                    queue.append(urlAndPath)
        level = level + 1 
        LEVEL = level
        print(f"Finished")




if __name__ == "__main__":
    history= getHistory()
    urlEncdr = getEncdr()
    try:
        scrape2(0)
    except:
        sleep(1)
        setQ(queue) 
        scrape2(LEVEL)
    setEncdr(urlEncdr)
    setHistory(history)

