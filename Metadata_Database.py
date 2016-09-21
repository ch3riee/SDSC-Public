
# coding: utf-8

# This is the code that grabs from the pickle files the information to form the pandas dataframe database that just holds metadata about each job. Also stores switch level data as well as number of hosts etc.

# Possible things to consider:
# 1) How to merge different databases if each database is based on day?
# 2) What other data might be useful to store in the database (metadata)?
# 3) Should we just have one huge database that we update each day? How should they be organized?
# 4) For now lets make one giant database that will point to each days or something.

# In[1]:

#these are just imports
try:
    import cPickle as pickle
except:
    import pickle


import numpy as np
import pandas as pd
from pandas import HDFStore


# In[2]:

#first we need to get the number of jobs and the job ids for the data frame, use OS.walk to traverse pickle file directory
import os
pickle_list = []
job_id = []
date = []
for root, dirs, files in os.walk("/Users/cheriehuang/Desktop/2016-06-23", topdown=False):
#for root, dirs, files in os.walk("/oasis/projects/nsf/ddp260/tcooper/xsede_stats/comet_pickles", topdown=False):
    for name in files:
        if name == '.DS_Store':
            continue
        pickle_list.append(os.path.join(root, name))
        templist = root.split('/')
        date.append(templist[4]) #can change this number based on the path or what not
        job_id.append(name)
#print job_id
#print pickle_list
#print date

    


# In[3]:

#first we want our code to create a h5 file we will be writing to
#all of our pickle files will essentially be stored onto here
#db = HDFStore('/oasis/projects/nsf/ddp260/cherieh/' + date[0] + '.h5')
db = HDFStore('/Users/cheriehuang/Desktop/' + date[0] + '.h5')


# In[4]:

#let's make the empty pandas dataframe first
columns = ['UserID', 'Project','WaitTime', 'RunTime', 'Status', 'Nodes', 'Cores', 'SwitchLevel', 'FileSize', 'Date', 'FilePath', 'Queue', 'Notes']
#the number of columns is based on how many pickle files there are. Maybe we can just do job ids? 


# In[5]:

df_ = pd.DataFrame(index=job_id, columns=columns)
#print df_
print df_[]


# In[6]:

#making our switch level dict first
sdict = {}
for i in range(1,73):
    if i <= 18:
        sdict[i] = 1
    elif (i > 18) and (i <= 36):
        sdict[i] = 2
    elif (i > 36) and (i <= 54):
        sdict[i] = 3
    elif (i > 54) and (i <= 72):
        sdict[i] = 4


# In[8]:

#now to get the epoch times converted, need to import this module
from tacc_stats.analysis.gen import tspl

#now let's try to fill in the columns and rows
idxlist = range(0, len(pickle_list))
for filename,jid,i,d in zip(pickle_list, job_id, idxlist, date):
    try:
        the_file = open(filename, 'rb')
        data = pickle.load(the_file)
    except:
        continue
    #now we need to get the specific info from data.acct
    theDict = data.acct
    if ((theDict['queue'] != 'compute') and (theDict['queue'] != 'gpu')):
        continue #will skip to the next pickle file
    df_.ix[i, 0] = theDict['uid']
    df_.ix[i, 1] = theDict['project']
    df_.ix[i, 4] = theDict['status']
    df_.ix[i, 5] = theDict['nodes']
    df_.ix[i, 6] = theDict['cores']
    
    df_.ix[i, 11] = theDict['queue']
    #the file name fill in ones
    df_.ix[i, 8] = os.path.getsize(filename)
    df_.ix[i, 10] = filename
    
    #now to convert the epoch times and then save it into the dataframe
    #epoch time = current time looking at minus original start time / 3600
    #start time - queue time should be the wait time
    #df_.ix[i, 2] = theDict['queue_time']
    #df_.ix[i, 3] = theDict['']
    #run time = (end time - start time) / 3600 which gives hours
    s = theDict['start_time']
    e = theDict['end_time']
    df_.ix[i,3] = (e - s)
    #the wait time is (queue time - start time)
    q = theDict['queue_time']
    df_.ix[i, 2] = (s - q)
    
    #now lets get the date
    #'/opt/xsede_stats/comet_pickles/2016-05-19/2785964' this is an example path
    df_.ix[i, 9] = d
    #first lets get the switch level data by looking at each host
    theHosts = data.hosts.keys()
    if len(theHosts) == 1:
        #if just one host then just l1 switch
        df_.ix[i, 7] = 1
    else: 
        for x in range(len(theHosts) - 1):
            #each host name is a string
            host = theHosts[x]
            host2 = theHosts[x + 1]
            rack_num = host[6:8]
            node_num = host[9:]
            rack_num2 = host2[6:8]
            node_num2 = host2[9:]
            if rack_num != rack_num2:
                #it is an l3 switch
                df_.ix[i,7] = 3
                break
            elif (rack_num == rack_num2):
                #so same rack, now need to check if l1 or l2
                if(sdict[int(node_num)] != sdict[int(node_num2)]):
                    if(2 > df_.ix[i,7] or (df_.ix[i,7] != df_.ix[i,7])):
                        df_.ix[i,7] = 2
                elif(sdict[int(node_num)] == sdict[int(node_num2)]):
                    if (1 > df_.ix[i,7] or (df_.ix[i,7] != df_.ix[i,7])):
                        df_.ix[i,7] = 1  

#print df_


# In[17]:

df_ = df_.dropna(how = 'all') #only drops if all columns in that row are NaN
#now drop all rows leftover that have file size of 0 (how could this happen)

df_ = df_[df_.FileSize != 0]

#now we want to save it into our h5 database file
db['theDataset'] = df_

db.close()



