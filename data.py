
# coding: utf-8

# In[1]:

#these are just imports
try:
    import cPickle as pickle
except:
    import pickle

import numpy
import pandas as pd
import os, sys


# In[2]:

from tacc_stats.analysis.plot.plots import Plot
class NewDevPlot(Plot):
    '''Altered DevPlot object'''
    
    def __init__(self, path, data, type_name, processes=1):
        '''
        :param path [string]: full path to pickle file
        :param data [pickle object]: unloaded piuckle data
        :param type_name [string]: io parameter of interest
        :processes [int]: default process value (do not change)
        :return x_val [array]: time vector in hours
        :return y_val [array]: io data 
        '''
        #Unload pickle data
        self.data = data
        self.path = path
        #Get schema information
        schema = self._build_schema(self.data,type_name)
        schema = [x.split(',')[0] for x in schema]
        #k1 will have a list of type_names of schema length for all keys
        self.k1 = {'intel_snb' : [type_name]*len(schema),
                  'intel_hsw' : [type_name]*len(schema),
                  'intel_pmc3' : [type_name]*len(schema)
                  }
        #k2 will have list generated with build_schema for all keys
        self.k2 = {'intel_snb': schema,
                  'intel_hsw': schema,
                  'intel_pmc3': schema
                  }
        #Initialize object
        super(NewDevPlot,self).__init__(data=data, type_name=type_name,processes=1)

    def get_data_dict(self):
        '''
        Get plot data in a dictionary
        :return value_dict [dict]: dictionary of job data
        '''
        self.setup(self.path,job_data=self.data)
        cpu_name = self.ts.pmc_type
        type_name=self.k1[cpu_name][0]
        events = self.k2[cpu_name]
        ts=self.ts
        n_events = len(events)
        do_rate = True
        scale = 1.0
        if type_name == 'mem': 
            do_rate = False
            scale=2.0**10
        if type_name == 'cpu':
            scale=ts.wayness*100.0
        meta_dict = {}
        for i in range(n_events):
            meta_dict[events[i]] = self._get_data([i],yscale=scale,do_rate=do_rate)
        #Get attritubes of type_name
        attr = meta_dict.keys()
        #Get unique host names
        hosts = []
        for a in attr:
            for key in meta_dict[a].keys():
                hosts.append(key)
        hosts = list(set(hosts))
        #Reorganize meta_dict to value dict
        value_dict = {}
        for host in hosts:
            value_dict[host] = {}
            for a in attr:
                value_dict[host][a] = meta_dict[a][host]
        return value_dict
    
    def _get_data(self,index,yscale,do_rate=True):
        '''Altered Plot.plot_line function'''
        xscale=3600.
        main_dict = {}
        
        for k in self.ts.j.hosts.keys():
            v=self.ts.assemble(index,k,0)
            if do_rate:
                val=numpy.divide(numpy.diff(v),numpy.diff(self.ts.t))
            else:
                val=(v[:-1]+v[1:])/(2.0)
            x_val = self.ts.t/xscale
            y_val = numpy.append(val,[val[-1]])/yscale
            main_dict[k]=[x_val, y_val]
        return main_dict
            
    def _build_schema(self, data,type_name):
        '''
        Get schema values for calculation
        :param data [unpickled object]: job data
        :param type_name [string]: io parameter of interest
        :return schema [object]: job schema
        '''
        schema = []
        for key,value in data.get_schema(type_name).iteritems():
            if value.unit:
                schema.append(value.key + ','+value.unit)
            else: schema.append(value.key)
        return schema
    
    def plot(self, jobid,job_data=None):
        '''Abstract method wrappper for PlotData that needs to be altered'''
        pass


# In[3]:

def io_info(path, data):
    '''
    :param path [string]: path to pickle
    :param data [unpickled object]: unloaded pickle data 
    '''
    
    io_param = {
        'ib_sw': ['rx_bytes','rx_packets','tx_bytes','tx_packets'],
        'nfs':['vfs_open','normal_read','normal_write','vfs_writepage','vfs_readpage'],
        'block':['rd_ios','rd_merges','rd_sectors','wr_ios','wr_merges','wr_sectors'],
        'llite':['read_bytes','write_bytes','open','close']
    }

    combined_dict = {}
    
    for io in io_param.keys():
        devplot = NewDevPlot(path, data,io)
        data_dict = devplot.get_data_dict()
        hosts = data_dict.keys()
        for host in hosts:
            #Adding attributes
            attr = io_param[io]
            for a in attr:
                try:
                    combined_dict[io+':'+a].append(data_dict[host][a][1])
                except:
                    combined_dict[io+':'+a] = []
                    combined_dict[io+':'+a].append(data_dict[host][a][1])
    for host in hosts:
        #Adding time
        try:
            combined_dict['Time'].append(data_dict[host][a][0])
        except:
            combined_dict['Time'] = []
            combined_dict['Time'].append(data_dict[host][a][0])
    return hosts, combined_dict


# In[5]:

def main(path):
    #first we need to get the number of jobs and the job ids for the data frame, use OS.walk to traverse pickle file directory
    pickle_list = []
    job_id = []
    date = []
    path = path[0]
    #path = "/oasis/projects/nsf/ddp260/tcooper/xsede_stats/comet_pickles/"+date
    #for root, dirs, files in os.walk("/Users/cheriehuang/Desktop/Dates", topdown=False):
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            if name == '.DS_Store':
                continue
            pickle_list.append(os.path.join(root, name))
            templist = root.split('/')
            date.append(templist[8]) #can change this number based on the path or what not
            job_id.append(name)
    print len(job_id)
    print len(pickle_list)
    print len(job_id)

    columns = ['UserID', 'Project','WaitTime', 'RunTime', 'Status', 'Nodes', 'Cores', 'SwitchLevel', 'FileSize', 'Date', 'FilePath', 'Queue', 'Notes','nfs:vfs_readpage', 'llite:open', 'block:wr_merges', 'block:rd_merges', 'nfs:vfs_writepage', 'block:rd_ios', 'ib_sw:tx_bytes', 'ib_sw:rx_packets', 'ib_sw:tx_packets', 'nfs:normal_write', 'nfs:normal_read', 'block:wr_sectors', 'nfs:vfs_open', 'block:wr_ios', 'ib_sw:rx_bytes', 'block:rd_sectors', 'llite:read_bytes', 'llite:write_bytes', 'llite:close', 'Time', 'Hosts']


    # In[8]:

    df_ = pd.DataFrame(index=job_id, columns=columns)


    # In[9]:

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


    # In[10]:

    #now to get the epoch times converted, need to import this module
    from tacc_stats.analysis.gen import tspl
    errlist = []
    errlist.append('1')
    #now let's try to fill in the columns and rows
    idxlist = range(0, len(pickle_list))
    for filename,jid,i,d in zip(pickle_list, job_id, idxlist, date):
        try:
            the_file = open(filename, 'rb')
            data = pickle.load(the_file)
        except:
            continue#now we need to get the specific info from data.acct
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
        #now we need to get all the time series data - as a dictionary
        try:
            hosts, combined_dict = io_info(jid, data)
        except:
            errlist.append(jid) #tells us which job ids had issues
            continue#13-32
        #hardcoding for now
        df_.ix[i,13] = combined_dict['nfs:vfs_readpage']
        df_.ix[i,14] = combined_dict['llite:open']
        df_.ix[i,15] = combined_dict['block:wr_merges']
        df_.ix[i,16] = combined_dict['block:rd_merges']
        df_.ix[i,17] = combined_dict['nfs:vfs_writepage']
        df_.ix[i,18] = combined_dict['block:rd_ios']
        df_.ix[i,19] = combined_dict['ib_sw:tx_bytes']
        df_.ix[i,20] = combined_dict['ib_sw:rx_packets']
        df_.ix[i,21] = combined_dict['ib_sw:tx_packets']
        df_.ix[i,22] = combined_dict['nfs:normal_write']
        df_.ix[i,23] = combined_dict['nfs:normal_read']
        df_.ix[i,24] = combined_dict['block:wr_sectors']
        df_.ix[i,25] = combined_dict['nfs:vfs_open']
        df_.ix[i,26] = combined_dict['block:wr_ios']
        df_.ix[i,27] = combined_dict['ib_sw:rx_bytes']
        df_.ix[i,28] = combined_dict['block:rd_sectors']
        df_.ix[i,29] = combined_dict['llite:read_bytes']
        df_.ix[i,30] = combined_dict['llite:write_bytes']
        df_.ix[i,31] = combined_dict['llite:close']
        df_.ix[i,32] = combined_dict['Time']
        df_.ix[i,33] = hosts
    print len(errlist)
    # In[11]:
    print len(df_)

    df_ = df_.dropna(how = 'all') #only drops if all columns in that row are NaN
    df_ = df_[df_.FileSize != 0]


    # In[12]:
    print len(df_)
    loc = path.split('/')[-1]
    df_.to_pickle('/home/ssnazrul/Engility/Pickles/'+loc+'.pkl')

if __name__=='__main__':
    main(sys.argv[1:])



