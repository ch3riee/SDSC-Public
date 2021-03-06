
# coding: utf-8

# In[1]:

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter
import calendar as cal
import datetime as dt
import numpy as np
import os, shutil, gc, sys
from sklearn.cluster import DBSCAN as sklearnDBSCAN
from scipy.spatial.distance import cdist, pdist
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler 


# In[2]:

m=500
tol_kmeans = 0.005
sklearn = True

if sklearn:
    m='kmeans'
    
try:
    df = pd.read_pickle('mini_merged.pkl')
except IOError:
    df = pd.read_pickle('clean_merged.pkl')
    
df = df[df['Notes'].isnull()]
df.UserID = df.UserID.astype('int')
#df = df.sample(frac=0.47)
#df.head()


# In[3]:

def fixer(ts, threshold):
    theArray = np.asarray(ts)
    for a in theArray:
        #filter negatives
        np.place(a, a < 0, 0.0)
        #replace all values greater than treshold with nan
        np.place(a, a > threshold, np.nan)
        #calculate average (without nan)
        the_avg = np.nanmean(a)
        #0 if average is nan (for jobs with all bad data.. like less than 10 min with spikes)
        if the_avg == np.nan:
            the_avg = 0.0
        #replace nan with average
        np.place(a, a != a, the_avg)
    return theArray


# In[4]:

io_columns = [
#         'nfs:normal_read', 'nfs:normal_write',
        'block:rd_sectors','block:wr_sectors', 
        'llite:read_bytes', 'llite:write_bytes'
        ]

import warnings
warnings.filterwarnings('ignore')

ndf = df[df['Notes'].isnull()]
ndf['block:rd_sectors'] = ndf['block:rd_sectors'].apply(lambda x: [i*512 for i in x])
ndf['block:wr_sectors'] = ndf['block:wr_sectors'].apply(lambda x: [i*512 for i in x])
# ndf['nfs:vfs_readpage'] = ndf['nfs:vfs_readpage'].apply(lambda x: fixer([x],5e10)[0])
# ndf['nfs:vfs_writepage'] = ndf['nfs:vfs_writepage'].apply(lambda x: fixer([x],5e10)[0])

for ts in io_columns:
    ndf[ts+'_agg'] = ndf[ts].apply(lambda x: np.sum(x))
    
#ndf['SU'] = ndf['RunTime'] * ndf['Nodes'] / 3600

ndf['JobID'] = ndf.index


# In[5]:

columns = ndf.columns
col_agg = [i for i in columns if '_agg' in i]
cluster_col = col_agg + ['JobID', 'UserID','Time']
df2 = ndf[cluster_col]


# ###Custom DBSCAN

# In[6]:

import gc
import numpy as np
from sklearn.metrics.pairwise import pairwise_distances
from sklearn.neighbors import kneighbors_graph 
from sklearn.utils import check_random_state
from tempfile import NamedTemporaryFile
import tables
import time
import warnings

np.seterr(invalid = 'ignore')
warnings.filterwarnings('ignore', category = DeprecationWarning)


__all__ = ['DBSCAN', 'load', 'shoot']


def memory():
    """Determine the machine's memory specifications.
    Returns
    -------
    mem_info : dictonary
        Holds the current values for the total, free and used memory of the system.
    """

    mem_info = {}

    with open('/proc/meminfo') as file:
        c = 0
        for line in file:
            lst = line.split()
            if str(lst[0]) == 'MemTotal:':
                mem_info['total'] = int(lst[1])
            elif str(lst[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                c += int(lst[1])
        mem_info['free'] = c
        mem_info['used'] = (mem_info['total']) - c

    return mem_info


def get_chunk_size(N, n):
    """Given a dimension of size 'N', determine the number of rows or columns 
       that can fit into memory.
    Parameters
    ----------
    N : int
        The size of one of the dimension of a two-dimensional array.  
    n : int
        The number of times an 'N' by 'chunks_size' array can fit in memory.
    Returns
    -------
    chunks_size : int
        The size of a dimension orthogonal to the dimension of size 'N'. 
    """

    mem_free = memory()['free']
    if mem_free > 60000000:
        chunks_size = int(((mem_free - 10000000) * 1000) / (4 * n * N))
        return chunks_size
    elif mem_free > 40000000:
        chunks_size = int(((mem_free - 7000000) * 1000) / (4 * n * N))
        return chunks_size
    elif mem_free > 14000000:
        chunks_size = int(((mem_free - 2000000) * 1000) / (4 * n * N))
        return chunks_size
    elif mem_free > 8000000:
        chunks_size = int(((mem_free - 1400000) * 1000) / (4 * n * N))
        return chunks_size
    elif mem_free > 2000000:
        chunks_size = int(((mem_free - 900000) * 1000) / (4 * n * N))
        return chunks_size
    elif mem_free > 1000000:
        chunks_size = int(((mem_free - 400000) * 1000) / (4 * n * N))
        return chunks_size
    else:
        raise MemoryError("\nERROR: DBSCAN_multiplex @ get_chunk_size:\n"
                          "this machine does not have enough free memory "
                          "to perform the remaining computations.\n")


def load(hdf5_file_name, data, minPts, eps = None, quantile = 50, subsamples_matrix = None, samples_weights = None, 
metric = 'minkowski', p = 2, verbose = True):
    """Determines the radius 'eps' for DBSCAN clustering of 'data' in an adaptive, data-dependent way.
    Parameters
    ----------
    hdf5_file_name : file object or string
        The handle or name of an HDF5 data structure where any array needed for DBSCAN
        and too large to fit into memory is to be stored.
    data : array of shape (n_samples, n_features)
        An array of features retained from the data-set to be analysed. 
        Subsamples of this curated data-set can also be analysed by a call to DBSCAN by providing an appropriate 
        list of selected samples labels, stored in 'subsamples_matrix' (see below).
    subsamples_matrix : array of shape (n_runs, n_subsamples), optional (default = None)
        Each row of this matrix contains a set of indices identifying the samples selected from the whole data-set
        for each of 'n_runs' independent rounds of DBSCAN clusterings.
    minPts : int
        The number of points within an epsilon-radius hypershpere for the said region to qualify as dense.
    eps : float, optional (default = None)
        Sets the maximum distance separating two data-points for those data-points to be considered 
        as part of the same neighborhood.
    quantile : int, optional (default = 50)
        If 'eps' is not provided by the user, it will be determined as the 'quantile' of the distribution 
        of the k-nearest distances to each sample, with k set to 'minPts'.
    samples_weights : array of shape (n_runs, n_samples), optional (default = None)
        Holds the weights of each sample. A sample with weight greater than 'minPts' is guaranteed to be
        a core sample; a sample with negative weight tends to prevent its 'eps'-neighbors from being core. 
        Weights are absolute and default to 1.
    metric : string or callable, optional (default = 'euclidean')
        The metric to use for computing the pairwise distances between samples 
        (each sample corresponds to a row in 'data'). If metric is a string or callable, it must be compatible 
        with metrics.pairwise.pairwise_distances.
    p : float, optional (default = 2)
        If a Minkowski metric is used, 'p' determines its power.
    verbose : Boolean, optional (default = True)
        Whether to display messages reporting the status of the computations and the time it took 
        to complete each major stage of the algorithm. 
    Returns
    -------
    eps : float
        The parameter of DBSCAN clustering specifying if points are density-reachable. 
        This is either a copy of the value provided at input or, if the user did not specify a value of 'eps' at input, 
        the return value if the one determined from k-distance graphs from the data-set.
    References
    ----------
    Ester, M., H. P. Kriegel, J. Sander and X. Xu, "A Density-Based
    Algorithm for Discovering Clusters in Large Spatial Databases with Noise".
    In: Proceedings of the 2nd International Conference on Knowledge Discovery
    and Data Mining, Portland, OR, AAAI Press, pp. 226-231. 1996
    """
    
    data = np.array(data, copy = False)
    if data.ndim > 2:
        raise ValueError("\nERROR: DBSCAN_multiplex @ load:\n" 
                         "the data array is of dimension %d. Please provide a two-dimensional "
                         "array instead.\n" % data.ndim)

    if subsamples_matrix is None:
        subsamples_matrix = np.arange(data.shape[0], dtype = int)
        subsamples_matrix = subsamples_matrix.reshape(1, -1)
 
    else:
        subsamples_matrix = np.array(subsamples_matrix, copy = False)

    if subsamples_matrix.ndim > 2:
        raise ValueError("\nERROR: DBSCAN_multiplex @ load:\n"
                         "the array of subsampled indices is of dimension %d. "
                         "Please provide a two-dimensional array instead.\n" % subsamples_matrix.ndim)

    if (data.dtype.char in np.typecodes['AllFloat'] and not np.isfinite(data.sum()) and not np.all(np.isfinite(data))):
        raise ValueError('\nERROR: DBSCAN_multiplex @ load:\n'
                         'the data vector contains at least one infinite or NaN entry.\n')

    if (subsamples_matrix.dtype.type is np.int_ and not np.isfinite(subsamples_matrix.sum()) and not np.all(np.isfinite(subsamples_matrix))):
        raise ValueError('\nERROR: DBSCAN_multiplex @ load:\n' 
                         'the array of subsampled indices contains at least one infinite or NaN entry.\n')

    if not np.all(subsamples_matrix >= 0):
        raise ValueError('\nERROR: DBSCAN_multiplex @ load:\n'
                         'the sampled indices should all be positive integers.\n') 

    N_samples = data.shape[0]
    N_runs, N_subsamples = subsamples_matrix.shape

    if N_subsamples > N_samples:
        raise ValueError('\nERROR: DBSCAN_multiplex @ load:\n'
                         'the number of sampled indices cannot exceed the total number of samples in the whole data-set.\n')

    for i in xrange(N_runs):
        subsamples_matrix[i] = np.unique(subsamples_matrix[i])
 
    if not isinstance(minPts, int):
        raise TypeError("\nERROR: DBSCAN_multiplex @ load:\n"
                        "the parameter 'minPts' must be an integer.\n")

    if minPts < 2:
        raise ValueError("\nERROR: DBSCAN_multiplex @ load:\n"
                         "the value of 'minPts' must be larger than 1.\n")        

    if eps is None:
        # Determine the parameter 'eps' as the median of the distribution
        # of the maximum of the minPts-nearest neighbors distances for each sample.
        if verbose:
            print("INFO: DBSCAN_multiplex @ load:\n"
                  "starting the determination of an appropriate value of 'eps' for this data-set"
                  " and for the other parameter of the DBSCAN algorithm set to {minPts}.\n"
                  "This might take a while.".format(**locals()))

        beg_eps = time.time()

        quantile = np.rint(quantile)
        quantile = np.clip(quantile, 0, 100)

        k_distances = kneighbors_graph(data, minPts, mode = 'distance', metric = metric, p = p).data
 
        radii = np.zeros(N_samples, dtype = float)
        for i in xrange(0, minPts):
            radii = np.maximum(radii, k_distances[i::minPts]) 
             
        if quantile == 50:     
            eps = round(np.median(radii, overwrite_input = True), 4)
        else:
            eps = round(np.percentile(radii, quantile), 4)

        end_eps = time.time()

        if verbose:
            print("\nINFO: DBSCAN_multiplex @ load:\n"
                  "done with evaluating parameter 'eps' from the data-set provided."
                  " This took {} seconds. Value of epsilon: {}.".format(round(end_eps - beg_eps, 4), eps))

    else:
        if not (isinstance(eps, float) or isinstance(eps, int)):
            raise ValueError("\nERROR: DBSCAN_multiplex @ load:\n"
                             "please provide a numeric value for the radius 'eps'.\n")

        if not eps > 0.0:
            raise ValueError("\nERROR: DBSCAN_multiplex @ load:\n"
                             "the radius 'eps' must be positive.\n")

        eps = round(eps, 4)

    # For all samples with a large enough neighborhood, 'neighborhoods_indices' 
    # and 'neighborhoods_indptr' help us find the neighbors to every sample. Note
    # that this definition of neighbors leaves the original point in,
    # which will be considered later.
    if verbose:
        print("\nINFO: DBSCAN_multiplex @ load:\n"
             "identifying the neighbors within an hypersphere of radius {eps} around each sample,"
             " while at the same time evaluating the number of epsilon-neighbors for each sample.\n"
             "This might take a fair amount of time.".format(**locals()))

    beg_neigh = time.time()

    fileh = tables.open_file(hdf5_file_name, mode = 'r+')
    DBSCAN_group = fileh.create_group(fileh.root, 'DBSCAN_group')

    neighborhoods_indices = fileh.create_earray(DBSCAN_group, 'neighborhoods_indices', tables.Int32Atom(), (0,), 
                                                'Indices array for sparse matrix of neighborhoods', 
                                                expectedrows = int((N_samples ** 2) / 50))

    # 'neighborhoods_indptr' is such that for each of row i of the data-matrix 
    # neighborhoods_indices[neighborhoods_indptr[i]:neighborhoods_indptr[i+1]]
    # contains the column indices of row i from the array of 
    # 'eps'-neighborhoods.
    neighborhoods_indptr = np.zeros(1, dtype = np.int64)

    # For each sample, 'neighbors_counts' will keep a tally of the number 
    # of its  neighbors within a hypersphere of radius 'eps'. 
    # Note that the sample itself is counted as part of this neighborhood.
    neighbors_counts = fileh.create_carray(DBSCAN_group, 'neighbors_counts', tables.Int32Atom(), (N_runs, N_samples), 
                                           'Array of the number of neighbors around each sample of a set of subsampled points', 
                                           filters = None)   

    chunks_size = get_chunk_size(N_samples, 3)
    for i in xrange(0, N_samples, chunks_size):
        chunk = data[i:min(i + chunks_size, N_samples)]

        D = pairwise_distances(chunk, data, metric = metric, p = p, n_jobs = 1)
            
        D = (D <= eps)

        if samples_weights is None:
            for run in xrange(N_runs):
                x = subsamples_matrix[run]
                M = np.take(D, x, axis = 1)

                legit_rows = np.intersect1d(i + np.arange(min(chunks_size, N_samples - i)), x, assume_unique = True)
                M = np.take(M, legit_rows - i, axis = 0)
                
                neighbors_counts[run, legit_rows] = M.sum(axis = 1)

                del M
        else:
            for run in xrange(N_runs):
                x = subsamples_matrix[run]

                M = np.take(D, x, axis = 1)

                legit_rows = np.intersect1d(i + np.arange(min(chunks_size, N_samples - i)), x, assume_unique = True)
                M = np.take(M, legit_rows - i, axis = 0)

                neighbors_counts[run, legit_rows] = np.array([np.sum(samples_weights[x[row]]) for row in M])

                del M

        candidates = np.where(D == True)

        del D

        neighborhoods_indices.append(candidates[1])

        _, nbr = np.unique(candidates[0], return_counts = True)
        counts = np.cumsum(nbr) + neighborhoods_indptr[-1]

        del candidates

        neighborhoods_indptr = np.append(neighborhoods_indptr, counts)

    fileh.create_carray(DBSCAN_group, 'neighborhoods_indptr', tables.Int64Atom(), (N_samples + 1,), 
                        'Array of cumulative number of column indices for each row', filters = None)
    fileh.root.DBSCAN_group.neighborhoods_indptr[:] = neighborhoods_indptr[:]

    fileh.create_carray(DBSCAN_group, 'subsamples_matrix', tables.Int32Atom(), (N_runs, N_subsamples), 
                        'Array of subsamples indices', filters = None)
    fileh.root.DBSCAN_group.subsamples_matrix[:] = subsamples_matrix[:]

    fileh.close()

    end_neigh = time.time()

    if verbose:
        print("\nINFO: DBSCAN_multiplex @ load:\n"
              "done with the neighborhoods. This step took {} seconds.".format(round(end_neigh - beg_neigh, 4)))

    gc.collect()

    return eps
    

def shoot(hdf5_file_name, minPts, sample_ID = 0, random_state = None, verbose = True): 
    """Perform DBSCAN clustering with parameters 'minPts' and 'eps'
        (as determined by a prior call to 'load' from this module). 
        If multiple subsamples of the dataset were provided in a preliminary call to 'load', 
        'sample_ID' specifies which one of those subsamples is to undergo DBSCAN clustering. 
         
    Parameters
    ----------
    hdf5_file_name : file object or string
        The handle or name of an HDF5 file where any array needed for DBSCAN and too large to fit into memory 
        is to be stored. Procedure 'shoot' relies on arrays stored in this data structure by a previous
        call to 'load' (see corresponding documentation)
    sample_ID : int, optional (default = 0)
        Identifies the particular set of selected data-points on which to perform DBSCAN.
        If not subsamples were provided in the call to 'load', the whole dataset will be subjected to DBSCAN clustering.
    minPts : int
        The number of points within a 'eps'-radius hypershpere for this region to qualify as dense.
    random_state: np.RandomState, optional (default = None)
        The generator used to reorder the samples. If None at input, will be set to np.random.
    verbose : Boolean, optional (default = True)
        Whether to display messages concerning the status of the computations and the time it took to complete 
        each major stage of the algorithm.
    Returns
    -------
    core_samples : array of shape (n_core_samples, )
        Indices of the core samples.
    labels : array of shape (N_samples, ) 
        Holds the cluster labels of each sample. The points considered as noise have entries -1. 
        The points not initially selected for clustering (i.e. not listed in 'subsampled_indices', if the latter
        has been provided in the call to 'load' from this module) are labelled -2.
    References
    ----------
    Ester, M., H. P. Kriegel, J. Sander and X. Xu, "A Density-Based
    Algorithm for Discovering Clusters in Large Spatial Databases with Noise".
    In: Proceedings of the 2nd International Conference on Knowledge Discovery
    and Data Mining, Portland, OR, AAAI Press, pp. 226-231. 1996
    """       
        
    fileh = tables.open_file(hdf5_file_name, mode = 'r+')

    neighborhoods_indices = fileh.root.DBSCAN_group.neighborhoods_indices
    neighborhoods_indptr = fileh.root.DBSCAN_group.neighborhoods_indptr[:]

    neighbors_counts = fileh.root.DBSCAN_group.neighbors_counts[sample_ID]
    subsampled_indices = fileh.root.DBSCAN_group.subsamples_matrix[sample_ID]

    N_samples = neighborhoods_indptr.size - 1
    N_runs, N_subsamples = fileh.root.DBSCAN_group.subsamples_matrix.shape

    if not isinstance(sample_ID, int):
        raise ValueError("\nERROR: DBSCAN_multiplex @ shoot:\n"
                         "'sample_ID' must be an integer identifying the set of subsampled indices "
                         "on which to perform DBSCAN clustering\n")    

    if (sample_ID < 0) or (sample_ID >= N_runs):
        raise ValueError("\nERROR: DBSCAN_multiplex @ shoot:\n"
                 "'sample_ID' must belong to the interval [0; {}].\n".format(N_runs - 1))
      
    # points that have not been sampled are labelled with -2
    labels = np.full(N_samples, -2, dtype = int)
    # among the points selected for clustering, 
    # all are initally characterized as noise
    labels[subsampled_indices] = - 1
    
    random_state = check_random_state(random_state)

    core_samples = np.flatnonzero(neighbors_counts >= minPts)
 
    index_order = np.take(core_samples, random_state.permutation(core_samples.size))

    cluster_ID = 0

    # Look at all the selected samples, see if they qualify as core samples
    # Create a new cluster from those core samples
    for index in index_order:
        if labels[index] not in {-1, -2}:
            continue

        labels[index] = cluster_ID

        candidates = [index]
        while len(candidates) > 0:
            candidate_neighbors = np.zeros(0, dtype = np.int32)
            for k in candidates:
                candidate_neighbors = np.append(candidate_neighbors, 
                                                neighborhoods_indices[neighborhoods_indptr[k]: neighborhoods_indptr[k+1]])
                candidate_neighbors = np.unique(candidate_neighbors)

            candidate_neighbors = np.intersect1d(candidate_neighbors, subsampled_indices, assume_unique = True)
                
            not_noise_anymore = np.compress(np.take(labels, candidate_neighbors) == -1, candidate_neighbors)
            
            labels[not_noise_anymore] = cluster_ID

            # Eliminate as potential candidates the points that have already 
            # been used to expand the current cluster by a trail 
            # of density-reachable points
            candidates = np.intersect1d(not_noise_anymore, core_samples, assume_unique = True) 
     
        cluster_ID += 1
    # Done with building this cluster. 
    # "cluster_ID" is now labelling the next cluster.

    fileh.close()

    gc.collect()

    return core_samples, labels


def DBSCAN(data, minPts, eps = None, quantile = 50, subsamples_matrix = None, samples_weights = None, 
metric = 'minkowski', p = 2, verbose = True):
    """Performs Density-Based Spatial Clustering of Applications with Noise,
        possibly on various subsamples or combinations of data-points extracted from the whole dataset, 'data'.
        
        If the radius 'eps' is not provided by the user, it will be determined in an adaptive, data-dependent way 
        by a call to 'load' from this module (see the corresponding documentation for more explanations).
        
        Unlike Scikit-learn's and many other versions of DBSCAN, this implementation does not experience failure 
        due to 'MemoryError' exceptions for large data-sets.
        Indeed, any array too large to fit into memory is stored on disk in an HDF5 data structure.
    
    Parameters
    ----------
    data : array of shape (n_samples, n_features)
        The data-set to be analysed. Subsamples of this curated data-set can also be analysed 
        by a call to DBSCAN by providing lits of selected data-points, stored in 'subsamples_matrix' (see below).
    subsamples_matrix : array of shape (n_runs, n_subsamples), optional (default = None)
        Each row of this matrix contains a set of indices identifying the samples selected from the whole data-set 
        for each of 'n_runs' independent rounds of DBSCAN clusterings.
    minPts : int
        The number of points within an epsilon-radius hypershpere for the said region to qualify as dense.
    eps : float, optional (default = None)
        Sets the maximum distance separating two data-points for those data-points to be considered 
        as part of the same neighborhood.
    quantile : int, optional (default = 50)
        If 'eps' is not provided by the user, it will be determined as the 'quantile' of the distribution
        of the k-nearest distances to each sample, with k set to 'minPts'.
    samples_weights : array of shape (n_runs, n_samples), optional (default = None)
        Holds the weights of each sample. A sample with weight greater than 'minPts' is guaranteed 
        to be a core sample; a sample with negative weight tends to prevent its 'eps'-neighbors from being core. 
        Weights are absolute and default to 1.
    metric : string or callable, optional (default = 'euclidean')
        The metric to use for computing the pairwise distances between samples
        (each sample corresponds to a row in 'data'). 
        If metric is a string or callable, it must be compatible with metrics.pairwise.pairwise_distances.
    p : float, optional (default = 2)
        If a Minkowski metric is used, 'p' denotes its power.
    verbose : Boolean, optional (default = True)
        Whether to display messages reporting the status of the computations and the time it took to complete
        each major stage of the algorithm. 
    
    Returns
    -------
    eps : float
        The parameter of DBSCAN clustering specifying if points are density-reachable. 
        This is relevant if the user chose to let our procedures search for a value of this radius as a quantile
        of the distribution of 'minPts'-nearest distances for each data-point.
    
    labels_matrix : array of shape (N_samples, ) 
        For each sample, specifies the identity of the cluster to which it has been
        assigned by DBSCAN. The points classified as noise have entries -1. The points that have not been
        considered for clustering are labelled -2.
        
    References
    ----------
    Ester, M., H. P. Kriegel, J. Sander and X. Xu, "A Density-Based
    Algorithm for Discovering Clusters in Large Spatial Databases with Noise".
    In: Proceedings of the 2nd International Conference on Knowledge Discovery
    and Data Mining, Portland, OR, AAAI Press, pp. 226-231. 1996
    """
        
    assert isinstance(minPts, int) or type(minPts) is np.int_
    assert minPts > 1

    if subsamples_matrix is None:
        subsamples_matrix = np.arange(data.shape[0], dtype = int)
        subsamples_matrix = subsamples_matrix.reshape(1, -1)
    else:
        subsamples_matrix = np.array(subsamples_matrix, copy = False)

    N_runs = subsamples_matrix.shape[0]
    N_samples = data.shape[0]

    labels_matrix = np.zeros((N_runs, N_samples), dtype = int)

    with NamedTemporaryFile('w', suffix = '.h5', delete = True, dir = './') as f:
        eps = load(f.name, data, minPts, eps, quantile, subsamples_matrix, samples_weights, metric, p, verbose)

        for run in xrange(N_runs):
            _, labels = shoot(f.name, minPts, sample_ID = run, verbose = verbose)
            labels_matrix[run] = labels

    return eps, labels_matrix


# ###Overall Clustering

# In[7]:

def clustering(df1, df2, file_name='Cluster_Results', tol=10, store_idx=True, 
               plot=False, plotData=False, pca2D=True, sklearn=sklearn):
    '''
    param df1 [DataFrame] : Original DataFrame with time series data
    param df2 [DataFrame] : DataFrame with columns for clustering (including Time, UserID and JobID)
    file_name [str] : Folder name
    tol [int] : Minimum cluster size to be considered significant
    store_idx [bool] : store cluster ID with Job ID
    plot [bool] : Plot time series from each cluster
    plotData [bool] : Store plot data
    pca2D [bool] : plot data for 2D PCA
    '''
    
    #IO Columns
    io_columns = ['nfs:vfs_readpage', 'nfs:vfs_writepage',
        'block:rd_ios','block:wr_ios', 
        'llite:read_bytes', 'llite:write_bytes']
    
    #Remove folder if it exists
    if os.path.exists(file_name):
        shutil.rmtree(file_name)
        
    #Create working directory
    os.makedirs(file_name)
      
    #meta_df = df2.copy(deep=True) #For calculations
    meta_df2 = df2[['JobID','UserID','Time']] #For getting the JobID, UserID and Time
    meta_df = df2.drop(['JobID','UserID','Time'],axis=1)

    #log
    with open(file_name+'/log.txt','w') as log_handle:
        log_handle.write('Generating log\n')
        log_handle.write('Starting...\n')
#         log_handle.write('df1:\t'+str(df1.memory_usage().sum())+'\n')
#         log_handle.write('df2:\t'+str(df2.memory_usage().sum())+'\n')
        log_handle.write('meta_df:\t'+str(meta_df.memory_usage().sum())+'\n')
        log_handle.write('meta_df2:\t'+str(meta_df2.memory_usage().sum())+'\n')
    
    #results
    with open(file_name+'/log.txt','a') as log_handle:
        log_handle.write('Creating results.txt\n')
        
    with open(file_name+'/results.txt', 'w') as handle:
        
        #Title
        handle.write('This file contains info on all clusters\n\n')
    
        #Scaling and Decomposition
        scale = StandardScaler()
        cluster_data = scale.fit_transform(meta_df.values)
        pca = PCA()
        pca.fit(cluster_data)
        cumulative_variance = np.cumsum(pca.explained_variance_ratio_)
        n_components = [i for i,val in enumerate(cumulative_variance) if val>0.80][0]
        cluster_data = PCA(n_components=n_components).fit(cluster_data).transform(cluster_data)
        
        with open(file_name+'/log.txt','a') as log_handle:
            log_handle.write('PCA Components:\t'+str(n_components)+'\n')
        
        #Totals
        totalJob = float(len(meta_df2))
        totalUsers = float(len(meta_df2['UserID'].unique()))
        
        handle.write('Total Jobs:\t'+str(int(totalJob))+'\n')
        handle.write('Total Users:\t'+str(int(totalUsers))+'\n\n')

        #Clustering
        with open(file_name+'/log.txt','a') as log_handle:
            log_handle.write('Testing gaussian methods for clustering\n')
        
        #Sklearn Clustering vs Custom Multiplex
        if sklearn:
            with open(file_name+'/log.txt','a') as log_handle:
                log_handle.write('Fitting with Sklearn KMeans...\n')
            # elbow curve
            with open(file_name+'/log.txt','a') as log_handle:
                log_handle.write('Calculating number of clusters with elbow curve...\n')
            diff = 999999 #Arbritrary large difference value
            k = 1         #Starting point for number of components
            avgWithinSS_0 = 999999 #Arbritrary large start value
            while True:
                KM = KMeans(n_clusters=k).fit(cluster_data)
                centroids = KM.cluster_centers_
                D_k = [cdist(cluster_data, centroids, 'euclidean')]
                Idx = [np.argmin(D,axis=1) for D in D_k]
                dist = [np.min(D,axis=1) for D in D_k]
                avgWithinSS_1 = [sum(d)/np.shape(cluster_data)[0] for d in dist][0]
                diff = avgWithinSS_0 - avgWithinSS_1
                with open(file_name+'/log.txt','a') as log_handle:
                    log_handle.write('Difference in Variance:\t'+str(diff)+'\n')
                if diff > tol_kmeans:
                    k += 1
                    avgWithinSS_0 = avgWithinSS_1
                else:
                    break
            with open(file_name+'/log.txt','a') as log_handle:
                log_handle.write('Fitting with Sklearn KMeans...\n')
            clustering_algo = KMeans(n_clusters=k)
            with open(file_name+'/log.txt','a') as log_handle:
                log_handle.write('Fitting with KMeans...\n')
            clustering_algo.fit(cluster_data)
            with open(file_name+'/log.txt','a') as log_handle:
                log_handle.write('Predicting clusters...\n')
            clusters = clustering_algo.labels_
        else:
            with open(file_name+'/log.txt','a') as log_handle:
                log_handle.write('Fitting with Custom Multiplex DBSCAN...\n')
            eps, clusters = DBSCAN(cluster_data, minPts = m, verbose = False)
            clusters = clusters[0] #transform [[0,1,4...]] to [0,1,4...]
            with open(file_name+'/log.txt','a') as log_handle:
                log_handle.write('EPS Value: %f\n'%eps)
        
        if len(set(clusters))>1:
            with open(file_name+'/log.txt','a') as log_handle:
                log_handle.write('Number of clusters:\t'+str(len(set(clusters)))+'\n')
            handle.write('Number of clusters:\t'+str(len(set(clusters)))+'\n')
        else:
            with open(file_name+'/log.txt','a') as log_handle:
                log_handle.write('Not enough clusters with gaussian methods\n')
            handle.write('Not enough clusters with DBSCAN\n')
            return
        
        meta_df['Clusters'] = clusters
        meta_df2['Clusters'] = clusters
        
        #clusterID.csv
        if store_idx:
            with open(file_name+'/log.txt','a') as log_handle:
                log_handle.write('Creating clusterID.csv.gz\n')
            meta_df2[['JobID','UserID','Clusters']].to_csv(
                file_name+'/clusterID.csv.gz', compression='gzip', index=False
            )

        
        #Cluster Size
        jobSize = {}
        userSize = {}
        jobPercent = {}
        userPercent = {}
        
        for i in set(clusters):
            jobSize[i] = meta_df2[meta_df2['Clusters']==i].shape[0]
            userSize[i] = meta_df2[meta_df2['Clusters']==i]['UserID'].unique().shape[0]
            jobPercent[i] = '%.2f'%(float(jobSize[i])/totalJob*100)
            userPercent[i] = '%.2f'%(float(userSize[i])/totalUsers*100)
            
        handle.write('\n\n\n\nID\tJobs\t\tUsers:\n')
        for i in set(clusters):
            handle.write(str(i)+'\t'+str(jobSize[i])+' ('+jobPercent[i]+'%)'+'\t'+str(userSize[i])+                         ' ('+userPercent[i]+'%)'+str('\n'))

        handle.write('\n\n\n\nCluster Centers:\n\n')
        handle.write(str(meta_df.groupby('Clusters').mean()))
        
    #describe.html
    sig_cluster = []
    for k in jobSize.keys():
        if jobSize[k]>tol:
            sig_cluster.append(k)
    des = meta_df2[meta_df2['Clusters'].isin(sig_cluster)].describe()
    with open(file_name+'/log.txt','a') as log_handle:
        log_handle.write('Creating describe.html\n')
        log_handle.write('des:\t'+str(des.memory_usage().sum())+'\n')
    des.to_csv(file_name+'/describe.csv')
    gc.collect()
    
    #Centers:
    with open(file_name+'/log.txt','a') as log_handle:
        log_handle.write('Creating centers.pkl\n')
    meta_df.groupby('Clusters').mean().to_pickle(file_name+'/centers.pkl')

    #significant
    with open(file_name+'/log.txt','a') as log_handle:
        log_handle.write('Creating signficant.txt\n')
        
    with open(file_name+'/significant.txt', 'w') as handle:
        
        #Title
        handle.write('This file contains info on clusters with atleast '+str(tol)+ ' jobs\n\n')
        
        #Select clusters of min size of 1000
        with open(file_name+'/log.txt','a') as log_handle:
            log_handle.write('Significant Clusters:\t'+str(len(sig_cluster))+'\n')
        handle.write('Significant Clusters:\t'+str(len(sig_cluster))+'\n\n')
        
        meta_df = meta_df[meta_df['Clusters'].isin(sig_cluster)]
        meta_df2 = meta_df2[meta_df2['Clusters'].isin(sig_cluster)]
        
        handle.write('Total Jobs:\t'+str(int(totalJob))+'\n')
        handle.write('Total Users:\t'+str(int(totalUsers))+'\n')
            
        #Cluster Size
        jobSize = {}
        userSize = {}
        jobPercent = {}
        userPercent = {}
        
        for i in set(sig_cluster):
            jobSize[i] = meta_df2[meta_df2['Clusters']==i].shape[0]
            userSize[i] = meta_df2[meta_df2['Clusters']==i]['UserID'].unique().shape[0]
            jobPercent[i] = '%.2f'%(float(jobSize[i])/totalJob*100)
            userPercent[i] = '%.2f'%(float(userSize[i])/totalUsers*100)    
        
        handle.write('\nID\tJobs\t\tUsers:\n')
        for i in set(sig_cluster):
            handle.write(str(i)+'\t'+str(jobSize[i])+' ('+jobPercent[i]+'%)'+'\t'+str(userSize[i])+                         ' ('+userPercent[i]+'%)'+'\n')
        
        #Ratios:
        agg_col = [i for i in meta_df2.columns if '_agg' in i]
        meta_df2['Total'] = [0] * len(meta_df2)
        for col in agg_col:
            meta_df2['Total'] += meta_df2[col]
        for col in agg_col:
            meta_df2[col[:-4]+'_ratio'] = meta_df2[col]/meta_df2['Total']
        meta_df2 = meta_df2.drop('Total', axis=1)

        handle.write('\n\n\n\nCluster Centers:\n\n')
        handle.write(str(meta_df.groupby('Clusters').mean()))
        
        #Upper Bound
        handle.write('\n\n\n\n\nUpper Bound\n\n')
        for col in agg_col:
            out = meta_df2[meta_df2[col]>(des[col]['75%']+1.5*(des[col]['75%']-des[col]['25%']))][col]
            if len(out)>0:
                handle.write(col)
                handle.write(str(out)+'\n\n')
                
        #Upper Bound
        handle.write('\n\nLower Bound\n\n')
        for col in agg_col:
            out = meta_df2[meta_df2[col]<(des[col]['25%']-1.5*(des[col]['75%']-des[col]['25%']))][col]
            if len(out)>0:
                handle.write(col)
                handle.write(str(out)+'\n\n')
    
    #Plot Data
    if plotData:
        with open(file_name+'/log.txt','a') as log_handle:
            log_handle.write('Colelcting Plot Data\n')
        idx = np.hstack(
                    meta_df.reset_index()
                    .groupby('Clusters')['index']
                    .apply(list).apply(lambda x:x[:3]).values
                         )
        plot_df = meta_df2.loc[idx,io_columns+['Time','Clusters']]
        plot_df.to_pickle(file_name+'/plot_data.pkl')
        
    #2D PCA Data
    if pca2D:
        with open(file_name+'/log.txt','a') as log_handle:
            log_handle.write('Colelcting 2D PCA Data\n')
        pca = PCA(n_components=2)
        data2D = pca.fit(cluster_data).transform(cluster_data)
        data2D_df = pd.DataFrame({'V1':data2D[:,0],
                                  'V2':data2D[:,1],
                                  'Clusters':clusters})
        data2D_df.to_csv(file_name+'/pca2D.csv.gz', compression='gzip', index=False)
        
        
    #Plots
    if plot:
        with open(file_name+'/log.txt','a') as log_handle:
            log_handle.write('Plotting Time Series of Significant Clusters\n')

        #Pick significant clusters
        for i in sig_cluster:

            #Create Figure
            fig = plt.figure(figsize=(25,15))
            fig.suptitle('Cluster: '+str(i), fontsize=20)
            gs = gridspec.GridSpec(6, 3)

            #Pick 3 random rows for each cluster
            idx_list = meta_df2[meta_df2['Clusters']==i]            .sample(n=3)['JobID'].values

            #Plotting
            col_id = -1
            for idx in idx_list:
                col_id += 1
                row_id = -1
                for io in io_columns:
                    row_id += 1
                    time_series_list = df1.loc[idx, io]
                    time_list = df1.loc[idx,'Time']
                    ax = fig.add_subplot(gs[row_id,col_id])
                    for ts_idx in range(len(time_series_list)):
                        ax.step(time_list[ts_idx],time_series_list[ts_idx])
                        ax.set_ylabel(io)
                        ax.set_xlim(min(time_list[ts_idx]),max(time_list[ts_idx]))
                        if row_id == 0:
                            ax.set_title('JobID: '+str(idx))
                        if row_id == 5:
                            ax.set_xlabel('Time/Hours')
            plt.savefig(file_name+'/Cluster_'+str(i)+'.png')        
        
        plt.figure(figsize=(10,5))
        leg = []
        for i in data2D_df['Clusters'].unique():
            if i== 7:
                plt.plot(V1,V2, 'x', alpha=0.3)
            leg.append(i)
            V1 = data2D_df[data2D_df['Clusters']==i]['V1']
            V2 = data2D_df[data2D_df['Clusters']==i]['V2']
            plt.plot(V1,V2, 'o', alpha=0.3)
            plt.legend(leg, loc=0)
        plt.title('Aggregate I/O Clusters')
        plt.xlabel('PCA Component 1')
        plt.ylabel('PCA Component 2')
        plt.savefig(file_name+'/plot2D.png')
        
    with open(file_name+'/log.txt','a') as log_handle:
        log_handle.write('Analysis Complete!!!\n')


# In[8]:

clustering(ndf, df2, sklearn=sklearn, file_name='Clean_NFS_'+str(m))

