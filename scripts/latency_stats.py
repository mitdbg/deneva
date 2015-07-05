import numpy as np
import math

# Units of time per second
CONVERSIONS = {
    "s"  : 1.0,
    "ms" : 1000.0,
    "us" : 1000000.0,
    "ns" : 1000000000.0,
}

class LatencyStats(object):

    def __init__(self,latencies,metadata={},in_time_unit='ns',out_time_unit='ms',store=False):
        self._metadata = metadata 
        self._in_time_unit = in_time_unit.lower()
        self._out_time_unit = out_time_unit.lower()
        self.__set_conversion_factor__()
        self._mean = np.mean(latencies)
        self._percentiles = np.percentile(latencies,[50.0,90.0,95.0,99.0])
        self._min = min(latencies)
        self._max = max(latencies)
        if store:
            self._latencies = latencies
        else:
            self._latencies = None
    
    def get_latencies(self):
        return self._latencies

    def get_metadata(self):
        return self._metadata

    def get_mean(self):
        return self._mean*self._conversion_factor
    
    def get_50th(self):
        return self._percentiles[0]*self._conversion_factor
    
    def get_90th(self):
        return self._percentiles[1]*self._conversion_factor

    def get_95th(self):
        return self._percentiles[2]*self._conversion_factor

    def get_99th(self):
        return self._percentiles[3]*self._conversion_factor

    def get_min(self):
        return self._min*self._conversion_factor
    
    def get_max(self):
        return self._max*self._conversion_factor

    def get_percentiles(self):
        return [p*self._conversion_factor for p in self._percentiles]
    
    def set_output_time_unit(self,new_unit):
        self._out_time_unit = new_unit
        self.__set_conversion_factor__()

    def __set_conversion_factor__(self):
        if not self._in_time_unit in CONVERSIONS:
            raise Exception("Invalid input time unit: {}".format(self._in_time_unit))
        if not self._out_time_unit in CONVERSIONS:
            raise Exception("Invalid output time unit: {}".format(self._out_time_unit))
        self._conversion_factor = CONVERSIONS[self._out_time_unit]/CONVERSIONS[self._in_time_unit]

def exec_fn(ls,fn):
    if fn == "99th":
        return ls.get_99th()
    elif fn == "95th":
        return ls.get_95th()
    elif fn == "90th":
        return ls.get_90th()
    elif fn == "50th":
        return ls.get_50th()
    elif fn == "mean":
        return ls.get_mean()
    elif fn == "max":
        return ls.get_max()
    else:
        assert False

# Returns data that is within m standard deviations of the mean
def reject_outliers(data,m=3,method='mean'):
    data2 = np.asarray(data)
    if method == 'median': # possibly less biased by outliers than the mean
        d = np.abs(data2 - np.median(data2))
        mdev = np.median(d)
        data2 = data2[d < m * mdev]
    else:
        data2 = data2[np.abs(data2-np.mean(data2)) < m * np.std(data2)]
    return data2.tolist()


