from datetime import datetime as dt
import numpy as np
import pandas as pd


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def invalid_timebins(invalidation_stamps, timebin_len, invalidate_cut) :

    """
    Helper function for screen_behaviour
    
    Return a dataframe with bin_ids of the timebins where the phone is assumed to be turned off 
    """    
    
    first_time = int((dt(year = 2013, month = 9, day=1) - dt(year=1970, month=1, day=1)).days * (24 * 60 * 60))
    first = pd.DataFrame({'timestamp': first_time}, index = [0])
    
    delta = ((dt(year = 2015, month = 8,day=31, hour=23, minute=59, second=59) - dt(year=1970, month=1, day=1)))
    last_time = int(delta.days * 24 * 60 * 60 + delta.seconds)
    last = pd.DataFrame({'timestamp': last_time}, index = [0])
    
    invalidation_stamps = pd.concat([first, invalidation_stamps, last], ignore_index = True)
    
    invalid_stamps = invalid_timestamps(invalidation_stamps, invalidate_cut)
    
    invalid_bins = invalid_bins_frame(invalid_stamps, timebin_len)
    
    invalid_bins = invalid_bins.drop_duplicates()
    
    return invalid_bins


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------



def invalid_timestamps(invalidation_stamps, invalidate_cut) :
    
    """
    Helper function for invalid_timebins
    
    Return a dataframe with the timestamps, for which all timebins in between have to be invalidated
    """
    
    invalidation_stamps['timediff'] = invalidation_stamps['timestamp'].diff().fillna(0).astype(int)
    
    return invalidation_stamps[invalidation_stamps['timediff'] > invalidate_cut]


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def invalid_bins_frame(invalid_stamps, timebin_len) :
    
    """
    Helper function for invalid_timebins
    
    Return a dataframe with bin_ids of the timebins where the phone is assumed to be turned off 
    """
    
    invalid_bins = []
    
    for i in range(len(invalid_stamps)) :
        
        r = invalid_stamps.iloc[i]
        
        invalid_bins.extend( invalid_bins_id(int(r['timestamp']), int(r['timediff']), timebin_len) )
    
    invalid_bins = pd.DataFrame({'bin_id': invalid_bins})
    
    invalid_bins['invalid'] = 1
    
    return invalid_bins

#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def invalid_bins_id(timestamp, timediff, timebin_len) :
    
    """
    Helper function for invalid_bins_frame
    
    Determine the bins to invalidate given a timestamp and timeinterval
    """
    
    return ( list(range((timestamp - timediff) // timebin_len, (timestamp // timebin_len) + 1)) )


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------