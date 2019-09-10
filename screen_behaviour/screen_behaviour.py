from datetime import datetime as dt
import numpy as np
import pandas as pd

from .invalidate_bins import invalid_timebins
from .screen_measures import prepare_screen_measurement, screen_measures, merge_short_long


#*****************************************************************************************************************
#*****************************************************************************************************************
#*****************************************************************************************************************


def screen_behaviour(screen, 
                     invalidation_stamps, 
                     timebin_len = 900, 
                     invalidate_cut = 1800, 
                     short_ses_len = 35,
                     max_screen_ses = 7200) :
    
    """
    Return a dataframe with the number of seconds and the number of times, the screen has been on in each timebin.
    
    Note: Due to the burden of history the helper functions of this function uses a different representation of
    the timebins than the main function. The main function represents each timebin by the number of seconds passed,
    since the beginning of epoch time until the beginning of the timebin. The helper functions represent each
    timebin by a so called bin_id, which is the number of timebins passed, since the beginning of epoch time.
    
    Parameters
    ----------
    screen              : pandas.DataFrame

                          A dataframe with two variables:
                          * screen_on: 1, when the screen is turned on. 
                                       0, when the screen is turned off.
                          * timestamp: The epoch time of the given observation. 
  
                          The dataframe should only contain observations from one user.
  
                          This dataframe is used to construct the measures of the given user's screen usage.
    
    invalidation_stamps : pandas.DataFrame
                    
                          A dataframe with one variable:
                          * timestamp: The epoch time when a signal was received from the phone.

                          The dataframe should only contain observations from one user.

                          This dataframe is used to determine the timebins, where the phone is assumed to be off,
                          which means the bins should be invalidated.
    
    timebin_len         : int
    
                          Number of seconds in each timebin.
    
    invalidate_cut      : int
    
                          Maximum number of seconds allowed between two timestamps in invalidation_stamps,
                          before the phone is assumed to be off in between the two stamps.
                          
    short_ses_len       : int
                          
                          Maximum number of seconds the screen can be on, before the given screen session
                          is considered a long session instead of a short session.
    
    max_screen_ses      : int
    
                          Maximum number of seconds the screen can be on, before the given screen session
                          is considered unrealistically long and therefore invalidated.
    
    Output
    ------
    A pandas.DataFrame with measures of screen usage for the given user. 
    
    The data has eight variables:
    
    * user_idx              : int
     
                              Id of the user that the measurements are made for. 
                              Same number in all the rows.
     
    * timebin               : int 
    
                              Number of seconds since the beginning of epoch time.
    
    * screentime_short_ses  : int 
                             
                              Number of seconds the screen was turned on in short sessions during the given timebin.
    
    * screentime_long_ses   : int
    
                              Number of seconds the screen was turned on in long sessions during the given timebin.
    
    * screentime            : int
    
                              Number of seconds the screen was turned on during the given timebin.
                              The sum of screentime_short_ses and screentime_long_ses.

    * screencount_short_ses : int
    
                              Number of times the screen was turned on in short sessions during the given timebin.
    
    * screencount_long_ses  : int
    
                              Number of times the screen was turned on in long sessions during the given timebin.
    
    * screencount           : int
    
                              Number of times the screen was turned on during the given timebin.
                              The sum of screencount_short_ses and screencount_long_ses.
    
    """
    
    #Prepare the screen and the invalidation_stamps dataframe ----------------------
    screen_user = screen.loc[screen.index[0], 'user_idx']
    invalidation_user = invalidation_stamps.loc[invalidation_stamps.index[0], 'user_idx']
    assert (screen_user == invalidation_user)
    
    screen = screen.drop('user_idx', axis = 1)
    invalidation_stamps = invalidation_stamps.drop('user_idx', axis = 1)
    
    screen = sort_by_timestamp(screen)
    invalidation_stamps = sort_by_timestamp(invalidation_stamps)
    #-------------------------------------------------------------------------------
    
    #Determine the invalid timebins
    invalid_bins = invalid_timebins(invalidation_stamps, timebin_len, invalidate_cut)
    
    #Invalidate screen observations
    screen = invalidate_off_bins(screen, invalid_bins, timebin_len)
    screen = invalidate_twins(screen)
    #-------------------------------------------------------------------------------
    
    #Prepare the screen dataframe for the screen_measure function
    screen = prepare_screen_measurement(screen, timebin_len, short_ses_len, max_screen_ses)
    
    #Calculate the screen measurements
    screen_mes_short_ses = screen_measures(screen, timebin_len, True)
    screen_mes_long_ses = screen_measures(screen, timebin_len, False)
    screen_mes_both = merge_short_long(screen_mes_short_ses, screen_mes_long_ses)
    #-------------------------------------------------------------------------------
    
    #Change from the bin_id representation of timebins to the time-at-start representation 
    invalid_bins['timebin'] = invalid_bins['bin_id'] * timebin_len
    invalid_bins = invalid_bins.drop('bin_id', axis = 1)
    screen_mes_both['timebin'] = screen_mes_both['bin_id'] * timebin_len
    screen_mes_both = screen_mes_both.drop('bin_id', axis = 1)
    #-------------------------------------------------------------------------------------
    
    #Get all valid timebins for the user
    valid_bins = valid_timebins(invalid_bins, timebin_len)
    
    #Add zeros in the valid timebins without positive measurements
    screen_mes_w_zeros = valid_bins.merge(screen_mes_both, on = 'timebin', how = 'left')
    
    screen_mes_list = ['screentime', 'screentime_short_ses', 'screentime_long_ses',
                       'screencount', 'screencount_short_ses', 'screencount_long_ses']
    
    screen_mes_w_zeros.loc[np.isnan(screen_mes_w_zeros.screentime), screen_mes_list] = 0
    #--------------------------------------------------------------------------------------
    #Transform the scale to percent of timebin instead of number of seconds in timebin
    screen_mes_w_zeros[screen_mes_list] = screen_mes_w_zeros[screen_mes_list] / timebin_len * 100
    
    #Add the user id to the dataframe
    screen_mes_w_zeros['user_idx'] = screen_user
    
    return screen_mes_w_zeros.astype(int)


#*****************************************************************************************************************
#*****************************************************************************************************************
#*****************************************************************************************************************


def sort_by_timestamp(df) :
    
    """
    Helper function for screen_behaviour
    
    Return a dataframe which has been sorted according to the timestamps
    """
    
    return df.reset_index().sort_values(['timestamp','index']).drop('index',axis=1).reset_index(drop=True)


#-----------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------


def invalidate_off_bins(screen, off_bins_df, timebin_len) :
    
    """
    Helper function for screen_behaviour
    
    Invalidate screen observations made in timebins, where the phone is assumed to be turned off
    according to the off_bins_df
    """
    
    screen['bin_id'] = screen['timestamp'] // timebin_len
    
    screen = screen.merge(off_bins_df, on = 'bin_id', how = 'left')
    
    screen.loc[screen['invalid'] == 1, 'timestamp'] = np.nan
    
    screen = screen.drop(['invalid', 'bin_id'], axis=1)
    
    return screen


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def invalidate_twins(screen) :
    
    """
    Helper function for screen_behaviour
    
    Invalidate screen observations if the screen is turned on twice without being turned off in between
    or vice versa
    """
            
    screen['twins_test'] = screen['screen_on'].diff()
    
    screen.loc[screen['twins_test']==0, 'timestamp'] = np.nan
    
    screen = screen.drop('twins_test',axis=1)
    
    return screen


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def valid_timebins(invalid_bins, timebin_len) : 
    
    """
    Helper function for screen_behaviour
    
    Return a dataframe with all the timebins where the user's phone is assumed to be turned on.
    """  
    
    first_timebin = ((dt(2013,9,day=1) - dt(year=1970,month=1,day=1)).days) * (24 * 60 * 60)

    delta = ((dt(2015, 8, day=31, hour=23, minute=59, second=59) - dt(year=1970, month=1, day=1)))
    last_timestamp = (delta.days * 24 * 60 * 60 + delta.seconds)
    last_timebin = last_timestamp // timebin_len * timebin_len
    
    all_timebins = pd.DataFrame({'timebin': range(first_timebin, last_timebin + 1, timebin_len)})
    
    valid_bins = all_timebins.merge(invalid_bins, how = 'left')
    valid_bins = valid_bins.loc[np.isnan(valid_bins.invalid), ['timebin']]
    
    return valid_bins.reset_index(drop = True)


#*****************************************************************************************************************
#*****************************************************************************************************************
#*****************************************************************************************************************




