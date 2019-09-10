from datetime import datetime as dt
import numpy as np
import pandas as pd


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def prepare_screen_measurement(screen, timebin_len, short_ses_len, max_screen_ses) :

    """
    Helper function for screen_behaviour
    
    Preprocess the screen data to make it ready for the screen_measures function
    """    
    
    screen['bin_id'] = screen['timestamp'] // timebin_len
    
    screen['timediff'] = screen['timestamp'].diff()
    
    screen.loc[screen.index[0], 'timediff']=0
    
    screen['bin_id_diff'] = screen['bin_id'].diff().fillna(0).astype(int)
    
    screen = screen[~np.isnan(screen['timediff'])]
    
    screen = screen.loc[screen.screen_on == 0, :]
    
    screen = screen.loc[0 < screen['timediff'], :]
    
    screen = screen.loc[screen['timediff']  <= max_screen_ses, :]
       
    screen['short_session'] = (screen['timediff'] <= short_ses_len)
    
    return screen


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def screen_measures(screen, timebin_len, short_session) :

    """
    Helper function for screen_behaviour
    
    Calculate the screen usage measures for the given user from the user's screen-on-off data.
    The screen measures are calculated for either short or long screen sessions only according to 
    the value of the boolean parameter short_session.
    """  
    
    screen = screen.loc[screen.short_session == short_session, :]
    
    if short_session : 
        time_colname = 'screentime_short_ses'
        count_colname = 'screencount_short_ses'
        
    else : 
        time_colname = 'screentime_long_ses'
        count_colname = 'screencount_long_ses'
    
    #simple observations which do not cross bin_id boundaries:
    simple = screen.loc[screen.bin_id_diff == 0, :].copy()
    
    #nasty observations which do cross bin_id boundaries:
    nasty = screen.loc[screen.bin_id_diff > 0, :].copy()
    
    something_simple = (len(simple) > 0)
    something_nasty = (len(nasty) > 0)
    
    #screen measures for simple observations:
    if something_simple :
        
        screen_mes_simple = simple[['bin_id','timediff']].groupby('bin_id', as_index = False).sum()
        screen_mes_simple = screen_mes_simple.rename(columns = {'timediff' : 'screentime_simple'})
        
        simple['screencount_simple'] = 1
        screencount_simple = simple[['bin_id', 'screencount_simple']].groupby('bin_id', as_index = False).count()
        
        screen_mes_simple = screen_mes_simple.merge(screencount_simple, on = 'bin_id', how = 'outer').fillna(0)
        
    #screen measures for nasty observations:
    if something_nasty :
        
        nasty['bin_id_starttime'] = nasty['bin_id'] * timebin_len
        nasty['time_last_bin_id'] = nasty['timestamp'] - nasty['bin_id_starttime']
        
        screen_mes_nasty = f2(nasty, timebin_len)
    
    #merge the measures for simple and nasty observations
    if (something_simple and something_nasty) :    
        screen_mes = screen_mes_simple.merge(screen_mes_nasty, on = 'bin_id', how = 'outer').fillna(0)
        screen_mes[time_colname] = screen_mes['screentime_simple'] + screen_mes['screentime_nasty']
        screen_mes[count_colname] = screen_mes['screencount_simple'] + screen_mes['screencount_nasty']
    
    elif (something_simple) :
        screen_mes = screen_mes_simple.rename(columns = {'screentime_simple': time_colname,
                                                         'screencount_simple': count_colname})
        
    elif (something_nasty) :
        screen_mes = screen_mes_nasty.rename(columns = {'screentime_nasty': time_colname,
                                                        'screencount_nasty': count_colname})
        
    else :
        screen_mes = pd.DataFrame({'bin_id':[], time_colname:[], count_colname: []})
   
    screen_mes = screen_mes[['bin_id',time_colname, count_colname]]
    
    return screen_mes


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def f2(nasty, timebin_len) :
    
    """Helper function for screentime"""
    
    screen_measures = []
    
    for i in range(len(nasty)) :
        
        r = nasty.iloc[i]
        
        these_measures = f1(int(r['bin_id']),
                            int(r['bin_id_diff']),
                            r['timediff'],
                            r['time_last_bin_id'],
                            timebin_len)
        
        screen_measures.append(these_measures)
    
    screen_measures = pd.concat(screen_measures, ignore_index = True)
    
    if any(screen_measures.duplicated(subset = 'bin_id')) : 
        
        screen_measures['screentime_nasty'] = screen_measures[['bin_id','screentime_nasty']]\
                                              .groupby('bin_id').transform('sum')
        
        screen_measures['screencount_nasty'] = screen_measures[['bin_id', 'screencount_nasty']]\
                                               .groupby('bin_id').transform('sum')
        
        screen_measures = screen_measures.drop_duplicates(subset = 'bin_id')
    
    return screen_measures


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def f1(bin_id, bin_id_diff, timediff, time_last_bin_id, bin_len) :
    
    """Helper function for f2"""
    
    p = range(bin_id - bin_id_diff, bin_id + 1)
    
    time_first_bin_id = [timediff - (bin_len * (bin_id_diff - 1)) - time_last_bin_id]
    time_middle_bin_ids = ([bin_len] * (bin_id_diff - 1))
    time_last_bin_id = [time_last_bin_id]
    
    counts = [1] + ( [0] * (len(p) - 1) )
    
    return pd.DataFrame({'bin_id' : p, 
                         'screentime_nasty' : time_first_bin_id + time_middle_bin_ids + time_last_bin_id,
                         'screencount_nasty' : counts})


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------


def merge_short_long(short, long) :

    """
    Helper function for screen_behaviour
    
    Merge the screen measures for the short and long screen sessions into a single dataframe
    """  
    
    both = short.merge(long, on = 'bin_id', how = 'outer').fillna(0)
    
    both['screentime'] = both['screentime_short_ses'] + both['screentime_long_ses']
    
    both['screencount'] = both['screencount_short_ses'] + both['screencount_long_ses']
    
    return both


#----------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------