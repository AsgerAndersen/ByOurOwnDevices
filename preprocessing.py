#!/usr/bin/env python
# coding: utf-8

# ## Load libraries

# In[3]:


import os
projpath = 'H:/workdata/705805/'
os.chdir(projpath)


# In[4]:


import pandas as pd
import numpy as np
from datetime import datetime as dt
import cns


# ## Global parameters

# In[5]:


timebin_len = 900


# ## Check the timestamps of the input datasets

# In[6]:


screen = pd.read_csv('data/raw/fixed/external/screen.csv')
invalidation_stamps = pd.read_pickle('data/preproc/behavior/sensor_time.pkl')


# Find the first and last timebin in the entire experiment:

# In[7]:


first_timebin = ((dt(2013,9,day=1) - dt(year=1970,month=1,day=1)).days) * (24 * 60 * 60)
delta = ((dt(2015, 8, day=31, hour=23, minute=59, second=59) - dt(year=1970, month=1, day=1)))
last_timestamp = (delta.days * 24 * 60 * 60 + delta.seconds)
last_timebin = last_timestamp // timebin_len * timebin_len

# Remove the sensor_time timestamps that lie outside the borders defined by the experiment:

# In[10]:


invalidation_stamps_filtered = invalidation_stamps.loc[(invalidation_stamps['timestamp_5m'] >= first_timebin)  & (invalidation_stamps['timestamp_5m'] <= last_timebin)]


# Save the updated timestamps to be used for invalidation

# In[11]:


invalidation_stamps_filtered.to_pickle('personal/asger/preprocessed_data/invalidation_stamps_1m.pkl')


# ## Build screen behaviour data set

# Read in the input datasets for this section:

# In[12]:


screen = pd.read_csv('data/raw/fixed/external/screen.csv')
invalidation_stamps = pd.read_pickle('personal/asger/preprocessed_data/invalidation_stamps_1m.pkl')
user_map = pd.read_pickle('data/preproc/users/all_users.pkl').loc[:,['user_idx','user']]


# Prepare the input data sets, so they fit the function that will build the screen behaviour dataset:

# In[16]:


screen = screen.merge(user_map, how = 'left')
screen = screen.drop('user', axis = 1)
invalidation_stamps = invalidation_stamps.rename(columns = {'timestamp_5m': 'timestamp'})
invalidation_stamps = invalidation_stamps[['timestamp', 'user_idx']]


# Build the screen behaviour dataset with the help of the screen_behaviour function. Note that the screen_behaviour function is quite time consuming (it has to run over night). If you want to speed it up, this block of code is very easy to parallelize with ipyparallel. Path to screen_behaviour function: cns/preproc/screen/screen_behaviour.py.

# In[19]:


by_user_screen = [u_df for u,u_df in screen.groupby('user_idx')]
by_user_invalidation = [u_df for u,u_df in invalidation_stamps.groupby('user_idx')]
parsed = [cns.screen_behaviour(screen, invalidation, timebin_len, 3*timebin_length, only_screen_behav = False) for (screen, invalidation) in zip(by_user_screen, by_user_invalidation)]


# In[20]:


invalid_binss = [p[0] for p in parsed]
screen_diffs = [p[1] for p in parsed]
screen_behavs = [p[2] for p in parsed]
n_invs = [p[3] for p in parsed]
invalid_bins = pd.concat(invalid_binss, ignore_index = True)
screen_sessions = pd.concat(screen_diffs, ignore_index = True)
screen_behav = pd.concat(screen_behavs, ignore_index = True)
n_inv = pd.concat(n_invs, ignore_index = True)


# Save the output datasets:

# In[21]:


screen_behav.to_pickle('personal/asger/preprocessed_data/screen_behaviour_1m.pkl')
screen_sessions.to_pickle('personal/asger/preprocessed_data/screen_sessions_1m.pkl')
invalid_bins.to_pickle('personal/asger/preprocessed_data/invalid_bins_1m.pkl')
n_inv.to_pickle('personal/asger/preprocessed_data/invalidation_counts_1m.pkl')


# ## Build screen behaviour in class dataset

# Read in the input datasets for this section:

# In[6]:


screen_behav = pd.read_pickle('personal/asger/preprocessed_data/screen_behaviour_15m.pkl')
attend = pd.read_pickle('data/preproc/behavior/attendance_geofence.pkl')
temp_map = cns.get_temporal_context_frame()[['hourbin', 'semester']]


# In[10]:


attend['timebin'] = attend['timestamp_qrtr'].astype(int)
attend = attend.drop('timestamp_qrtr', axis=1)


# Split the screen behaviour into two parts: One with all the timebins, where a given user attended class, and one where the user did not:

# In[12]:


def merge_attendance(screen_behav, attend, hourbin_semester_map) :
    attend = attend.loc[attend.check_attend == 1, :].copy()
    attend = attend.drop_duplicates(subset=['user_idx','timebin'])
    attend['hourbin'] = attend['timebin'] // 3600 * 3600
    attend = attend.merge(hourbin_semester_map, on = 'hourbin', how = 'left')
    attend = attend.drop(['hourbin'], axis = 1)
    merged = screen_behav.merge(attend, on = ['user_idx', 'timebin'], how = 'left')
    inclass = merged[merged.check_attend == 1].copy().drop('check_attend', axis = 1)
    notinclass = merged[merged.check_attend != 1].copy().drop(['check_attend', 'course_number', 'semester'], axis = 1)
    return inclass, notinclass


# Change the semester for the second part of the math course that runs over two semesters. Also, add semester to course numbers to make sure that the resulting string is a unique course id:

# In[16]:


def change_sem(row) :
    if (row['course_number'] == '01005') :
        if (row['semester'] == 'fall_2013') :
            return 'spring 2014'
        elif (row['semester'] == 'fall_2014') :
            return 'spring_2015'
        else :
            return row['semester']
    else :
        return row['semester']


# In[17]:


screen_behav_inclass['semester'] = screen_behav_inclass.apply(change_sem, axis = 1)
screen_behav_inclass['course_num_sem'] = screen_behav_inclass['course_number'] + '_' + screen_behav_inclass['semester']
screen_behav_inclass = screen_behav_inclass.drop('course_number', axis = 1)


# In[139]:


screen_behav_inclass['pause_v1'] = (pd.to_datetime(screen_behav_inclass['timebin'],unit='s').dt.minute==45)
screen_behav_inclass['pause_v2'] = (pd.to_datetime(screen_behav_inclass['timebin'],unit='s').dt.minute==0)


# In[140]:


screen_behav_inclass.to_pickle('personal/asger/preprocessed_data/screen_behaviour_inclass.pkl')
screen_behav_notinclass.to_pickle('personal/asger/preprocessed_data/screen_behaviour_notinclass.pkl')


# ## Build course attention and performance dataset

# Read in the input datasets for this section:

# In[6]:


screen_behav_inclass = pd.read_pickle('personal/asger/preprocessed_data/screen_behaviour_inclass.pkl')
grades = pd.read_pickle('data/preproc/dtu/grades_date.pkl')
grades_alt = pd.read_pickle('data/preproc/dtu/grades_alt.pkl')


# Calculate the attention measures:

# In[7]:


attention = screen_behav_inclass.drop(['timebin'], axis = 1)            .groupby(['user_idx', 'semester', 'course_num_sem'], as_index = False).mean().drop(['sms','fb_post','pause_v1','pause_v2'],axis=1)


# In[9]:


screen_behav_inclass_nopause_v1 = screen_behav_inclass.loc[~screen_behav_inclass['pause_v1'],['user_idx','course_num_sem','screentime','pause_v1']].copy()
screen_behav_inclass_nopause_v1 = screen_behav_inclass_nopause_v1.drop('pause_v1', axis=1).rename(columns={'screentime':'screentime_nopause_v1'})
attention_nopause_v1 = screen_behav_inclass_nopause_v1.groupby(['user_idx', 'course_num_sem'], as_index = False).mean()


# In[10]:


screen_behav_inclass_nopause_v2 = screen_behav_inclass.loc[~screen_behav_inclass['pause_v2'],['user_idx','course_num_sem','screentime','pause_v2']].copy()
screen_behav_inclass_nopause_v2 = screen_behav_inclass_nopause_v2.drop('pause_v2', axis=1).rename(columns={'screentime':'screentime_nopause_v2'})
attention_nopause_v2 = screen_behav_inclass_nopause_v2.groupby(['user_idx', 'course_num_sem'], as_index = False).mean()


# In[11]:


attention = attention.merge(attention_sms, how='left').merge(attention_nopause_v1, how='left').merge(attention_nopause_v2, how='left')


# In[16]:


grades_alt = grades_alt.dropna(subset=['user_idx', 'class_code'])


# In[19]:


grades_alt['course_number'] = grades_alt.class_code.astype(int).astype(str)


# In[20]:


grades_alt = grades_alt[['course_number', 'user_idx', 'grade', 'semester']]


# In[21]:


counts = screen_behav_inclass[['timebin','user_idx','course_num_sem']]         .groupby(['user_idx','course_num_sem'], as_index=False)         .count().rename(columns = {'timebin' : 'measurement_count'})
attention = attention.merge(counts, on = ['user_idx','course_num_sem'], how = 'left')


# Filter out observation from the unrelevant semesters and prepare the grades data to be merge:

# In[22]:


relevant_semesters = ['fall_2013','fall_2014','spring_2014','spring_2015']
attention_filt_1 = attention.loc[attention.semester.isin(relevant_semesters), :]
grades['course_num_sem'] = grades['course_number'] + '_' + grades['semester']
grades = grades.loc[grades.user_idx.isin(attention_filt_1.user_idx.unique()), :]
grades = grades.loc[grades.course_num_sem.isin(attention_filt_1.course_num_sem.unique()), :]
grades_alt['course_num_sem'] = grades_alt['course_number'] + '_' + grades_alt['semester']
grades_alt = grades_alt.loc[grades_alt.user_idx.isin(attention_filt_1.user_idx.unique()), :]
grades_alt = grades_alt.loc[grades_alt.course_num_sem.isin(attention_filt_1.course_num_sem.unique()), :]


# In[24]:


grades['grade'] = grades.grade_num_infer.astype(int).astype(str)


# In[27]:


class smart_dic(dict) :
    def __missing__(self, key):
        return key
grade_map = smart_dic({'00':'0', '02':'2'})


# In[28]:


grades_alt['grade'] = grades_alt.grade.map(grade_map)


# In[29]:


all_grades = pd.concat([grades[['course_num_sem', 'user_idx', 'grade', 'semester']], grades_alt[['course_num_sem', 'user_idx', 'grade', 'semester']]])


# In[30]:


all_grades = all_grades.drop_duplicates()


# In[33]:


def remove_dubs(df) :
    if (len(df)==1) :
        return (df)
    else: 
        if (df['grade'].isin(['EM', 'BE', 'S', 'IB', 'SN',
       'IG']).all()) :
            df.sample(1, random_state=1801)
        else :
            df = df.loc[~(df['grade'].isin(['EM', 'BE', 'S', 'IB', 'SN',
           'IG']))]
            if (len(df)==1) :
                return (df)
            else :
                df = df.loc[~(df['grade'] == '-3'),]
                if (len(df)==1) :
                    return (df)
                else :
                    df = df.loc[~(df['grade'] == '-0'),]
                    if (len(df)==1) :
                        return (df)
                    else :
                        df.sample(1, random_state=1801)


# In[34]:


grouped_grades = [u_df for u,u_df in all_grades.groupby(['user_idx','course_num_sem'])]


# In[36]:


all_grades = pd.concat([remove_dubs(df) for df in grouped_grades], ignore_index=True)


# Merge grades on attention measures:

# In[38]:


attention_w_grades = attention_filt_1.merge(all_grades, on = ['course_num_sem','user_idx','semester'], how = 'inner')


# Save the out dataset:

# In[39]:


attention_w_num_grades = attention_w_grades.loc[attention_w_grades.grade.isin(['-3','0','2','4','7','10','12']),]


# In[40]:


attention_w_num_grades.to_pickle('personal/asger/preprocessed_data/course_attention_performance.pkl')


# ## Build user level control variables

# Read in the input datesets for this section:

# In[67]:


screen_behav = pd.read_pickle('personal/asger/preprocessed_data/screen_behaviour_notinclass.pkl')
grades_primary = pd.read_pickle('data/struct/features/grades_primary.pkl')
grades_highschool = pd.read_pickle('data/struct/features/grades_hs.pkl')
parent_edu = pd.read_pickle('data/struct/features/parent_edu.pkl')
parent_inc = pd.read_pickle('data/struct/features/parent_inc.pkl')
dem = pd.read_pickle('data/struct/features/demographics.pkl')
survey = pd.read_pickle('data/struct/features/survey.pkl')
organization_dtu = pd.read_pickle('data/preproc/dtu/organization.pkl')
user_map = pd.read_pickle('data/preproc/users/all_users.pkl').loc[:,['user_idx','user']]
organization_dtu = organization_dtu.merge(user_map, on='user', how='inner')
temp_context = cns.get_temporal_context_frame()[['hourbin','hour','semester']]


# Calculate the average screen behaviour out of class during daytime:

# In[68]:


def is_day(row) :
    if ((1 <= row['hour']) and (row['hour'] <= 6)) :
        return False
    else :
        return True


# In[69]:


temp_context['day'] = temp_context.apply(is_day, axis = 1)
screen_behav['hourbin'] = screen_behav['timebin'] // 3600 * 3600
screen_behav = screen_behav.merge(temp_context, on = 'hourbin')
screen_behav = screen_behav.loc[screen_behav.day, :]
screen_behav = screen_behav.drop(['timebin','hourbin','hour','day'], axis = 1)


# In[18]:


avr_screen_behav = screen_behav.groupby('user_idx', as_index = False).mean()


# Choose the relevant variables in the background variables datasets:

# In[10]:


grades_primary = grades_primary.reset_index()
grades_highschool = grades_highschool.reset_index()
parent_edu = parent_edu.reset_index()
parent_inc = parent_inc.reset_index()
dem = dem.reset_index()


# In[11]:


avr_screen_behav = avr_screen_behav[['user_idx','screentime','screencount']]
avr_screen_behav = avr_screen_behav.rename(columns = {'screentime' : 'screentime_outofclass', 'screencount' : 'screencount_outofclass'})
psychology = survey[['1_bfi_agreeableness', '1_bfi_conscientiousness', '1_bfi_extraversion', '1_bfi_neuroticism', '1_bfi_openness', '1_locus_of_control','1_ambition','1_self_efficacy']].copy()
psychology['user_idx'] = psychology.index
psychology = psychology.rename(columns={'1_bfi_agreeableness':'agreeableness', '1_bfi_conscientiousness':'conscientiousness', '1_bfi_extraversion':'extraversion', '1_bfi_neuroticism':'neuroticism', '1_bfi_openness':'openness', '1_locus_of_control':'locus_of_control', '1_ambition': 'ambition', '1_self_efficacy':'self_efficacy'})
health = survey[['1_bmi', '1_physical_activity', '1_smoke_freq']].copy()
health = health.rename(columns={'1_bmi':'bmi','1_physical_activity':'physichal_activity', '1_smoke_freq': 'smoke_freq'})
health['user_idx'] = health.index
chosen_grades_highschool = grades_highschool[['user_idx','hs_matematik','hs_gpa']]
chosen_grades_highschool = chosen_grades_highschool.rename(columns={'hs_matematik': 'hs_math'})
chosen_grades_primary = grades_primary[['user_idx', 'elem_matematik_exam','elem_gpa']]
chosen_grades_primary = chosen_grades_primary.rename(columns = {'elem_matematik_exam': 'elem_math'})
parent_edu_max = parent_edu[['user_idx', 'edu_max']]
parent_edu_max = parent_edu_max.rename(columns = {'edu_max':'parent_edu_max'})
parent_inc_mean_max = parent_inc[['user_idx', 'inc_max', 'inc_mean']]
parent_inc_mean_max = parent_inc_mean_max.rename(columns = {'inc_max':'parent_inc_max', 'inc_mean': 'parent_inc_mean'})
dem = dem.drop('immig_desc', axis=1)
organization_dtu = organization_dtu[['user_idx', 'study']]


# In[12]:


merged = avr_screen_behav.merge(chosen_grades_primary, on = 'user_idx', how = 'left')
merged = merged.merge(chosen_grades_highschool, on = 'user_idx', how = 'left')
merged = merged.merge(parent_edu_max, on = 'user_idx', how = 'left')
merged = merged.merge(parent_inc_mean_max, on = 'user_idx', how = 'left')
merged = merged.merge(dem, on = 'user_idx', how = 'left')
merged = merged.merge(psychology, on='user_idx',how='left')
merged = merged.merge(health, on='user_idx',how='left')
merged = merged.merge(organization_dtu, on='user_idx',how='left')


# In[79]:


merged.to_pickle('personal/asger/preprocessed_data/user_level_control_vars.pkl')


# ## Build user-course level control variables

# Read the input datasets for this section:

# In[35]:


screen_behav_ooc = pd.read_pickle('personal/asger/preprocessed_data/screen_behaviour_notinclass.pkl')
screen_behav_inclass = pd.read_pickle('personal/asger/preprocessed_data/screen_behaviour_inclass.pkl')
attend = pd.read_pickle('data/preproc/behavior/attendance_geofence.pkl')
temp_map = cns.get_temporal_context_frame()[['hourbin', 'hour', 'semester']]


# In[36]:


def is_day(row) :
    if ((1 <= row['hour']) and (row['hour'] <= 6)) :
        return False
    else :
        return True


# In[37]:


temp_map['day'] = temp_map.apply(is_day, axis = 1)
screen_behav_ooc['hourbin'] = screen_behav_ooc['timebin'] // 3600 * 3600
screen_behav_ooc = screen_behav_ooc.merge(temp_map, on = 'hourbin')
screen_behav_ooc = screen_behav_ooc.loc[screen_behav_ooc.day, :]
screen_behav_ooc = screen_behav_ooc.drop(['timebin','hourbin','hour','day'], axis = 1)


# In[38]:


temp_map['day'] = temp_map.apply(is_day, axis = 1)
screen_behav_inclass['hourbin'] = screen_behav_inclass['timebin'] // 3600 * 3600
screen_behav_inclass = screen_behav_inclass.merge(temp_map.drop('semester',axis=1), on = 'hourbin')
screen_behav_inclass = screen_behav_inclass.loc[screen_behav_inclass.day, :]
screen_behav_inclass = screen_behav_inclass.drop(['timebin','hourbin','hour','day'], axis = 1)


# In[39]:


avr_screen_behav_ooc_semester = screen_behav_ooc.groupby(['user_idx','semester'], as_index = False).mean()


# In[40]:


avr_screen_behav_inclass_semester = screen_behav_inclass.groupby(['user_idx','semester'], as_index = False).mean()


# Calculate how much of the time each user attended scheduled classtime for the courses, he/she was signed up for:

# In[41]:


attend = attend.rename(columns = {'timestamp_qrtr' : 'timebin'})
attend = attend.loc[~ np.isnan(attend.check_attend), :].copy()
attend['hourbin'] = attend['timebin'] // 3600 * 3600
attend = attend.merge(temp_map, on = 'hourbin', how = 'left')
attend = attend.drop(['hourbin'], axis = 1)
attend['course_num_sem'] = attend['course_number'] + "_" + attend['semester']


# In[42]:


avr_attendance = attend.drop(['course_number','timebin','hour','day'], axis = 1).groupby(['user_idx','course_num_sem','semester'],as_index=False).mean()


# In[43]:


avr_attendance = avr_attendance.rename(columns={'check_attend':'attendance'})
avr_attendance['attendance'] = avr_attendance['attendance']*100


# In[44]:


avr_attendance_semester = attend.drop(['course_number','timebin','hour'], axis = 1).groupby(['user_idx','semester'],as_index=False).mean()


# In[45]:


avr_attendance = avr_attendance.merge(avr_attendance_semester.rename(columns={'check_attend':'attendance_semester'}), on=['user_idx','semester'])


# In[47]:


avr_screen_behav_inclass_semester = avr_screen_behav_inclass_semester.rename(columns={'screentime':'screentime_semester'})
avr_screen_behav_ooc_semester = avr_screen_behav_ooc_semester.rename(columns={'screentime':'screentime_outofclass_semester'})


# In[49]:


usercoursectrls = avr_attendance.merge(avr_screen_behav_ooc_semester[['user_idx','semester','screentime_outofclass_semester']],on=['user_idx','semester'],how='left').merge(avr_screen_behav_inclass_semester[['user_idx','semester','screentime_semester']],on=['user_idx','semester'],how='left')


# In[51]:


usercoursectrls = usercoursectrls.drop(['semester','day'],axis=1)


# In[52]:


usercoursectrls.to_pickle('personal/asger/preprocessed_data/user_course_level_control_vars.pkl')


# ## Build analysis dataset

# Read in the input datasets for this section:

# In[80]:


course_att_perf = pd.read_pickle('personal/asger/preprocessed_data/course_attention_performance.pkl')
user_cont_vars = pd.read_pickle('personal/asger/preprocessed_data/user_level_control_vars.pkl')
user_course_cont_vars = pd.read_pickle('personal/asger/preprocessed_data/user_course_level_control_vars.pkl')


# Merge the control variables in the attention performance dataset:

# In[82]:


analysis = course_att_perf.merge(user_cont_vars, on=['user_idx'], how='left').merge(user_course_cont_vars, on=['user_idx', 'course_num_sem'], how='left')


# In[84]:


analysis_filt1 = analysis.loc[~((analysis.screentime_outofclass == 0) & (analysis.screentime_uavr == 0))].copy()


# In[86]:


analysis_filt2 = analysis_filt1[analysis_filt1['measurement_count']>=40]


# In[88]:


analysis_filt2.to_csv('personal/asger/preprocessed_data/analysis.csv', index=False)


# In[35]:


x = analysis_filt2[['user_idx','semester','course_num_sem', 'screentime_short_ses',
       'screencount_short_ses', 'screentime_long_ses', 'screencount_long_ses',
       'screentime', 'screencount','attendance','grade']]


# In[36]:


x.to_pickle('personal/asger/preprocessed_data/screen_attendance_course_specific.pkl')

