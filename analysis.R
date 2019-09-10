setwd('H:/workdata/705805')

library(tidyverse)
library(stringr)
library(gridExtra)
library(stargazer)
library(lmtest)
library(plm)
library(splines)
library(reporttools)

##########################################################################################################
###### Regression table reporting ########################################################################
##########################################################################################################

report <- function(mods, type, omit=NULL, p=NULL, se=NULL, 
                   dependent=NULL, varnames=NULL, ci=T)  {
  stargazer(mods, 
            omit=c('Constant', 
                   regex('user_idx.*'), 
                   regex('course_num.*'), 
                   regex('study.*'),
                   regex('semesterspring.*'),
                   regex('semesterfall.*'),
                   omit), 
            omit.stat=c('ser','f'), 
            type=type,
            p=p,
            se=se,
            ci=ci,
            dep.var.caption = dependent,
            covariate.labels = varnames,
            digits=2
            #star.cutoffs = c(0.05,0.01)
  )
}

report_rob <- function(mods, type, omit=NULL, p=NULL, 
                       se=NULL, dependent=NULL, varnames=NULL,
                       ci=T) {
  report(mods[['mods']], type=type, omit=omit, p=mods[['rob_ps']], se=mods[['rob_ses']], dependent = dependent, varnames = varnames, ci=ci)
}

##########################################################################################################
###### Function for building model formulas ##############################################################
##########################################################################################################

build_forms <- function(response, 
                        main_preds, 
                        controls, 
                        rand_effects = NULL, 
                        accumulate_controls = F,
                        add_only_ctrls = F,
                        only_full_mod = F,
                        no_full_mod = F,
                        remove = NULL) {
  controls_blocks <- map(controls, function(c) str_c(c, collapse = ' + '))
  all_controls <- list(str_c(controls_blocks, collapse = ' + '))
  if (accumulate_controls) {
    controls_blocks <- map(1:length(controls_blocks), 
                           function(i) str_c(controls_blocks[1:i], collapse = ' + '))
  }
  else {
    controls_blocks <- c(controls_blocks, all_controls)
  }
  preds <- controls_blocks
  if (!is.null(main_preds)) {
    if (length(main_preds)>1){
      main_preds <- str_c(main_preds, collapse = ' + ')
    } 
    preds <- map(controls_blocks, function(c) str_c(main_preds, ' + ', c))
    preds <- c(list(main_preds), preds)
  }
  if (add_only_ctrls) {
    preds <- c(head(preds, -1), all_controls, tail(preds, 1))  
  }
  if (!is.null(remove)) {
    preds <- c(head(preds, -1), 
               str_replace_all(tail(preds, 1), remove, ''), 
               tail(preds, 1))
  }
  if (!is.null(rand_effects)) {
    if (length(rand_effects)>1) {
      rand_effects <- str_c(rand_effects, collapse = ' + ')
    }
    preds <- map(preds, function(ps) str_c(ps, ' + ', rand_effects))
  }
  forms <- map(preds, function(ps) str_c(response, ' ~ ', ps))
  if (only_full_mod) {
    forms[[length(forms)]]
  }
  else if (no_full_mod) {
    forms[1:(length(forms)-1)]
  }
  else {
    forms
  }
}

#########################################################################################
###### Load and Filter Data #############################################################
#########################################################################################

course_ind_df <- read_csv('personal/asger/preprocessed_data/analysis.csv') %>% 
  filter(is.element(grade, c('-3','0','2','4','7','10','12')),
         !is.na(attendance)) %>% 
  mutate(grade = as.integer(grade)) %>% 
  mutate(user_idx = factor(user_idx),
         semester = factor(semester, levels=c('fall_2013','spring_2014','fall_2014','spring_2015')),
         course_num=str_remove(course_num_sem,'_.*'),
         year=ifelse(is.element(semester,c('fall_2013','spring_2014')),'2013/2014','2014/2015'),
         user_idx_sem = str_c(user_idx,semester),
         smoker = as.integer(smoke_freq<3),
         skipping = 100 - attendance,
         skipping_semester = 100 - 100*attendance_semester,
         skipping_uavr = 100 - 100*attendance_uavr,
         parent_inc_mean = parent_inc_mean / 10000,
         screentime_ratio = screentime / screentime_outofclass) %>%
  select(-screentime_short_ses,
         -screencount_short_ses, -screencount_long_ses,
         -screencount, -screencount_outofclass,
         -elem_gpa, -elem_math,-hs_math, -parent_inc_max) %>% 
  filter(!is.na(hs_gpa), !is.na(parent_edu_max), !is.na(parent_inc_mean)) %>% 
  mutate(screentime=screentime/100,
         screentime_outofclass=screentime_outofclass/100,
         skipping=skipping/100,
         screentime_nopause_v1=screentime_nopause_v1/100,
         screentime_nopause_v2=screentime_nopause_v2/100)

sem_course_count <- course_ind_df %>% group_by(user_idx,semester) %>% count() %>% rename(n_courses_sem=n)
course_ind_df <- course_ind_df %>% left_join(sem_course_count) %>% filter(n_courses_sem>1)

filter_sparse_groups <- function(df, threshold) {
  continue <- T
  while (continue) {
    before <- nrow(df)
    users_pr_course <- df %>%
      group_by(course_num_sem) %>%
      summarise(n_u = n())
    df %<>%
      left_join(users_pr_course, by = "course_num_sem") %>%
      filter(n_u>=threshold) %>%
      select(-n_u)
    courses_pr_user <- df %>%
      group_by(user_idx) %>%
      summarise(n_c = n())
    df %<>%
      left_join(courses_pr_user, by = 'user_idx') %>%
      filter(n_c>=threshold) %>%
      select(-n_c)
    after <- nrow(df)
    continue <- after < before
  }
  rm(courses_pr_user, users_pr_course, after, before, continue)
  df
}

course_ind_df <- course_ind_df %>%
  filter_sparse_groups(2)

ind_avr_df <- course_ind_df %>%
  group_by(user_idx) %>%
  summarise_if(is.numeric, mean) %>%
  rename(gpa_den = grade) %>%
  ungroup() %>%
  left_join(course_ind_df %>% select(user_idx, study) %>% distinct(),
            by='user_idx') %>%
  mutate(male = factor(male),
         smoker = factor(smoker),
         screentime_ratio = screentime_uavr / screentime_outofclass)


###########################################################################################
###########################################################################################
########################### MAIN ANALYSIS #################################################
###########################################################################################
###########################################################################################

###########################################################################################
###### Descriptive statistics #############################################################
###########################################################################################

name_scheme <- function(data, type=c('contcat','cont','cat')) {
  if (type=='contcat') {
    return (rename(data, 'High school grade average' = hs_gpa, 'Parents max years of education' = parent_edu_max,
                   'Parents mean income' = parent_inc_mean, 'Age' = age, 'Agreeableness' = agreeableness,
                   'Conscientiousness' = conscientiousness, 'Extraversion' = extraversion, 'Neuroticism' = neuroticism,
                   'Openness' = openness, 'Locus of control' = locus_of_control, 'BMI' = bmi, 'Male' = male, 'Smoker' = smoker))
  }
  if (type=='cont') {
    return (rename(data, 'High school grade average' = hs_gpa, 'Parents max years of education' = parent_edu_max,
                   'Parents mean income' = parent_inc_mean, 'Age' = age, 'Agreeableness' = agreeableness,
                   'Conscientiousness' = conscientiousness, 'Extraversion' = extraversion, 'Neuroticism' = neuroticism,
                   'Openness' = openness, 'Locus of control' = locus_of_control, 'BMI' = bmi))
  }
  if (type=='cat') {
    return (rename(data, 'Male' = male, 'Smoker' = smoker))
  }
}


grouped_quantile <- function(obs, q, n) {
  obs <- sort(obs)
  groups <- head(rep(1:ceiling(length(obs)/n), each=n), length(obs))
  return (data_frame(obs=obs, group=groups) %>%
            group_by(group) %>%
            summarise(obs = mean(obs)) %>%
            .$obs %>%
            quantile(q))
}

descstatscont <- list('min'=function(obs) grouped_quantile(obs, 0, 5),
                      '1st quartile' = function(obs) grouped_quantile(obs, .25, 5),
                      'median' = function(obs) grouped_quantile(obs, .5, 5),
                      '3rd quartile' = function(obs) grouped_quantile(obs, .75, 5),
                      'max' = function(obs) grouped_quantile(obs, 1, 5))

tableContinuous(ind_avr_df %>% select(age, hs_gpa, parent_edu_max, parent_inc_mean,
                                      agreeableness, conscientiousness, extraversion, neuroticism,
                                      openness, locus_of_control, bmi) %>%
                  name_scheme(type='cont') %>%
                  as.data.frame(),
                stats = descstatscont)

tableNominal(ind_avr_df %>% select(male, smoker) %>%
               name_scheme(type='cat') %>%
               as.data.frame(),
             cumsum = FALSE)

############################################################################################
###### Marginal distributions of behavioral variables ######################################
############################################################################################

makemargfig <- function(data, var, xlabel, xlim, xbreaks) {
  gqs <- grouped_quantile(data[[var]], c(.25,.5,.75), 5)
  data %>%
    ggplot(aes_string(var)) +
    geom_density(fill='grey') +
    geom_vline(xintercept = gqs,
               linetype='dashed',
               color='blue') +
    xlab(xlabel) +
    ylab('Density') +
    scale_x_continuous(limits = xlim, breaks=xbreaks) +
    theme(axis.text.y = element_blank(),
          axis.ticks.y = element_blank(),
          axis.title.y = element_blank(),
          axis.text.x = element_text(size=13),
          axis.title.x = element_text(size=14))
}

grid.arrange(grobs=list(makemargfig(course_ind_df, 'screentime', 
                                    'A: Smartphone use in-class', 
                                    c(0,.42), seq(0,.4,0.1)),
                        makemargfig(course_ind_df, 'skipping', 
                                    'B: Class-skipping', 
                                    c(0,1), seq(0,1,0.2)),
                        makemargfig(ind_avr_df, 'screentime', 
                                    'C: Average smartphone use in-class', 
                                    c(0,.42), seq(0,.4,0.1)),
                        makemargfig(ind_avr_df, 'skipping', 
                                    'D: Average class-skipping', 
                                    c(0,1), seq(0,1,.2))),
             nrow=2)

###########################################################################################
###### Define covariates ##################################################################
###########################################################################################

socioeco <- c('parent_inc_mean', 
              'parent_edu_max')
demographic <- c('age',
                 'male')
past_perf <- c('hs_gpa')
psychological <- c("agreeableness", 
                   "extraversion", 
                   "neuroticism", 
                   "openness",
                   "conscientiousness",
                   "locus_of_control")
health <- c('bmi',
            'smoker')
ctrls <- c(demographic,
           socioeco,
           health,
           psychological)

###########################################################################################
###### Cross-sectional models #############################################################
###########################################################################################

make_mods <- function(df, resp, preds, crtls, accum_controls, no_full_mod) {
  mods <- map(build_forms(resp, 
                          preds,
                          crtls, accumulate_controls = accum_controls, no_full_mod=no_full_mod),
              function(f) lm(f, data = df, singular.ok = F))
  mods_rob <- map(mods, function(fm) coeftest(fm, vcov = vcovHC(fm, type='HC3')))
  ses_rob <- map(mods_rob, function(x) x[,2])
  ps_rob <- map(mods_rob, function(x) x[,4])
  list('mods'=mods, 'rob_ses'=ses_rob, 'rob_ps'=ps_rob)
}

make_mods_iter <- function(df, resp, preds_list, crtls, accum_controls, no_full_mod) map(preds_list, function(preds) make_mods(df,resp,preds,crtls,accum_controls, no_full_mod))

static_mods <- make_mods_iter(ind_avr_df, 'gpa_den',
                              list(c('screentime', 'screentime_outofclass'),c('screentime'),c('screentime_outofclass')),
                              list(ctrls, 'skipping', past_perf),
                              T,F)

report_rob(static_mods[[1]], 'text')

static_mods_scrt <- make_mods(ind_avr_df %>% 
                                select(screentime,
                                       screentime_outofclass, skipping,
                                       hs_gpa, age, male,
                                       parent_inc_mean, parent_edu_max,
                                       bmi,smoker, agreeableness, 
                                       extraversion, neuroticism, openness,
                                       conscientiousness, locus_of_control) %>% 
                                mutate_if(is.numeric,function(col)(col-mean(col))/sd(col)), 
                              'screentime',
                              NULL,
                              list('screentime_outofclass','skipping','hs_gpa',
                                   demographic,socioeco,health,psychological),
                              F,F)

report_rob(static_mods_scrt,'text')

#########################################################################################
###### Panel models #####################################################################
#########################################################################################

course_mods_scrt <- make_mods(course_ind_df, 'grade',
                              c('screentime','skipping'),
                              list('user_idx',
                                   'user_idx_sem',
                                   'course_num',
                                   c('user_idx','course_num')),
                              F,T)
report(course_mods_scrt$mods,'text')

#########################################################################################
#########################################################################################
###### Supplemental Online Material #####################################################
#########################################################################################
#########################################################################################

#########################################################################################
###### Default turn off time ############################################################
#########################################################################################

screensessionsDF <- read_csv('personal/asger/preprocessed_data/screen_sessions.csv')

screensessionsDF %>%
  ggplot(aes(timediff)) +
  geom_density(fill='grey') +
  geom_vline(xintercept = c(10,30),
             color='blue',
             linetype='dashed') +
  xlab('Length of screen session') +
  xlim(c(0,300))

#########################################################################################
###### Cross sectional with studies #####################################################
#########################################################################################

static_mods_study <- 
  make_mods(ind_avr_df, 'gpa_den',
            c('screentime', 'screentime_outofclass','study'),
            list(ctrls, 'skipping', past_perf),
            T,F)

report_rob(static_mods_study,'text')

#########################################################################################
###### Panel models, attendance heterogeneity ###########################################
#########################################################################################


hs_gpa_med <- median(ind_avr_df$hs_gpa)
skipping_med <- median(course_ind_df$skipping)
course_ind_df <- 
  course_ind_df %>% 
  mutate(skipping_top = skipping > skipping_med,
         hs_gpa_top = hs_gpa > hs_gpa_med)

course_mods_scrt_skiprob <- make_mods(course_ind_df, 'grade',
                                      c('screentime*skipping_top'),
                                      list('user_idx',
                                           'user_idx_sem',
                                           'course_num',
                                           c('user_idx','course_num')),
                                      F,T)
report(course_mods_scrt_skiprob$mods, 'text')

fe_res_info <- function(df,resp,fes) {
  mods <- map(fes, function(fe) summary(lm(str_c(resp,'~',fe),data=df)))
  res <- map(mods,function(mod) mod$residuals)
  r2s <- map(mods,function(mod) mod$r.squared)
  res <- matrix(data=unlist(res),ncol=length(fes))
  colnames(res) <- fes
  names(r2s) <- fes
  list('res'=as_data_frame(res),'r2s'=r2s)
}

fe_res_infos <- function(df,resps,fes) {
  res <- map(resps, function(resp) fe_res_info(df,resp,fes))
  names(res) <- resps
  return(res)
}

ress <- fe_res_infos(course_ind_df,c('grade','screentime','skipping'),list('user_idx','user_idx_sem','course_num',c('user_idx','course_num')))

map(ress,function(r)r$r2s)

#########################################################################################
###### Nonlinear models #################################################################
#########################################################################################

screentime_uavr_knots <- quantile(ind_avr_df$screentime_uavr, c(0.25,0.5,0.75))
thislinmod <- lm(gpa_den ~ screentime_uavr + screentime_outofclass + hs_gpa + skipping + bmi + smoker + conscientiousness + locus_of_control + age + male + parent_inc_mean + parent_edu_max + agreeableness + extraversion + neuroticism + openness, data=ind_avr_df)
thisnonlinmod <- lm(gpa_den ~ ns(screentime_uavr, knots = screentime_uavr_knots) + screentime_outofclass + hs_gpa + skipping + bmi + smoker + conscientiousness + locus_of_control + age + male + parent_inc_mean + parent_edu_max + agreeableness + extraversion + neuroticism + openness, data=ind_avr_df)
anova(thislinmod, thisnonlinmod)
AIC(thislinmod)
AIC(thisnonlinmod)

screentime_knots <- quantile(course_ind_df$screentime, c(0.25,0.5,0.75))
thislinmodfe <- lm(grade ~ screentime + skipping + user_idx + course_num_sem, data=course_ind_df)
thisnonlinmodfe <- lm(grade ~ ns(screentime, knots=screentime_knots) + skipping + user_idx + course_num_sem, data=course_ind_df)
anova(thislinmodfe, thisnonlinmodfe)
AIC(thislinmodfe)
AIC(thisnonlinmodfe)

#########################################################################################
###### Breaks between classes ###########################################################
#########################################################################################

static_mods_nobreaks <- make_mods_iter(ind_avr_df, 'gpa_den',
                                    list(c('screentime_nopause_v1', 'screentime_outofclass'),c('screentime_nopause_v2', 'screentime_outofclass')),
                                    list(ctrls, 'skipping', past_perf),
                                    T,F)

report_rob(static_mods_nobreaks[[1]], 'text')
report_rob(static_mods_nobreaks[[2]], 'text')

course_mods_scrt_nobreaks <- make_mods_iter(course_ind_df, 'grade',
                              list(c('screentime_nopause_v1','skipping'),c('screentime_nopause_v2','skipping')),
                              list('user_idx',
                                   'user_idx_sem',
                                   'course_num',
                                   c('user_idx','course_num')),
                              F,T)
report(course_mods_scrt[[1]]$mods,'text')
report(course_mods_scrt[[2]]$mods,'text')



