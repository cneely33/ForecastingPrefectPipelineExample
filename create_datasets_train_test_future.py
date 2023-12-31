      
################################################
################ add holiday ###################
################################################
from pandas.tseries.holiday import Holiday, USMemorialDay, USLaborDay, USThanksgivingDay, \
     AbstractHolidayCalendar, nearest_workday
import pandas as pd
import numpy as np
     
class CustomCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('New Years Day', month=1, day=1, observance=nearest_workday),
        USMemorialDay,
        Holiday('July 4th', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas Day', month=12, day=25, observance=nearest_workday)
        ] 

###############################################################################
###############################################################################
def create_model_datasets(df, date_column, prediction_length, sku_column, freq, categorical_columns):
    
###############################################################################
################### Pad time seires with future datapoints ####################
###############################################################################
    def create_future_ts(df):
            
        df_end_date = df[date_column].max()
        num_periods_to_append = prediction_length + 1
        
        ### Frequency hard coded to "W-MON" based on business rule and availabilty within EDW
        future_window = pd.date_range(start=df_end_date, periods=num_periods_to_append, freq="W-MON")[1:]
        future_window.freq = None
        
        future_df = pd.DataFrame(future_window, columns=[date_column])
        future_df['future_ind'] = True
        
        df_unique_items = df[[sku_column]].drop_duplicates()
        future_df = df_unique_items.merge(future_df, how='cross')
        
        future_df = pd.concat([df, future_df])
        
        return future_df
    
    df = create_future_ts(df)
    
################################################
################ add holiday ###################
################################################

    def create_holiday_ind(df):
        ##TODO: create path to join holidayson dataset at the day level
        cal = CustomCalendar()
        df_end_date = df[date_column].max()
        df_start_date = df[date_column].min()
        
        holidays = pd.DataFrame(cal.holidays(start=df_start_date, end=df_end_date).to_pydatetime(),
                                columns=['holiday_dates'])
        
        ## for joining to weekly interval
        holidays['holiday_week_start'] = holidays['holiday_dates'].dt.to_period(freq).apply(lambda r: r.start_time)
        holidays['holiday_ind'] = 1
        
        ## join holiday indicator on the start of the week
        df = df.merge(holidays, left_on=date_column, right_on='holiday_week_start', how='left')
        df.drop(columns=['holiday_dates', 'holiday_week_start'], inplace=True)
        df['holiday_ind'].fillna(0, inplace=True)
        
        return df
    
    df = create_holiday_ind(df)
    

    ########################################################################
    ### get the date for start of training period
    df_dates_unique = np.sort(df[date_column].unique())
    
    # cutoff_date = df_dates_unique[-prediction_length * 2:].min()
    cutoff_date_test = df_dates_unique[-prediction_length:].min()
    
    ########################################################################

    ####### set index to date and sort #######
    df.sort_values(by=[sku_column, date_column], axis=0, inplace=True)
    df = df.set_index(date_column)
    
    ####### convert Static Feature columns to categorical datatype #######
    for col in categorical_columns:
        df[col] = df[col].astype('category')

    #######################################################
    ############ Create Train Test Splits #################
    #######################################################
    # train = df[df.index < cutoff_date]
    # target_window_train = train.index[-prediction_length:]
    
    test = df[df.index < cutoff_date_test]
    # target_window_test = test.index[-prediction_length:]
    
    future_df = df
    # future_window = future_df.index[-prediction_length:]
    
    return [test, future_df]