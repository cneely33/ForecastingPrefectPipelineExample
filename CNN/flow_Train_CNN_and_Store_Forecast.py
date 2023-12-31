from prefect import task, flow, get_run_logger
from prefect.server.schemas.states import Failed
import datetime
import numpy as np
import pandas as pd
import os
#saves model as serialized object
from pathlib import Path

from gluonts.evaluation import make_evaluation_predictions, Evaluator
# from gluonts.dataset.split import split

from gluonts.mx.model.seq2seq import MQCNNEstimator
from gluonts.mx import Trainer
from gluonts.dataset.pandas import PandasDataset
# from gluonts.evaluation.metrics import quantile_loss, mase, mse, abs_error

# loads model back
from gluonts.model.predictor import Predictor

### create train, test, and future split
from Models.GluonTS import create_datasets_train_test_future
from Models.GluonTS import extract_forecast_values
from Models.GluonTS import s3_file_trans
from Models.GluonTS import prep_data_for_forecast

from mods.DBA import emailBatchResultStatus as em
from mods.logs import dynamLogging as dl
from mods.secrets import secrets

## contacts to notify
# email_notification_list = ['neely@lipseys.com']

############ forecast parameters ############
## number of periods to predict
prediction_length = 9
## frequency of data; used for seasonality vars
freq = 'W'
### required features ###
date_column = 'date'
target_column = 'quantity'
sku_column = 'sku'
### optional features ###
categorical_columns = ['Place categorical features here']
## features dynamic real
quantitative_columns_known = ['place quant features with known future values here']
## PAST features dynamic real
quantitative_columns_past = ['place quant feature with UNKNOWN future values here']

### metadata
model_name = 'CNN'
flow_name = 'Forecast_SKU_Demand_CNN'
version = '2.0'

########################################################################################################################################
##################################################### Paramaters From Optuna ###########################################################
########################################################################################################################################

## CNN with item attributes added
param_dict = {
    'context_length': 6,
    'learning_rate': 0.0022709042425165175,
    'weight_decay': 0.00043613321370926747, 
    }


def get_start_and_end_date(df, date_column, prediction_length):
    df_end_date = df[date_column].max()
    num_periods_to_append = prediction_length + 1

    ### Frequency hard coded to "W-MON" based on business rule and availabilty within EDW
    future_window = pd.date_range(start=df_end_date, periods=num_periods_to_append, freq="W-MON")[1:]

    start = future_window.min()
    end = future_window.max() + pd.DateOffset(days=6)
    
    return start, end


######################################################################################################################################################
################################################### Train Models with Optimal Paramaters #############################################################
######################################################################################################################################################


def train_model(param_dict,
                static_cardinality,
                dataset,
                prediction_length,
                freq):
    
    
    estimator = MQCNNEstimator(
        prediction_length=prediction_length,
        context_length= param_dict['context_length'] * prediction_length,
        freq=freq,
        use_feat_static_cat=True,
        use_past_feat_dynamic_real=True,
        use_feat_dynamic_real=True,
        cardinality=static_cardinality,
        scaling_decoder_dynamic_feature=True,
          # decoder_mlp_dim_seq =[30],
          channels_seq=[30,30,30],
          dilation_seq=[1,3,5],
          kernel_size_seq=[7,3,3],
          quantiles=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9] ,
        trainer=Trainer(
            ctx="cpu",
            # epochs=1,
            epochs=500,
            learning_rate=param_dict['learning_rate'],
            weight_decay=param_dict['weight_decay'],
            num_batches_per_epoch=100)
      
    )
    
    predictor= estimator.train(dataset)
    
    return predictor

#### Create If True/False Controllers####
@task(task_run_name="Action If True: {value}")
def ActionIfTrue(value):
    return value

@task(task_run_name='Action If False: {value}')
def ActionIfFalse(value):
    return value

#### Create Condition Checks ####
@task(name="Check_Row_Count")
def CheckConditionRowCount(check_value):
    check_value = len(check_value)
    check_value = check_value == 0
    return check_value

@task(name="Expected Value Check")
def CheckConditionExpectedValue(check_value):
    return check_value == True


########### Configure Meta Tasks #########
@task(name="start_logging")
def initialize_log(flow_name):
    ################################
    ######## Initalize Log #########
    logger = dl.logging_func(__file__, flow_name)
    logger_prefect = get_run_logger()
    logger.info("logging on")
    ################################
    return logger

@task(name="load_env_vars")
def initialize_env_vars():
    try:
        secrets.load_env_vars()
    except Exception as err:
         raise Exception(err)


@task(name="Notify Results")
def notify_results(__file__, results, failed):    
    em.emailResults(__file__, results, 0)
    if failed == True:
        err = 'Failed with unexpected results'
        return Failed(message=err)

######################################################################################################################################

############### Historic Transactions ###############
@task(name="Pull data from Redshift", retries=2, retry_delay_seconds=10)
def pull_data(historicRun, week):
    if historicRun == True:
        #  week = 0 gets previous week ending sunday as start; 1 gets 2 weeks prior, etc.
        ### function removed that censors data based on number of weeks passed
        df = prep_data_for_forecast.pull_historic_data(week)
    else:
        df = prep_data_for_forecast.pull_current_data()
    return df



### df_poisson, df_zero_inflated_poisson, df_negaBi, df_zero_inflated_negaBi
@task(name="Create datasets, model, and forecast")
def create_forecast(df,
                    date_column,
                    prediction_length,
                    target_column,
                    sku_column,
                    freq,
                    categorical_columns,
                    param_dict,
                    quantitative_columns_past,
                    quantitative_columns_known):
    
    ## split time series into train, test, and future
    df_list = create_datasets_train_test_future.create_model_datasets(df,
                                                                    date_column,
                                                                    prediction_length,
                                                                    sku_column,
                                                                    freq,
                                                                    categorical_columns,)
    
    ## Create gluonts Pandas dataframe required for model
    test_datasets, future_datasets, static_cardinality = prep_data_for_forecast.CNN_ts_datasets(df_list,
                                                                                                   freq, target_column,
                                                                                                   sku_column, categorical_columns,
                                                                                                   quantitative_columns_past,
                                                                                                   quantitative_columns_known)
    ## train model and create predictor object
    predictor = train_model(param_dict,
                    static_cardinality,
                    test_datasets,
                    prediction_length,
                    freq)
    
    ## forecast new data
    forecast_it, ts_it = make_evaluation_predictions(dataset=future_datasets,
                                                     predictor=predictor,
                                                     ##TODO experiemnt with num_samples
                                                     num_samples=100,  # number of sample paths we want for evaluation
                                                     )
    ## unpack forecast
    forecasts = list(forecast_it)
    
    return forecasts

## optional stored saved model locally; possible to store in S3
###########################################################
    # project_path = r"""C:\Users\%username%\Setup"""
    # predictor_folder = 'negative_binomial'
    # predictor_path = os.path.join(project_path, predictor_folder)
    ## save model
    # predictor_nb.serialize(Path(predictor_path))
    ## load model
    # predictor_nb = Predictor.deserialize(Path(predictor_path))
###########################################################
########################################################### 
###########################################################

@task(name="Pull forecast quantiles")
def get_forecast_values(forecasts, prediction_length):
    forecasts_df = extract_forecast_values.pull_forecast_values(forecasts, prediction_length)
    return forecasts_df

@task(name="Store forcast in S3", retries=2, retry_delay_seconds=10)
def store_forecast_values(forecasts_df_future, model_name, version, target_column, start, end):
    key = s3_file_trans.store_in_s3(forecasts_df_future, model_name, version, target_column, start, end)
    return key
######################################################################################################################################
######################################################################################################################################

@flow(log_prints=True, name=flow_name)
def flow_generate_cnn_forecast(
        historicRun: bool = False,
        weeksPrior: list = [0]
        ):     
    
    ############ Start Log ############
    env_vars = initialize_env_vars.submit()
    
    
    start_log = initialize_log.submit(flow_name, wait_for=[env_vars])
    
    for i in weeksPrior:
        week = i
        print("Running forecast for week {week}".format(week=week))
        
        ### Pull data from EDW
        df = pull_data(historicRun, week, wait_for=[start_log, env_vars])
        
        ### get start and end date of forecast
        start, end = get_start_and_end_date(df, date_column, prediction_length)
        
        ### Generate Forecast ### 
        forecasts_future = create_forecast(df,
                                        # df_poisson,
                                        date_column,
                                        prediction_length,
                                        target_column,
                                        sku_column,
                                        freq,
                                        categorical_columns,
                                        param_dict,
                                        quantitative_columns_past,
                                        quantitative_columns_known,
                                          wait_for=[df]
                                         )
        
        ## extra relevant forecast values
        forecasts_df_future = get_forecast_values(forecasts_future, prediction_length, wait_for=[forecasts_future])
        
        ### store results in S3 ###
        s3_key = store_forecast_values(forecasts_df_future, model_name, version, target_column, start, end, wait_for=[forecasts_df_future])
        
    return s3_key

if __name__ == "__main__":
    flow_generate_cnn_forecast(False, [0])
