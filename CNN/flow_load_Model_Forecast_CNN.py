from prefect import task, flow, get_run_logger
from mods.DBA import emailBatchResultStatus as em
from mods.logs import dynamLogging as dl
from mods.secrets import secrets

from Models.GluonTS import pull_extract_from_s3
from Models.GluonTS.CNN import load_landing_tables as land
from Models.GluonTS.CNN import load_stage_tables as stage
from Models.GluonTS.CNN import load_prod_tables as prod
from Models.GluonTS import archive_extracts_s3

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
def notify_results(__file__, results):    
    em.emailResults(__file__, results, 0)
        
@task(name="Copy from s3 to Landing")
def s3_to_redshift_data_land(testProd_ind):
    result = land.load_landing_tables(testProd_ind)
    return result
 
@task(name="Blend Data and Load to Stage")
def blend_data_and_stage(testProd_ind):
    result = stage.load_stage_tables(testProd_ind)
    return result

@task(name="Load Data into Prod")
def load_data_into_prod(testProd_ind):
    result = prod.load_prod_tables(testProd_ind)
    return result         
        
@task(name="Pull Key List from s3", retries=2, retry_delay_seconds=5)
def pull_s3_obj_list():
    
    prefix = 'PROD/STG/Models/DemandForecast/CNN'
    obj_list = pull_extract_from_s3.pull_s3_objects(prefix)
    
    a_and_d_key_list = []
    for i in obj_list:
        a_and_d_key_list.append(i.key)
    
    return a_and_d_key_list    

@task(name='Archive Extracts in S3')  
def archive_extracts(list_of_dirs):
    bucket='my_s3_bucket'
    
    ## after all success move all extract to archive folder within their current folder
    result = archive_extracts_s3.s3_archive_extracts(list_of_dirs, bucket)
    
    if result == True:
        archived = 'Archive Successful'
    else:
        #'Archive Failed'
        archived = result
    
    return archived

@flow(log_prints=True, name='Load CNN Forecast',)
def load_model_forecast_cnn(testProd_ind: str = 'Prod'):  
    
    env_vars = initialize_env_vars.submit()
    
    flow_name = 'Load CNN Forecast'
    start_log = initialize_log.submit(flow_name, wait_for=[env_vars])
    
    load_landing_result = s3_to_redshift_data_land.submit(testProd_ind, wait_for=[start_log])
     
    load_stage_result = blend_data_and_stage.submit(testProd_ind, wait_for=[load_landing_result])
    
    load_data_layer = load_data_into_prod.submit(testProd_ind, wait_for=[load_stage_result])
    
    s3_obj_key_list = pull_s3_obj_list(wait_for=[load_data_layer])
    
    ## Archive Extracts in S3
    archived_extracts = archive_extracts(s3_obj_key_list, wait_for=[s3_obj_key_list])
    
    ## Send Status of Batch Job 
    notify_results.submit(__file__, archived_extracts, wait_for=[archived_extracts])
        
        
if __name__ == "__main__":
    load_model_forecast_cnn()