from prefect.deployments import Deployment
# from prefect.server.schemas.schedules import CronSchedule
### import flow function
from Models.GluonTS.CNN.flow_load_Model_Forecast_CNN import load_model_forecast_cnn
from mods.secrets import secrets
from os import getenv

def deploy():
    secrets.load_env_vars()
    deployment = Deployment.build_from_flow(
        ### set Flow function
        flow=load_model_forecast_cnn,
        ### set Flow Name
        name="Forecast_SKU_Demand",
        ### set Schedule
        parameters={"testProd_ind": getenv("Environment")},
        infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
        work_pool_name=getenv("work_pool"),
        work_queue_name=getenv("work_queue"),
        tags=['ETL', 'Model', 'Forecast'],
    )
    deployment.apply()

if __name__ == "__main__":
    deploy()
    
