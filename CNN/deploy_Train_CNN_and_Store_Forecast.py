from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
### import flow function
import flow_Train_CNN_and_Store_Forecast

from mods.secrets import secrets
# from os import getenv

def deploy():
    secrets.load_env_vars()
    deployment = Deployment.build_from_flow(
        ### set Flow function
        flow=flow_Train_CNN_and_Store_Forecast.flow_generate_cnn_forecast,
        ### set Flow Name
        name="Forecast_SKU_Demand",
        ### set Schedule
        schedule=(CronSchedule(cron="30 00 * * 6", timezone="America/Chicago")),
        parameters={
                    'historicRun': False,
                    "weeksPrior": [0],
                    },
        infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
        work_pool_name="ForecastWorkQueue",
        work_queue_name="CNN",
        tags=['Model', 'Forecast'],
    )
    deployment.apply()

if __name__ == "__main__":
    deploy()
    
