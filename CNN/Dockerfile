# syntax=docker/dockerfile:1.2

FROM python:3.8.2

WORKDIR /app

## Copy requirements.txt
COPY ForecastingPrefectPipelineExample/CNN/requirements.txt .
RUN pip install -r requirements.txt

###install package to covert script to unix format
RUN apt-get update && apt-get install -y \
    dos2unix

## copy files over
COPY ForecastingPrefectPipelineExample/ ForecastingPrefectPipelineExample/
## copy required modules over
COPY mods/ mods/

## place deploy and flow script in parent folder
COPY ForecastingPrefectPipelineExample/CNN/flow_Train_CNN_and_Store_Forecast.py .
COPY Models/GluonTS/CNN/deploy_Train_CNN_and_Store_Forecast.py .

ENV PORT 8888
EXPOSE $PORT

# convert prefect startup script to unix
RUN dos2unix Models/GluonTS/prefect_login.sh

## start prefect worker, assign to pool and work queue 
CMD prefect worker start --pool "ForecastWorkQueue" --work-queue "CNN"

## keep container running to access terminal when debugging
#ENTRYPOINT ["tail", "-f", "/dev/null"]