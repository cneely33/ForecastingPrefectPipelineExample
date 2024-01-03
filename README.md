# Forecasting Prefect Pipeline Example

Boilerplate example of generating a quantile based forecast with GluonTS and managing deployment with Prefect
Prefect Action used to trigger flow_load_Model_Forecast_CNN.py after flow_Train_CNN_and_Store_Forecast.py successfully runs

Modifications needed to run:
1. input dataset at the SKU and interval level with prep_data_for_forecast module
2. configure DB connection and set target schema
3. set environment variables to allow connection to AWS assets
