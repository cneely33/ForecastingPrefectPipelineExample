######################################################
############## Pull Forecast Values Only #############
######################################################
def pull_forecast_values(forecasts, prediction_length):
    import pandas as pd
    ############################################################################################################################
    #### Get forecasted Values from forecast generator object that was creadted with make_evaluation_predictions and list() ####
    ############################################################################################################################

    forecast_df_list = []
    for forecast_entry in forecasts:
        
        data_dict = {
                    'forecast_item_id': [forecast_entry.item_id] * prediction_length,
                   # 'forecast_mean':  forecast_entry.mean,
                    'forecast_quantile_1':  forecast_entry.quantile(0.1),
                    'forecast_quantile_2':  forecast_entry.quantile(0.2),
                   'forecast_quantile_3':  forecast_entry.quantile(0.3),
                    'forecast_quantile_4':  forecast_entry.quantile(0.4),
                   'forecast_quantile_5':  forecast_entry.quantile(0.5),
                    'forecast_quantile_6':  forecast_entry.quantile(0.6),
                    'forecast_quantile_7':  forecast_entry.quantile(0.7),
                   'forecast_quantile_8':  forecast_entry.quantile(0.8),
                    'forecast_quantile_9':  forecast_entry.quantile(0.9),
                    # 'forecast_start_date':  forecast_entry.start_date
                    }
        
        data_df = pd.DataFrame(data_dict, index=forecast_entry.index)
        
        forecast_df_list.append(data_df)
        
    forecasts_df = pd.concat(forecast_df_list) 
    forecasts_df.reset_index(inplace=True)
    
    forecasts_df['start_forecast'] = forecasts_df['index'].apply(lambda x: x.start_time)
    forecasts_df['end_forecast'] = forecasts_df['index'].apply(lambda x: x.end_time)
    
    forecasts_df.drop(columns=['index'], inplace=True)
        
    return forecasts_df 
