################################################################################
########################### Create Forecast Eval ###############################
################################################################################
def eval_forecast(prediction_length, 
                  ### GluonTS Datset with formatted data
                  test_datasets, 
                  ## dataframe containing original data with 'future' values
                  df, 
                  ### dataframe containing forecasted values
                  forecasts_df):
    
    import math
    from gluonts.evaluation.metrics import quantile_loss, mase, mse, abs_error
    ##TODO: change hard code of years for seasonal window
    start_season = -52 - prediction_length
    end_season = -52
    
    #TODO: get these values elsewhere
    dataset_test_dict = {}
    for i in test_datasets:
        
        i["item_id"]
        ### get the actual values from the target series 
        current = i["target"][-prediction_length:]
        ### get the previous value to compare to current value
        previous_value = i["target"][:-1][-prediction_length:]
        ### get values from the previous season to compare to current value
        seasonal_naive = i["target"][start_season:end_season]
        ### get the previous prediction windows values to compare to current value
        previous_window_values = i["target"][:-prediction_length][-prediction_length:]
        
        naive_dict = {
            'actual_value':current,
            'pervious_value':previous_value,
            'seasonal_value': seasonal_naive,
            'previous_window_value': previous_window_values
            }
        
        i.update(naive_dict)
     
        dataset_test_dict[i['item_id']] = i
        
        
    DataFrameDict_test_baseline = {}
    for item_id, gdf in df.groupby('itemno'):
        
        if len(gdf) < prediction_length * 2:
            gdf = pd.concat([
                            pd.DataFrame(0, 
                                         columns=gdf.columns,
                                         index=np.arange(0, prediction_length * 2 - len(gdf)))
                              ,gdf
                              ])
            
        target_series = gdf[target_column][-prediction_length:].fillna(0)
        train_series = gdf[target_column][:-prediction_length].fillna(0)
        mean_fill = train_series.mean()
        
        ###### get eval metrics for baseline data
        ######
        previous_value = gdf[target_column][:-1][-prediction_length:].fillna(mean_fill)
        previous_values = train_series.append(previous_value).values
        gdf['qty_previous_time_step'] = previous_values
        ######
        forecast_series = previous_value.values
        rmse_previous_value = math.sqrt(mse(target_series, forecast_series))
        mse_previous_value = mse(target_series, forecast_series)
        abs_error_previous_value = abs_error(target_series, forecast_series)
        
        ######
        last_prediction_window = gdf[target_column][:-prediction_length][-prediction_length:].fillna(mean_fill)
        last_prediction_window_values = train_series.append(last_prediction_window).values
        gdf['qty_previous_window'] = last_prediction_window_values
        ######
        forecast_series = last_prediction_window.values
        rmse_last_pred_window = math.sqrt(mse(target_series, forecast_series))
        mse_last_pred_window = mse(target_series, forecast_series)
        abs_error_last_pred_window = abs_error(target_series, forecast_series)
        
        ######
        seasonal_naive = gdf[target_column][start_season:end_season].fillna(0)
        ## if there is not enough data for seasonal forecast use last predi window as values
        if len(seasonal_naive) == prediction_length:
            seasonal_naive_values = train_series.append(seasonal_naive)
            forecast_series = seasonal_naive.values
        else:
            seasonal_naive_values = train_series.append(last_prediction_window)
            forecast_series = last_prediction_window.values
            
        gdf['qty_previous_season'] = seasonal_naive_values.fillna(0).values
        rmse_seasonal_naive = math.sqrt(mse(target_series, forecast_series))
        mse_seasonal_naive = mse(target_series, forecast_series)
        abs_error_seasonal_naive = abs_error(target_series, forecast_series)
        
        
        ### add synthetic timeseries forecast to df
        DataFrameDict_test_baseline[item_id] = gdf
        
        ### get evaluation metrics for forecasted data
        forecast_values = forecasts_df[forecasts_df['forecast_item_id'] == item_id]
        forecast_median = forecast_values['forecast_quantile_5'].round().values
        forecast_mean = forecast_values['forecast_mean'].values
        
        ## bring in Quantile los to eval output
        q_loss_5 = quantile_loss(target_series, forecast_median, 0.5)
        
        
        ######
        ### squared == False for RMSE
        forecast_series = forecast_median
        rmse_forecast_qauntile_5 = math.sqrt(mse(target_series, forecast_series))
        mse_forecast_qauntile_5 = mse(target_series, forecast_series)
        abs_error_forecast_qauntile_5 = abs_error(target_series, forecast_series)
        ######
        ### squared == False for RMSE
        forecast_series= forecast_mean
        rmse_forecast_mean = math.sqrt(mse(target_series, forecast_series))
        mse_forecast_mean = mse(target_series, forecast_series)
        abs_error_forecast_mean = abs_error(target_series, forecast_series)
    
        
        
        eval_metrics_dict = {
                        'item_id_eval_stats': {
                            'item_id' : item_id,
                            ### root mean squared error
                            'rmse_forecast_qauntile_5': rmse_forecast_qauntile_5,
                            'rmse_forecast_mean' : rmse_forecast_mean,
                            'rmse_seasonal_naive': rmse_seasonal_naive,
                            'rmse_previous_value': rmse_previous_value,
                            'rmse_last_pred_window': rmse_last_pred_window,
                            ## mean squared error
                            'mse_forecast_qauntile_5' : mse_forecast_qauntile_5,
                            'mse_forecast_mean' : mse_forecast_mean,
                            'mse_seasonal_naive' : mse_seasonal_naive,
                            'mse_previous_value' : mse_previous_value,
                            'mse_last_pred_window' : mse_last_pred_window,
                            ## absolute error
                            'abs_error_forecast_qauntile_5' : abs_error_forecast_qauntile_5,
                            'abs_error_forecast_mean' : abs_error_forecast_mean,
                            'abs_error_seasonal_naive' : abs_error_seasonal_naive,
                            'abs_error_previous_value' : abs_error_previous_value,
                            'abs_error_last_pred_window' : abs_error_last_pred_window,
                            
                            ### forecast totals
                            'forecast_qauntile_5': sum(forecast_median),
                            'forecast_mean' : sum(forecast_mean),
                            'seasonal_naive': sum(seasonal_naive),
                            'previous_value': sum(previous_value),
                            'last_pred_window': sum(last_prediction_window),
                            'actual_total': sum(target_series),
                            
                            }
            
                        }
        
        dataset_test_dict[item_id].update(eval_metrics_dict)
        
    
    return dataset_test_dict