
def pull_current_data():
    from mods.DBA import RedshiftProdConn 
    import pandas as pd
    
    conn, cursor = RedshiftProdConn.connect_to_redshift()
    ## columns and table intentionally left blank
    sql = r"""SELECT 
               {columns}
            FROM {table}""".format(columns='', table='')
      
    df = pd.read_sql(sql, conn)
    conn.close()
    cursor.close()
    
    ## convert date_actual to pandas datetime
    df['first_day_of_week'] = pd.to_datetime(df['first_day_of_week'], format="%Y-%m-%d", errors="coerce")
    
    
    return df

#######################################################

def stratify_data_by_distribution(df, sku_column, target_column):
    ###################################################################
    ######## Create Item Sales Groups for Zero Inflated Model #########
    ###################################################################
    item_sales = df.groupby([sku_column])[target_column].sum()
    ##TODO: imporve break point for zero inflated models
    item_sales_gte = item_sales[item_sales >= 100]
    item_list_low_zero = item_sales_gte.index
    
    item_sales_lt = item_sales[item_sales < 100]
    item_list_high_zero = item_sales_lt.index
    
    #######################################################
    ################### Check Variance ####################
    #######################################################
    
    item_variance_mean = df.groupby(sku_column).agg({target_column: ['var', 'mean']})
    item_variance_mean.columns = ['var', 'mean']
    item_variance_mean['diff'] = item_variance_mean['var'] - item_variance_mean['mean']
    
    ##TODO: improve breakpoint for mean != variance
    df_mean_equal_var = item_variance_mean[item_variance_mean['diff'] < 3]
    item_list_mean_equals_var = df_mean_equal_var.index.to_list()
    

    ###############################################################################
    ############# split items into datasets with high and low variance ############
    ###############################################################################

    ### datasets for Poisson Model
    df_poisson = df[(df[sku_column].isin(item_list_mean_equals_var)) & (df[sku_column].isin(item_list_low_zero))]
    
    ### datasets for Excessive Zero Poisson Model
    df_zero_inflated_poisson = df[(df[sku_column].isin(item_list_mean_equals_var)) & (df[sku_column].isin(item_list_high_zero))]

    ### datasets for Negative Binomal Model
    df_negaBi = df[(~df[sku_column].isin(item_list_mean_equals_var)) & (df[sku_column].isin(item_list_low_zero))]

    ### datasets for Excessive Zero Negative Binomal Model
    df_zero_inflated_negaBi = df[(~df[sku_column].isin(item_list_mean_equals_var)) & (df[sku_column].isin(item_list_high_zero))]

    # return  [df_poisson, df_zero_inflated_poisson, df_negaBi, df_zero_inflated_negaBi]
    return  df_poisson, df_zero_inflated_poisson, df_negaBi, df_zero_inflated_negaBi
#######################################################

def DeepAR_ts_datasets(df_list, freq, target_column, sku_column,
                       categorical_columns, quantitative_columns_past, quantitative_columns_known):
    
    # train, test, future_df = df_list
    test, future_df = df_list
    
    from gluonts.dataset.pandas import PandasDataset
    ########################################################################
    ################# Get cardinality for static features ##################
    ########################################################################
    def get_cardinality_set(df, group_by_col: str, cat_columns: list):
        
        df = df[[sku_column] + categorical_columns].drop_duplicates()
        df = df.set_index(sku_column)

        static_cardinality_list = []
        for i in categorical_columns:
            static_cardinality_list.append(len(df[i].unique()))
            
        return static_cardinality_list, df

     
    #### Count Static Feature Values
    static_cardinality, static_features = get_cardinality_set(test, sku_column, categorical_columns)
    
    ###############################################################################
    ################# split long dataset into dict of dataframes ##################
    ###############################################################################
    def prep_dict(df, group_by_col: str):
        DataFrameDict = {}
        for item_id, gdf in df.groupby(group_by_col):
            DataFrameDict[item_id] = gdf
        return DataFrameDict
    
    #################### Train datasets ################################# 
    # DataFrameDict_train = prep_dict(train, sku_column)
    
    #################### Test datasets #################################    
    DataFrameDict_test = prep_dict(test, sku_column)
    
    #################### Future datasets #################################    
    DataFrameDict_future = prep_dict(future_df, sku_column)
    
    ############################################################################################
    ################# Create PandasDataset Object for Estimator and Predictor ##################
    ############################################################################################
    
    feat_dynamic_real = ['holiday_ind'] + quantitative_columns_known
    
    #train datasets
    # df_train
    # train_datasets = PandasDataset(DataFrameDict_train,
    #                         target=target_column,
    #                         freq=freq,
    #                         feat_dynamic_real=feat_dynamic_real,
    #                           static_features=static_features,
    #                         )
    
    ### test datasets
    # df_test 
    test_datasets = PandasDataset(DataFrameDict_test,
                            target=target_column,
                            freq=freq,
                            feat_dynamic_real=feat_dynamic_real,
                            static_features=static_features,
                            )
    
    ### future dataset
    # df_future
    future_datasets = PandasDataset(DataFrameDict_future,
                            target=target_column,
                            freq=freq,
                            feat_dynamic_real=feat_dynamic_real,
                            static_features=static_features,
                            )
    ## removed traindatatset from results not needed in flow
    return test_datasets, future_datasets, static_cardinality


def CNN_ts_datasets(df_list, freq, target_column, sku_column,
                    categorical_columns, quantitative_columns_past, quantitative_columns_known):
    
     # train, test, future_df = df_list
     test, future_df = df_list
     
     from gluonts.dataset.pandas import PandasDataset
     ########################################################################
     ################# Get cardinality for static features ##################
     ########################################################################
     def get_cardinality_set(df, group_by_col: str, cat_columns: list):
         
         df = df[[sku_column] + categorical_columns].drop_duplicates()
         df = df.set_index(sku_column)

         static_cardinality_list = []
         for i in categorical_columns:
             static_cardinality_list.append(len(df[i].unique()))
             
         return static_cardinality_list, df

     #### Count Static Feature Values
     static_cardinality, static_features = get_cardinality_set(test, sku_column, categorical_columns)
     
     ###############################################################################
     ################# split long dataset into dict of dataframes ##################
     ###############################################################################
     def prep_dict(df, group_by_col: str):
         DataFrameDict = {}
         for item_id, gdf in df.groupby(group_by_col):
             DataFrameDict[item_id] = gdf
         return DataFrameDict

    #################### Train datasets ################################# 
     # DataFrameDict_train = prep_dict(train, sku_column)
     
     #################### Test datasets #################################   
     #### dataset containing all records for test set
     DataFrameDict_test = prep_dict(test, sku_column)
     
     #### dataset containing all records and null future records
     DataFrameDict_future = prep_dict(future_df, sku_column)
     
     ############################################################################################
     ################# Create PandasDataset Object for Estimator and Predictor ##################
     ############################################################################################
     feat_dynamic_real = ['holiday_ind',] + quantitative_columns_known
     past_feat_dynamic_real = quantitative_columns_past
     
     #train dataset
     # train_datasets = PandasDataset(DataFrameDict_train,
     #                         target=target_column,
     #                         freq=freq,
     #                         feat_dynamic_real=feat_dynamic_real,
     #                           past_feat_dynamic_real=past_feat_dynamic_real,
     #                           static_features=static_features,
     #                         )

     ### test dataset
     test_datasets = PandasDataset(DataFrameDict_test,
                             target=target_column,
                             freq=freq,
                              feat_dynamic_real=feat_dynamic_real,
                             past_feat_dynamic_real=past_feat_dynamic_real,
                             static_features=static_features,
                             )

     ### test dataset
     future_datasets = PandasDataset(DataFrameDict_future,
                             target=target_column,
                             freq=freq,
                             unchecked=False,
                              feat_dynamic_real=feat_dynamic_real,
                             past_feat_dynamic_real=past_feat_dynamic_real,
                             static_features=static_features,
                             )

     # return train_datasets, test_datasets, future_datasets, static_cardinality
     return test_datasets, future_datasets, static_cardinality
    
