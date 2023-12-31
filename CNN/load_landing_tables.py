def load_landing_tables(testProd_ind):
    try:
        import os
        from prefect.server.schemas.states import Failed
        from mods.DBA import emailBatchResultStatus as em
        import pandas as pd
        import logging as logger
        from mods.DBA import ConnectToDB as cn
        from mods.secrets import secrets as sec
        
        aws_s3 = sec.aws_redshift_role('RedshiftS3AccessRole', 'credentials')
        
        #############################
        ### Set Script Parameters ###
        #############################
        
        #set name of columns from metadata lookup table
        query_FilePath = 'script_file_path'
        query_FileName = 'script_name'
        
        #######################################
        ### Set File path and load Metadata ###
        #######################################
        
        logger.info('loading script metadata ' + __name__)
        
        #open connection to local postgres
        selected_db = 'Redshift_EDW'
        user = 'user'
        conn1, cursor1 = cn.connect_to_db(selected_db, user, testProd_ind)
        
        #Metadata Table SQL String
        sql = """SELECT 
                {metadata_columns}
            FROM {metadata_table}";""".format(metadata_columns='dummyColumns', metadata_table='dummyTable')
        
        #retrive reporting metadata from DB
        df = pd.read_sql(sql, conn1)
        cursor1.close()
        conn1.close()
        
        df.sort_values(by=['step'], ascending=True, inplace=True)
        
        ## Step Exclusion
        # exclude_target_tables = ['Current_Customer_Dim', 'Item_Dim', 'SLM_Dim']
        # df = df[~df.Target_Table.isin(exclude_target_tables)]
        
        ### Only Select Active Reports; Static, always 1
        df = df.query('process == "Demand_Forecast"')
        df = df.query('active == 1')
        df = df.query('target_schema == "data_land"')
        df.reset_index(drop=True,inplace=True)
        
        
        ####################
        ### Load Scripts ###
        ####################
        script = ''
        
        for i, row in df.iterrows():
            
            logger.info('Setting query for step ' + str(row['step']))
            ## script path from metadata file
            fp = row[query_FilePath]
            fp = os.path.expanduser(fp)
            textFile = os.path.join(fp, row[query_FileName])
            
            #### Read in sql script from file
            with open(textFile, 'r') as file:
                script_local = file.read()
                script_local = script_local
                script = script + '   ' + script_local
        
        script = script.format(DevProd=testProd_ind.upper(), aws_iam_role=aws_s3)
                        
        ## Execute Script 
        logger.info('Executing Query in ' + __name__)
        selected_db = 'Redshift_EDW'
        user = 'user' 
        conn2, cursor2 = cn.connect_to_db(selected_db, user, testProd_ind)
        cursor2.execute(script)
        conn2.commit()
        cursor2.close()
        conn2.close()
        
        return 'Completed Loading Landing Tables'
    
    except Exception as err:
        results = err
        em.emailResults(__file__, results, 0)
        return Failed(message=err)