from mods.DBA import emailBatchResultStatus as em
from prefect.server.schemas.states import Failed


def upload_file_from_df(s3_resource, dataframe, folderpath, key_name, fileFormat='parquet', bucket='my_s3_bucket', seperator='|', headers=False):
    from io import StringIO, BytesIO
    from botocore.exceptions import ClientError
    import logging
    """Upload a file to an S3 bucket from memory
    
    :dataframe: dataframe to be placed into csv on S3
    :key: full path and file name on s3
    :return: True if file was uploaded, else False """
    
    if fileFormat == 'parquet':
        out_buffer = BytesIO()
        dataframe.to_parquet(out_buffer,
                             compression='gzip',
                             index=False,
                             allow_truncated_timestamps=True,
                             # use_deprecated_int96_timestamps=True,
                              coerce_timestamps ='ms'
                             )
        
    elif fileFormat == 'csv':
        out_buffer = StringIO()
        dataframe.to_csv(out_buffer, header=headers, index=False, sep=seperator)
    
    key = folderpath + key_name
    # Here Filename is the name of the local file and Key is the filename you’ll see in S3
    try:
        s3_resource.Object(bucket, key).put(Body=out_buffer.getvalue())
    except ClientError as e:
        logging.error(e)
        return False
    return True

def store_in_s3(df, model_name, version, target, start, end):
    from datetime import datetime
    import logging as logger
    from mods.DBA import s3_connect as s3re
    
    current_date_time = datetime.today()
    df['Extract_Date_Time'] = current_date_time
    df['Version'] = version
    df['Model_Name'] = model_name
    df['target'] = target
    df['start_window'] = start
    df['end_window'] = end
    
    # name of file to store extract
    # Set start date format value for sql query and csv
    startDate = current_date_time.strftime("%Y%m%d %H:%M:%S")
    filename = 'SKU_Forecast_{startDate}'.format(startDate=startDate)
    
    
    ## Connect to S3
    s3_resource = s3re.s3_resrouce_connection()
    ## Set S3 Write path and file name (key)
    zone_prefix = 'PROD/STG/'
    folderpath = 'Models/DemandForecast/'
    s3_zone_folder = zone_prefix + folderpath
    
    
    key_name = model_name + '/' + filename + '.parquet'
    try:
        logger.info('uploading ' + filename + ' to s3 ')
        ## Write data to S3
        upload_file_from_df(s3_resource, df, s3_zone_folder, key_name, bucket='my_s3_bucket')
        s3_key = s3_zone_folder + key_name
        
        return s3_key
    
    except Exception as err:
         logger.error(err)
         results = 'FAILED to upload file to s3!' + __name__
         logger.error(results)
         em.emailResults(__file__, results, 0)
         return Failed(message=err)
     