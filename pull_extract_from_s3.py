

def pull_s3_objects(prefix):
    try:
        from prefect.server.schemas.states import Failed
        import logging as logger
        from mods.DBA import s3_connect
        from mods.DBA import S3_file_trans
        
        s3_resource = s3_connect.s3_resrouce_connection()
        s3_bucket='my_s3_bucket'
        
        obj_list = S3_file_trans.s3_bucket_subset_obj_list(s3_resource, s3_bucket , prefix)
        
        return obj_list
    
    except Exception as err:
        logger.error(err)
        return Failed(message=err)
        
