def s3_archive_extracts(list_of_dirs, bucket='my_s3_bucket'):
    
    import logging as logger
    from botocore.exceptions import ClientError
    from mods.DBA import s3_connect as s3re
    from prefect.server.schemas.states import Failed
    from mods.DBA import emailBatchResultStatus as em
    import datetime
    
    flatlist=[]
    for sublist in list_of_dirs:
        if type(sublist) == list:
            for element in sublist:
                flatlist.append(element)
        else: 
            flatlist.append(sublist)
    
    s3_resource = s3re.s3_resrouce_connection()
    
    ## list of s3 keys that need to be moved
    for key in flatlist:
        try:
            logger.info('transfering ' + str(key))
            folderpath_list = key.split('/')
            ## pop last item in list to get filename
            filename = folderpath_list.pop()
            ## pop next item to get Extract Name
            extract_name = folderpath_list.pop()
            
            ## Get the current year
            current_year = datetime.datetime.now().strftime('%Y')
            ## recreate the prefix without the filename
            folderpath = '/'.join(folderpath_list)
            
            ## set new folder path
            archive_folderpath = folderpath + '/Archive/' + current_year
            new_key = archive_folderpath  + '/' + extract_name  + '/' + filename

            source = key
            destination = new_key
            copy_source = bucket + '/' + source
            
            # Copy object A as object B
            s3_resource.Object(bucket, destination).copy_from(CopySource=copy_source)
            # Delete the former object A
            s3_resource.Object(bucket, source).delete()
        
        except ClientError as e:
            logger.error(e)
            results = 'FAILED! ' + __name__
            logger.error(results)
            em.emailResults(__file__, results, 0)
            return Failed(message=e)
    return True
