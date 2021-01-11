import boto3                    #used to connect to aws servies
from io import BytesIO          #used to convert file into bytes in order to unzip
import zipfile                  #used to unzip the incomming file
import pymysql                  #used to connect to the mysql database
from mysql.connector import FieldType   #get the mysql field type in case formating is necessary
import re                       #used to format strings for mysql import
import datetime                 #not necessary in final version
import dateutil.tz              #not necessary in final version
import os
from dateutil.parser import parse
import math
from seronetdBUtilities import *
from seronetSnsMessagePublisher import *
import datetime
import dateutil.tz


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    ssm = boto3.client("ssm")
    #   outputput_bucket = ssm.get_parameter(Name="Unzipped_dest_bucket", WithDecryption=True).get("Parameter").get("Value")
    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    file_dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
    pre_valid_db = ssm.get_parameter(Name="Prevalidated_DB", WithDecryption=True).get("Parameter").get("Value")
    try:
        conn = pymysql.connect(host = host_client, user=user_name, password=user_password, db=file_dbname, connect_timeout=5)
        print("SUCCESS: Connection to RDS mysql instance succeeded for file remover tables")
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance.")
        return{} 
       
    sql_connect = conn.cursor()
    
    
    if 'testMode' in event:
        if event['testMode']=="on":
            print("testMode on")
            processing_table=1
    else:
        print("testMode off")
        table_sql_str = ("SELECT * FROM `" + file_dbname + "`.table_file_remover Where file_status = 'COPY_SUCCESSFUL'")
    
        processing_table = sql_connect.execute(table_sql_str)            #returns number of rows in the database
  
    rows = []
    if processing_table == 0:
        print('## There are no new files to add ##')
        print('## All Files have been checked, Closing the connections ##')
        sql_connect.close()
        conn.close()
        return{}
        
    elif processing_table > 0:
        if 'testMode' in event:
            if event['testMode']=="on":
                contents_list = event['s3'].split("/")
                temporary_filename=contents_list[3]
                temporary_filename_contents=temporary_filename.split(".")
                temporary_filetype=temporary_filename_contents[len(temporary_filename_contents)-1]
                rows=((12345, temporary_filename, event['s3'], "testing", "testing", "COPY_SUCCESSFUL", "testing", temporary_filetype, "submit", contents_list[1], "testing"),)
                
                desc=(('file_id', 3, None, 11, 11, 0, False), ('file_name', 253, None, 1020, 1020, 0, True), ('file_location', 253, None, 1020, 1020, 0, True), ('file_added_on', 12, None, 19, 19, 0, True), ('file_last_processed_on', 12, None, 19, 19, 0, True), ('file_status', 253, None, 180, 180, 0, True), ('file_origin', 253, None, 180, 180, 0, True), ('file_type', 253, None, 180, 180, 0, True), ('file_action', 253, None, 180, 180, 0, True), ('file_submitted_by', 253, None, 180, 180, 0, True), ('updated_by', 253, None, 180, 180, 0, True))
        else:
            print('SQL command was executed sucessfully')
            rows = sql_connect.fetchall()                   #list of all the data
            desc = sql_connect.description                  #tuple list of column names
            print(rows)
            print(desc)
        
       
        
    
        column_names_list = [];
        column_type_list = [];
        for col_name in desc:
            column_names_list.append(col_name[0])                        #converts tubple names in list of names
            column_type_list.append(FieldType.get_info(col_name[1]))     #type of variable

        file_id_index = column_names_list.index('file_id')
        file_name_index = column_names_list.index('file_name')
        file_location_index = column_names_list.index('file_location')
        
        
        for row_data in rows:   
            current_row = list(row_data)                                        #Current row function is interating on
            full_bucket_name = current_row[file_location_index]
            zip_file_name = current_row[file_name_index]
            org_file_id = current_row[file_id_index]
            name_parts_list = full_bucket_name.split("/")
            if len(name_parts_list) == 4:
                folder_name = name_parts_list[0]
                CBC_submission_name = name_parts_list[1]
                CBC_submission_date = name_parts_list[2]
                CBD_ZIP_filename = name_parts_list[3]
            elif len(name_parts_list) == 2:
                folder_name = name_parts_list[0]
                CBC_submission_name = "Test_CBC_Name"
                eastern = dateutil.tz.gettz('US/Eastern')
                CBC_submission_date=datetime.datetime.now(tz=eastern).strftime("%H-%M-%S-%m-%d-%Y")
                CBD_ZIP_filename = name_parts_list[1]
            
            first_folder_cut = full_bucket_name.find('/')                       #seperates the bucket and key names from the file path
            if first_folder_cut > -1:                                       
                key_name = full_bucket_name[(first_folder_cut+1):]
                bucket_name = full_bucket_name[:(first_folder_cut)]

            if(str(zip_file_name).endswith('.zip')):                            #checks to see if file name is a zip
                print("## UnZipping    folder name :: " + bucket_name + "    key name :: " + key_name)

                size = [2]*9
                zip_obj = s3_resource.Object(bucket_name = bucket_name, key = key_name)
                buffer = BytesIO(zip_obj.get()["Body"].read())
                z = zipfile.ZipFile(buffer)                                 #unzips the file into a temporary space
                 
                full_name_list = z.namelist()
                foreign_key_level = [0]*len(full_name_list)
                
                for filename in enumerate(z.namelist()):
                    if filename[1] in ['Demographic_Data.csv','Assay_Metadata.csv']:
                        foreign_key_level[filename[0]] = 0
                    elif filename[1] in ['Assay_Target.csv', 'Biospecimen_Metadata.csv', 'Prior_Test_Results.csv']:
                        foreign_key_level[filename[0]] = 1
                    elif filename[1] in ['Equipment_Metadata.csv', 'Reagent_Metadata.csv',  'Aliquot_Metadata.csv','Confirmatory_Test_Results.csv']:
                        foreign_key_level[filename[0]] = 2
         
                sort_idx = sorted(range(len(foreign_key_level)), key=lambda k: foreign_key_level[k])
                sort_idx = [int(l) for l in sort_idx]
                full_name_list =  [full_name_list[l] for l in sort_idx]
                
                validation_status_list=[]
                validation_file_location_list=[]
                
                for filename in full_name_list:                               #once zip is open, loops through file contents
                    file_info = z.getinfo(filename)
                    empty_count = 0
                    dub_counts = 0
                    if(str(filename).endswith('.zip')):
                        print('## zip file does not need to be coppied over, not moving')    
                    else:
                        try:                                                #writes the unziped contents into a new bucket for storage
                            validation_status = 'FILE_VALIDATION_SUCCESS'
                            new_key = CBC_submission_name+'/'+ CBC_submission_date+'/'+filename
                            #print("##unziped file location :: " + folder_name + "/" + new_key)
                            response = s3_resource.meta.client.upload_fileobj(z.open(filename),Bucket = folder_name, Key = new_key)
                            bucket = s3_resource.Bucket(folder_name)
                        
                            obj = bucket.Object(key = new_key)
                            response = obj.get()
                            
                            try:
                                lines = response['Body'].read().split(b'\r\n')                  #split on return, newline, and space character
                            except:
                                lines = response['Body'].read().split(b'\.r\.n')
############################################################################################################################
                            
                            for row in range(len(lines)):
                                if row == 0: 
                                    header_row = lines[row].decode('utf-8');        
                                    header_row = list(header_row.split(","));            
                                    continue
                                elif row > 0:        #row = 0 is the column header row
                                    current_row = lines[row].decode('utf-8');      
                                    current_row = list(current_row.split(","))

                                if len(current_row) <= 1:               #if list is empty then do not use
                                    empty_count = empty_count + 1;                
                                    continue
                                if(len(set(current_row))==1):
                                    empty_count = empty_count + 1;                 
                                    continue
                            
                                if filename.lower() == "demographic_data.csv":
                                    dub_counts = dub_counts + import_data_into_table (row,filename,pre_valid_db,"Demographic_Data",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                    import_data_into_table (row,filename,pre_valid_db,"Comorbidity",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                    import_data_into_table (row,filename,pre_valid_db,"Prior_Covid_Outcome",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                    import_data_into_table (row,filename,pre_valid_db,"Submission_MetaData",current_row,header_row,sql_connect,conn,CBC_submission_name,CBC_submission_date)
                                elif filename.lower() == "assay_metadata.csv":         
                                    dub_counts = dub_counts + import_data_into_table(row,filename,pre_valid_db,"Assay_Metadata",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                elif filename.lower() == "assay_target.csv":
                                    dub_counts = dub_counts + import_data_into_table(row,filename,pre_valid_db,"Assay_Target",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                elif filename.lower() == "biospecimen_metadata.csv":
                                    dub_counts = dub_counts + import_data_into_table (row,filename,pre_valid_db,"Biospecimen",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                    import_data_into_table (row,filename,pre_valid_db,"Collection_Tube",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                elif filename.lower() == "prior_test_results.csv":
                                    dub_counts = dub_counts + import_data_into_table (row,filename,pre_valid_db,"Prior_Test_Result",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                elif filename.lower() == "aliquot_metadata.csv":
                                    dub_counts = dub_counts + import_data_into_table(row,filename,pre_valid_db,"Aliquot",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                    import_data_into_table(row,filename,pre_valid_db,"Aliquot_Tube",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                elif filename.lower() == "equipment_metadata.csv":
                                    dub_counts = dub_counts + import_data_into_table (row,filename,pre_valid_db,"Equipment",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                elif filename.lower() == "confirmatory_test_results.csv":
                                    dub_counts = dub_counts + import_data_into_table (row,filename,pre_valid_db,"Confirmatory_Test_Result",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                elif filename.lower() == "reagent_metadata.csv":
                                    dub_counts = dub_counts + import_data_into_table (row,filename,pre_valid_db,"Reagent",current_row,header_row,sql_connect,conn,CBC_submission_name)
                                else:
                                    print(filename + " IS NOT an expected file and will not be written to database")
                                    validation_status = 'FILE_VALIDATION_Failure'
                            if(row - empty_count) == 0:
                                print(file_name + " is empty and not a valid file")
                                validation_status = 'FILE_VALIDATION_Failure'
                            print("## there were " + str(row - empty_count) + " rows found with " + str(dub_counts) + " dupliactes found :: " + str(row - empty_count - dub_counts) + " records written to the table")
############################################################################################################################
                        except Exception as error_msg:
                            validation_status = 'FILE_VALIDATION_Failure'
                            print(error_msg)
                    if filename.lower() == "biospecimen_metadata.csv":
                        query_auto = ("update `" + pre_valid_db + "`.`Biospecimen`"
                        "set Storage_Time_at_2_8 = TIME_TO_SEC(TIMEDIFF(Storage_End_Time_at_2_8 , Storage_Start_Time_at_2_8))/3600;")
                        processing_table = sql_connect.execute(query_auto) 
 ############################################################################################################################33
 ## after contents have been written to mysql, write name of file to file-processing table to show it was done
 ###################if it is testMode, than not update to the db
                    validation_file_location = 's3://' + folder_name + '/' + new_key 
                    validation_result_location = 'validation_result_location'
                    validation_notification_arn = 'validation_notification_arn'
                    validation_status_list.append(validation_status)# record each validation status
                    validation_file_location_list.append(validation_file_location)# record each validation file location 
                    
                    if 'testMode' in event:
                        if event['testMode']=="on":
                            print("Since testMode is on, the file unziping will not record to the database")
                    else:
                        query_auto = ("INSERT INTO `" + file_dbname + "`.`table_file_validator` "
                        "          (orig_file_id,   validation_file_location,         validation_status, validation_notification_arn,   validation_result_location,            validation_date)"
                        "VALUE ('" + str(org_file_id) + "','" + validation_file_location  + "','" + validation_status +"','" + validation_notification_arn +"','" + validation_result_location + "'," + "CONVERT_TZ(CURRENT_TIMESTAMP(), '+00:00', '-05:00'))")
                    
                        processing_table = sql_connect.execute(query_auto)      #mysql command that will update the file-processor table 


                    
                        if processing_table > 0:
                            print("###    " + filename + " has been processed, there were  " + str(empty_count) + "  blank lines found in the file")
            else:
                print(zip_file_name + 'is not a zip file.')
 ############################################################################################################################33
 ## after all files have been processed, update file remover to indicate it has been done
            
            if 'testMode' in event:
                if event['testMode']=="on":
                   file_submitted_by="'"+row_data[9]+"'"
                   
                    
            else:
                table_sql_str = ("UPDATE `" + file_dbname + "`.table_file_remover "
                "Set file_status = 'FILE_Processed'"
                "Where file_status = 'COPY_SUCCESSFUL' and file_location = '" + full_bucket_name + "'")
                
                processing_table = sql_connect.execute(table_sql_str)           #mysql command that changes the file-action flag so file wont be used again
                
                #get the prefix from the database
                
                job_table_name='table_file_remover'
                exe="SELECT * FROM "+job_table_name+" WHERE file_id="+str(org_file_id)
                sql_connect.execute(exe)
                sqlresult = sql_connect.fetchone()
                file_submitted_by="'"+sqlresult[9]+"'"
            
            ######publish message to sns topic
            #file_submitted_by="NULL"
            
            # getting the validation time stamp
            org_file_name=zip_file_name
            eastern = dateutil.tz.gettz('US/Eastern')
            timestampDB=datetime.datetime.now(tz=eastern).strftime('%Y-%m-%d %H:%M:%S')
            print(file_submitted_by)
            result={'org_file_id':str(org_file_id),'file_status':'FILE_Processed','validation_file_location_list':validation_file_location_list,'validation_status_list':validation_status_list,'full_name_list':full_name_list,'validation_date':timestampDB,'file_submitted_by':file_submitted_by,'previous_function':"prevalidator",'org_file_name':org_file_name}
            TopicArn_Success=ssm.get_parameter(Name="TopicArn_Success", WithDecryption=True).get("Parameter").get("Value")
            TopicArn_Failure = ssm.get_parameter(Name="TopicArn_Failure", WithDecryption=True).get("Parameter").get("Value")
            response=sns_publisher(result,TopicArn_Success,TopicArn_Failure)
            
####################################################################################################################    
    print('## All Files have been checked, Closing the connections ##')
    
    sql_connect.close()
    conn.commit()
    conn.close()

def import_data_into_table (row_idex,filename,valid_dbname,table_name,current_row,header_row,sql_connect,conn,CBC_submission_name,CBC_submission_time = "None_Provided"):
    if row_idex == 1:
        print("## writting " + filename + " into the `" + valid_dbname + "`.`" + table_name + "` table")

    query_str = ("show index from `" + valid_dbname + "`.`" + table_name + "` where Key_name = 'PRIMARY';")
    query_res = sql_connect.execute(query_str)
    rows = sql_connect.fetchall()
    dup_counts = 0;

    if query_res > 0:
        string_2 ="where "
        if query_res == 1:      #table has 1 primary key
            if table_name == "Submission_MetaData":             #primary key does not exist in the file
                Submission_ID = 1;
            else:
                curr_prim_key = current_row[header_row.index(rows[0][4])]
                string_2 = string_2 + " `" + rows[0][4] + "` = '" + curr_prim_key + "'"
        elif query_res == 2:      #table has 2 primary keys
            curr_prim_key_1 = current_row[header_row.index(rows[0][4])]
            curr_prim_key_2 = current_row[header_row.index(rows[1][4])]
            string_2 = string_2 + " `" + rows[0][4] + "` = '" + curr_prim_key_1 + "'"
            string_2 = string_2 + "and `" + rows[1][4] + "` = '" + curr_prim_key_2 + "'"
        elif query_res == 3:      #table has 3 primary keys
            curr_prim_key_1 = current_row[header_row.index(rows[0][4])]
            curr_prim_key_2 = current_row[header_row.index(rows[1][4])]
            curr_prim_key_3 = current_row[header_row.index(rows[2][4])]
            string_2 = string_2 + " `" + rows[0][4] + "` = '" + curr_prim_key_1 + "'"
            string_2 = string_2 + "and `" + rows[1][4] + "` = '" + curr_prim_key_2 + "'"
            string_2 = string_2 + "and `" + rows[2][4] + "` = '" + curr_prim_key_3 + "'"
        
        if table_name != "Submission_MetaData":             #primary key does not exist in the file
            query_str = ("select * from `" + valid_dbname + "`.`" + table_name + "` " + string_2)
            query_res = sql_connect.execute(query_str)
            if query_res > 0:
                dup_counts =  1;
                return dup_counts

    query_str = "select * from `" + valid_dbname + "`.`" + table_name + "`"
    query_res = sql_connect.execute(query_str)
    desc = sql_connect.description

    column_names_list = [];         column_type_list = [];          
    for col_name in desc:
        column_names_list.append(col_name[0]);        
        column_type_list.append(FieldType.get_info(col_name[1]))

    res_cols = [];          res_head = [];
    
    for val in enumerate(column_names_list): 
        if val[1] in header_row:
            match_idx = header_row.index(val[1])
            res_cols.append(match_idx)
            
    for val in enumerate(header_row): 
        if val[1] in column_names_list:
            match_idx = column_names_list.index(val[1])
            res_head.append(match_idx)       

    string_1 =  "INSERT INTO `" + valid_dbname + "`.`" + table_name + "`("
    string_2 =  "VALUE ("
    for i in res_cols:
        if header_row[i] == "Submission_ID":
            print(string_1)
            string_1 = string_1 + " "
        else:
            string_1 = string_1 + header_row[i] + ","
    string_1 = string_1[:-1] + ")"

    res_head.sort()

    for i in enumerate(res_cols):              #still need to check for boolen and date flags
        column_name = column_names_list[res_head[i[0]]]
        column_type = column_type_list[res_head[i[0]]]
        column_value = current_row[res_cols[i[0]]]
        
#        print("## column name :: " + column_name + "   column type :: " + column_type)

        if column_type.upper() == 'DATE':
            column_value = column_value.replace('/',',')
            string_2 = string_2 + "STR_TO_DATE('" +  column_value + "','%m,%d,%Y'),"
        elif column_type.upper() == 'TIME':
            string_2 = string_2 + "TIME_FORMAT('" +  column_value + "','%H:%i'),"
        elif column_type.upper() == 'TINY':
            if column_value == 'T':
                string_2 = string_2 + "1,"
            elif column_value == 'F':
                string_2 = string_2 + "0,"
        else:
            string_2 = string_2 + "'"  + column_value + "',"
    string_2 = string_2[:-1] + ")"
  
    if 'Submission_CBC' in column_names_list:
        string_1 = string_1[:-1] + ",Submission_CBC)"
        string_2 = string_2[:-1] + ",'" + CBC_submission_name + "')"

    if 'Submission_time' in column_names_list:
        string_1 = string_1[:-1] + ",Submission_time)"
        CBC_submission_time = CBC_submission_time.replace('-',',')
        CBC_submission_time = CBC_submission_time.replace('_',',')
        string_2 = string_2[:-1] + ",STR_TO_DATE('" +  CBC_submission_time + "','%H,%i,%S,%m,%d,%Y'))"
 
    query_auto = string_1 + string_2

    processing_table = sql_connect.execute(query_auto)
    if processing_table == 0:
        print("## error in submission string")
    else:
        conn.commit()
    return dup_counts
