import boto3                    #used to connect to aws servies
from io import BytesIO          #used to convert file into bytes in order to unzip
import zipfile                  #used to unzip the incomming file
import pymysql                  #used to connect to the mysql database
from mysql.connector import FieldType   #get the mysql field type in case formating is necessary
import re                       #used to format strings for mysql import
import datetime                 #not necessary in final version
import dateutil.tz              #not necessary in final version

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

    table_sql_str = ("SELECT * FROM " + file_dbname + ".table_file_remover "
    " Where file_status = 'COPY_SUCCESSFUL'")
    processing_table = sql_connect.execute(table_sql_str)            #returns number of rows in the database
  
    print("## there are " + str(processing_table) + " rows found in the table")
    rows = []
    if processing_table == 0:
        print('## There are no new files to add ##')
        print('## All Files have been checked, Closing the connections ##')
        sql_connect.close()
        conn.close()
        return{}
        
    elif processing_table > 0:
        print('SQL command was executed sucessfully')
        rows = sql_connect.fetchall()                   #list of all the data
        desc = sql_connect.description                  #tuple list of column names

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

                zip_obj = s3_resource.Object(bucket_name = bucket_name, key = key_name)
                buffer = BytesIO(zip_obj.get()["Body"].read())
                z = zipfile.ZipFile(buffer)                                 #unzips the file into a temporary space
                for filename in z.namelist():                               #once zip is open, loops through file contents
                    file_info = z.getinfo(filename)
                    print("## filename :: " + filename)
                    empty_count = 0
                    if(str(filename).endswith('.zip')):
                        print('## zip file does not need to be coppied over, not moving')    
                    else:
                        try:                                                #writes the unziped contents into a new bucket for storage
                        #    response = s3_resource.meta.client.upload_fileobj(z.open(filename),Bucket = outputput_bucket, Key = filename)
                        #    bucket = s3_resource.Bucket(outputput_bucket)
                        
                            new_key = CBC_submission_name+'/'+ CBC_submission_date+'/'+filename
                            response = s3_resource.meta.client.upload_fileobj(z.open(filename),Bucket = folder_name, Key = new_key)
                            bucket = s3_resource.Bucket(folder_name)
                        
                            obj = bucket.Object(key = new_key)
                            response = obj.get()
                            
                            try:
                                lines = response['Body'].read().split(b'\r\n')                  #split on return, newline, and space character
                            except:
                                lines = response['Body'].read().split(b'\.r\.n')    

                            for row in range(len(lines)):
                                if row == 0: 
                                    header_row = lines[row].decode('utf-8')
                                    header_row = list(header_row.split(","))
                                    continue
                                elif row > 0:        #row = 0 is the column header row
                                    current_row = lines[row].decode('utf-8')
                                    current_row = list(current_row.split(","))

                                if len(current_row) <= 1:               #if list is empty then do not use
                                    empty_count = empty_count + 1
                                    continue
                                if(len(set(current_row))==1):
                                    empty_count = empty_count + 1
                                    continue

                                if filename.lower() == "aliquot_metadata.csv":
                                    import_data_into_table(pre_valid_db,"Aliquot_Table",current_row,header_row,sql_connect,CBC_submission_name)
                                    import_data_into_table(pre_valid_db,"Aliquot_Tube",current_row,header_row,sql_connect,CBC_submission_name)
                                elif filename.lower() == "assay_metadata.csv":
                                    import_data_into_table(pre_valid_db,"Assay_Metadata",current_row,header_row,sql_connect,CBC_submission_name)
                                elif filename.lower() == "assay_target.csv":
                                    import_data_into_table(pre_valid_db,"Assay_Target",current_row,header_row,sql_connect,CBC_submission_name)
                                elif filename.lower() == "biospecimen_metadata.csv":
                                    import_data_into_table (pre_valid_db,"Biospecimen",current_row,header_row,sql_connect,CBC_submission_name)
                                    import_data_into_table (pre_valid_db,"Collection_Tube",current_row,header_row,sql_connect,CBC_submission_name)
                                elif filename.lower() == "confirmatory_test_results.csv":
                                    import_data_into_table (pre_valid_db,"Confirmatory_Test_Result",current_row,header_row,sql_connect,CBC_submission_name)
                                elif filename.lower() == "demographic_data.csv":
                                    import_data_into_table (pre_valid_db,"Demographic_Data",current_row,header_row,sql_connect,CBC_submission_name)
                                    import_data_into_table (pre_valid_db,"Comorbidity",current_row,header_row,sql_connect,CBC_submission_name)
                                    import_data_into_table (pre_valid_db,"Prior_Covid_Outcome",current_row,header_row,sql_connect,CBC_submission_name)
                                    import_data_into_table (pre_valid_db,"Submission_MetaData",current_row,header_row,sql_connect,CBC_submission_name,CBC_submission_date)
                                elif filename.lower() == "equipment_metadata.csv":
                                    import_data_into_table (pre_valid_db,"Equipment",current_row,header_row,sql_connect,CBC_submission_name)
                                elif filename.lower() == "prior_test_results.csv":
                                    import_data_into_table (pre_valid_db,"Prior_Test_Result",current_row,header_row,sql_connect,CBC_submission_name)
                                elif filename.lower() == "reagent_metadata.csv":
                                    import_data_into_table (pre_valid_db,"Reagent",current_row,header_row,sql_connect,CBC_submission_name)
#                                elif filename.lower() == "shipping-manifest.csv":
#                                    import_data_into_table (pre_valid_db,"Prior_Test_Result",current_row,header_row,sql_connect,CBC_submission_name)
                                else:
                                    print("## file name is not recongized ##")
                        except Exception as error_msg:
                            print(error_msg)
                    if filename.lower() == "biospecimen_metadata.csv":
                        query_auto = ("update " + pre_valid_db + ".`Biospecimen`"
                        "set Storage_Time_at_2_8 = TIME_TO_SEC(TIMEDIFF(Storage_End_Time_at_2_8 , Storage_Start_Time_at_2_8))/3600;")
                        processing_table = sql_connect.execute(query_auto) 
 ############################################################################################################################33
 ## after contents have been written to mysql, write name of file to file-processing table to show it was done
                    output_file_name = 's3://' + folder_name + '/' + new_key 
                  
                    query_auto = ("INSERT INTO " + file_dbname + ".`table_file_validator` "
                    "(orig_file_id, validation_file_location,validation_status,validation_result_location, validation_date,)"
                    "VALUE ('" + org_file_id ',' + output_file_name  + ',Unzipped_Success' + ',Result_location_location' + "',CONVERT_TZ(CURRENT_TIMESTAMP(), '+00:00', '-05:00'))")
                    processing_table = sql_connect.execute(query_auto)      #mysql command that will update the file-processor table 
                    
                    if processing_table > 0:
                        print("###    " + filename + " has been processed, there were  " + str(empty_count) + "  blank lines found in the file")
            else:
                print(zip_file_name + 'is not a zip file.')
 ############################################################################################################################33
 ## after all files have been processed, update file remover to indicate it has been done
               
            table_sql_str = ("UPDATE " + file_dbname + ".table_file_remover "
            "Set file_status = 'FILE_Processed'"
            "Where file_status = 'COPY_SUCCESSFUL' and file_location = '" + full_bucket_name + "'")
            
            processing_table = sql_connect.execute(table_sql_str)           #mysql command that changes the file-action flag so file wont be used again
####################################################################################################################    
    print('## All Files have been checked, Closing the connections ##')
    
    sql_connect.close()
    conn.commit()
    conn.close()

def import_data_into_table (file_dbname,table_name,current_row,header_row,sql_connect,CBC_submission_name,CBC_submission_time = "None_Provided"):

    sql_connect.execute("select * from " + file_dbname + ".`" + table_name + "`")
    desc = sql_connect.description

    column_names_list = [];         column_type_list = [];          
    for col_name in desc:
        column_names_list.append(col_name[0]);        column_type_list.append(FieldType.get_info(col_name[1]))
        
    res_cols = [];          res_head = [];
    
    for val in enumerate(column_names_list): 
        if val[1] in header_row:
            match_idx = header_row.index(val[1])
            res_cols.append(match_idx)
            
    for val in enumerate(header_row): 
        if val[1] in column_names_list:
            match_idx = column_names_list.index(val[1])
            res_head.append(match_idx)       

    string_1 =  "INSERT INTO " + file_dbname + ".`" + table_name + "`("
    string_2 =  "VALUE ("
    for i in res_cols:
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
