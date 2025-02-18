import pymysql
import os
import boto3
import json

def lambda_handler(event, context):
    # Fetch RDS credentials and endpoint from AWS SSM Parameter Store
    ssm = boto3.client("ssm", region_name="us-east-1")  # Replace with your AWS region

    try:
        # Fetch parameters from SSM securely
        db_name_param = ssm.get_parameter(Name="tcb-fullstack-app-mysql-db-name", WithDecryption=True)
        db_password_param = ssm.get_parameter(Name="tcb-fullstack-app-mysql-db-password", WithDecryption=True)
        db_user_param = ssm.get_parameter(Name="tcb-fullstack-app-mysql-db-user", WithDecryption=True)
        rds_endpoint_param = ssm.get_parameter(Name="tcb-fullstack-app-rds-endpoint", WithDecryption=True)

        # Extract values from the parameters
        DB_NAME = db_name_param["Parameter"]["Value"]
        DB_USER = db_user_param["Parameter"]["Value"]
        DB_PASSWORD = db_password_param["Parameter"]["Value"]
        RDS_ENDPOINT = rds_endpoint_param["Parameter"]["Value"]
        
    except ssm.exceptions.ParameterNotFound as e:
        print(f"Parameter not found: {e}")
        raise Exception("One or more required SSM parameters are missing.")
    except Exception as e:
        print(f"Error retrieving parameters: {e}")
        raise Exception("Error retrieving parameters from SSM.")
    
    # Database connection settings
    db_host = RDS_ENDPOINT  # Using RDS endpoint fetched from SSM

    # Connect to the database
    connection = pymysql.connect(host=db_host, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)

    try:
        with connection.cursor() as cursor:
            # SQL to create a new table
            sql = """
            CREATE TABLE IF NOT EXISTS your_table_name (
                id INT AUTO_INCREMENT PRIMARY KEY,
                column1 VARCHAR(255) NOT NULL,
                column2 VARCHAR(255) NOT NULL
            )"""
            cursor.execute(sql)
        connection.commit()
    finally:
        connection.close()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Table created successfully')
    }
