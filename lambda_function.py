import pymysql
import os
import boto3
import json

def lambda_handler(event, context):
    # Retrieve RDS credentials from SSM Parameter Store
    ssm = boto3.client('ssm')
    db_password = ssm.get_parameter(Name=os.environ['DB_PASSWORD_PARAM'], WithDecryption=True)['Parameter']['Value']
    
    # Database connection settings
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']
    db_user = os.environ['DB_USER']
    
    # Connect to the database
    connection = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name)
    
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
