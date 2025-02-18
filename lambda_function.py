import pymysql
import boto3
import os
import json

def lambda_handler(event, context):
    # Fetch RDS credentials and endpoint from AWS SSM Parameter Store
    ssm = boto3.client('ssm', region_name='us-east-1')  # Replace with your AWS region

    try:
        db_name_param = ssm.get_parameter(Name="tcb-fullstack-app-mysql-db-name", WithDecryption=True)
        db_password_param = ssm.get_parameter(Name="tcb-fullstack-app-mysql-db-password", WithDecryption=True)
        db_user_param = ssm.get_parameter(Name="tcb-fullstack-app-mysql-db-user", WithDecryption=True)
        rds_endpoint_param = ssm.get_parameter(Name="tcb-fullstack-app-rds-endpoint", WithDecryption=True)

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
    db_host = RDS_ENDPOINT

    # Connect to the database
    connection = pymysql.connect(host=db_host, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)

    try:
        with connection.cursor() as cursor:
            # SQL to create the tcb_enquiry table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS tcb_enquiry (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                phone VARCHAR(15) NOT NULL,
                message TEXT NOT NULL,
                course VARCHAR(255) NOT NULL
            )
            """
            cursor.execute(create_table_sql)
            connection.commit()  # Commit the table creation

    except Exception as e:
        print(f"Error executing SQL: {e}")
        connection.rollback()  # Rollback in case of error
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    finally:
        connection.close()  # Ensure the connection is closed

    return {
        'statusCode': 200,
        'body': json.dumps('Table created and data inserted successfully')
    }
