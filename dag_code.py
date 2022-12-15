# --------------------------------
# LIBRARIES
# --------------------------------

# Import Airflow Operators
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
# Import Library to set DAG timing
from datetime import datetime, timedelta
# Import Libraries to handle dataframes
import pandas as pd
# Import Library to send Slack Messages
import requests

# --------------------------------
# CONSTANTS
# --------------------------------

# Set input and output paths
FILE_PATH_INPUT = '/home/airflow/gcs/data/'
FILE_PATH_OUTPUT = '/home/airflow/gcs/data/'
# Import CSV into a pandas dataframe
df = pd.read_csv(FILE_PATH_INPUT+"diag.csv")
# Slack webhook link
slack_webhook = 'https://hooks.slack.com/services/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'

# --------------------------------
# FUNCTION DEFINITIONS
# --------------------------------

# Function to send messages over slack using the slack_webhook
def send_msg(text_string): requests.post(slack_webhook, json={'text': text_string}) 

# Function to generate the diagnostics report
# Add print statements to each variable so that it appears on the Logs
def send_report():
    """
    This function sends a diagnostics report to Slack with
    the below patient details:
    """
#1. Title
    send_msg("SUMMARY REPORT")

#1. Average O2 level
    average_o2=df['o2_level'].mean()
    print ("The average O2 level is", average_o2)
    message_a="The average O2 level is"
    text_string = 'average_o2 Value:{0}. {1}'.format(message_a,average_o2)
    send_msg(text_string)
#2. Average Heart Rate
    average_heart_rate=df['heart_rate'].mean()
    print ("The average heart rate is", average_heart_rate)
    message_b="The average heart rate is"
    text_string = 'average_heart_rate Value:{0}. {1}'.format(message_b,average_heart_rate)
    send_msg(text_string)
#3. Standard Deviation of O2 level
    std_o2=df['o2_level'].std()
    print ("The standard deviation of O2 level is", std_o2)  
    message_c="standard deviation of O2 Level is"
    text_string = 'std_o2 Value:{0}. {1}'.format(message_c,std_o2)
    send_msg(text_string)  
#4. Standard Deviation of Heart Rate    
    std_heart_rate=df['heart_rate'].std()
    print ("The standard deviation of heart rate is", std_heart_rate)
    message_d="standard deviation of heart rate is"
    text_string = 'std_heart_rate Value:{0}. {1}'.format(message_d,std_heart_rate)
    send_msg(text_string)  
#5. Minimum O2 level
    min_o2=df['o2_level'].min()
    print ("The minimum O2 level is", min_o2)     
    message_e="Minimum O2 Level is"
    text_string = 'min_o2 Value:{0}. {1}'.format(message_e,min_o2)
    send_msg(text_string)  
#6. Minimum Heart Rate
    min_heart_rate=df['heart_rate'].min()
    print ("The minimum heart rate is", min_heart_rate)
    message_f="Minimum Heart Rate is"
    text_string = 'min_heart_rate Value:{0}. {1}'.format(message_f,min_heart_rate)
    send_msg(text_string)  
#7. Maximum O2 level 
    max_o2=df['o2_level'].max()
    print ("The maximum O2 level is", max_o2) 
    message_g="The maximum O2 level is"
    text_string = 'max_o2 Value:{0}. {1}'.format(message_g,max_o2)
    send_msg(text_string)  
#8. Maximum Heart Rate
    max_heart_rate=df['heart_rate'].max()
    print ("The maximum heart rate is", max_heart_rate) 
    message_h="The maximum heart rate is"
    text_string = 'max_heart_rate Value:{0}. {1}'.format(message_h,max_heart_rate)
    send_msg(text_string)  

#3 Function to filter anomalies in the data
# Add print statements to each output dataframe so that it appears on the Logs
def flag_anomaly():
    hr_data=[]
    o2_data=[]

    hr_mean=80.81
    hr_sd=10.28
    level_3 = hr_mean + 3 * hr_sd
    level_6 = hr_mean - 3 * hr_sd
    for i in range(len(df)): 
        if  df['heart_rate'][i] >= level_3:
            message_1="Anamoly Detected"
            print ("Anamoly Detected in heart rate;;", df['heart_rate'][i], "is over mean + three times standard deviation") 
            hr_data.append(df['heart_rate'][i])
        elif  df['heart_rate'][i] <= level_6:
            message_2 ="Anamoly Detected"
            print ("Anamoly Detected in heart rate;", df['heart_rate'][i], "is below mean - three times standard deviation") 
            hr_data.append(df['heart_rate'][i])
    hr_data=pd.DataFrame({'hr_data': hr_data})
    hr_data=hr_data.set_index("hr_data").T
    hr_data.to_csv(FILE_PATH_OUTPUT+"HR_Anomaly.csv")
    

    o2_mean=96.19
    o2_sd=1.69
    level_33 = o2_mean + 3 * o2_sd
    level_66 = o2_mean - 3 * o2_sd
    for i in range(len(df)): 
        if  df['o2_level'][i] >= level_33:
            message_1="Anamoly Detected"
            print ("Anamoly Detected in O2 level;", df['o2_level'][i], "is over mean + three times standard deviation") 
            o2_data.append(df['heart_rate'][i])
        elif  df['o2_level'][i] <= level_66:
            message_2 ="Anamoly Detected"
            print ("Anamoly Detected in O2 level;", df['o2_level'][i], "is below mean - three times standard deviation") 
            o2_data.append(df['heart_rate'][i])
    o2_data=pd.DataFrame({'o2_data': o2_data})
    o2_data=o2_data.set_index("o2_data").T
    o2_data.to_csv(FILE_PATH_OUTPUT+"O2_Anomaly.csv")
        
# --------------------------------
# DAG definition
# --------------------------------

# Define the defualt args

default_args = {    
    'owner':'user_name',
    'start_date':datetime(2022, 10, 9),
    'depends_on_past':False,
    'email': ['gbengwyzime@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

# Create the DAG object
with DAG(
    'dag_code',
    default_args=default_args,
    description='dag_code',
    schedule_interval='*/15 * * * *',
    catchup=False    
) as dag:

    # start_task
        start_task = DummyOperator(
        task_id='start',
        dag=dag
    )

    # file_sensor_task
        file_sensor_task = FileSensor(
        task_id='file_sensor',
        poke_interval=15,
        filepath=FILE_PATH_INPUT,
        timeout=5,
        dag=dag
    )

    # send_report_task
        send_report_task = PythonOperator(
        task_id='send_report_task',
        python_callable=send_report,
        dag=dag
    )
    # flag_anomaly_task
        flag_anomaly_task = PythonOperator(
        task_id='flag_anomaly_task',
        python_callable=flag_anomaly,
        dag=dag
    )    
    # end_task
        end_task = DummyOperator(
        task_id='end_task',
        dag=dag
    )
# Set the dependencies
start_task >> file_sensor_task >> [send_report_task, flag_anomaly_task] >> end_task
# --------------------------------