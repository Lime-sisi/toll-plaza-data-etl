from airflow.models import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "../../data/tolldata.tgz")

SOURCE_TGZ_FILE = DATA_FILE
EXTRACTION_PATH = os.path.join(BASE_DIR, "../../data/")

VEHICLE_START_CSV = EXTRACTION_PATH + 'vehicle-data.csv'
TOLLPLAZA_START_TSV = EXTRACTION_PATH + 'tollplaza-data.tsv'
PAYMENT_START_TXT = EXTRACTION_PATH + 'payment-data.txt'

VEHICLE_CLEANED_CSV = EXTRACTION_PATH + 'csv_data.csv'
TOLLPLAZA_CLEANED_CSV = EXTRACTION_PATH + 'tsv_data.csv'
PAYMENT_CLEANED_CSV = EXTRACTION_PATH + 'fixed_width_data.csv'

EXTRACTED_CSV = EXTRACTION_PATH + 'extracted_data.csv'
TRANSFORMED_CSV = EXTRACTION_PATH + 'transformed_data.csv'

default_args={
    'owner' : 'username',
    'start_date' : days_ago(0),
    'email' : ['your_email@example.com'],
    'email_on_failure' : True,
    'email_on_retry': True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

dag= DAG(
    'toll_plaza_data_pipeline',
    default_args = default_args,
    schedule_interval = timedelta(days=1),
    description = 'tolldata ETL pipeline with Apache Airflow',
)

unzip_data = BashOperator(    
    task_id = 'unzip',
    bash_command=f'tar -xzf {SOURCE_TGZ_FILE} -C {EXTRACTION_PATH}',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id = 'extract_csv',
    bash_command = (f'cut -d "," -f 1,2,3,4 {VEHICLE_START_CSV} > {VEHICLE_CLEANED_CSV}'
    f' && echo {VEHICLE_CLEANED_CSV}'),  
    do_xcom_push=True,
    dag = dag,
)

extract_data_from_tsv = BashOperator(   
    task_id='extract_tsv',
    bash_command = (f"cut -f 5,6,7 {TOLLPLAZA_START_TSV} "
    f"| tr '\t' ',' | sed 's/\\r$//'  > {TOLLPLAZA_CLEANED_CSV}"),  
    dag=dag,
)

extract_data_from_fixed_width = BashOperator( 
    task_id='extract_fixed',
    bash_command = (f'cut -c 59-67 {PAYMENT_START_TXT}'
    f'| tr " " "," > {PAYMENT_CLEANED_CSV}'),
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate',
    bash_command = (f'paste -d "," '
    f'{{{{ ti.xcom_pull(task_ids=\'extract_csv\') }}}} '
    f'{TOLLPLAZA_CLEANED_CSV} {PAYMENT_CLEANED_CSV} > {EXTRACTED_CSV}'),
    dag=dag,
)   

transform_data = BashOperator( 
    task_id='transform',
    bash_command = (f'awk -F"," \'BEGIN{{OFS=","}} {{$4=toupper($4);print}}\' '
    f'{EXTRACTED_CSV} > {TRANSFORMED_CSV}'),
    dag=dag,
)  

unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] \
 >> consolidate_data >> transform_data



