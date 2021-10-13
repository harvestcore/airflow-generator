from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from .generator import generate_airflows

### Python operator callables

def check_python_dependencies():
    import subprocess
    import sys
    dependencies = ['pandas', 'pymongo', 'urllib']

    for dep in dependencies:
        try:
            exec(f"import {dep}")
        except:
            subprocess.check_call([
                sys.executable, "-m", "pip", "install",
                dep
            ])

def process_csv():
    import pandas as pd

    temperature = pd.read_csv(
        '/tmp/p2/csv/temperature.csv',
        header=0
    )
    
    humidity = pd.read_csv(
        '/tmp/p2/csv/humidity.csv',
        header=0
    )

    frame = pd.DataFrame(data={
        'DATE': temperature['datetime'],
        'TEMP': temperature['San Francisco'],
        'HUM': humidity['San Francisco']
    })

    frame.to_csv(
        '/tmp/p2/mongo/sanfrancisco.csv',
        sep=',',
        encoding='utf-8',
        index=False
    )


FLOWS = [
    {
        'flow_id': 'check_dependencies',
        'tasks': [
            {
                'id': 'check_python_dependencies',
                'type': PythonOperator,
                'params': {
                    'python_callable': check_python_dependencies
                }
            },
            {
                'id': 'create_root_folder',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        rm -rf /tmp/p2/* && \
                        mkdir -p /tmp/p2 && \
                        mkdir -p /tmp/p2/csv'
                }
            }
        ]
    },
    {
        'flow_id': 'download_data',
        'depends_on': 'create_root_folder',
        'tasks': [
            {
                'id': 'download_csv_data',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2/csv && \
                        wget https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip && \
                        wget https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip'
                }
            },
            {
                'id': 'show_folder',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2/csv && \
                        ls -lah'
                }
            },
            {
                'id': 'unzip_csv_data',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2/csv && \
                        unzip -u humidity.csv.zip && \
                        unzip -u temperature.csv.zip'
                }
            },
            {
                'id': 'download_repo',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2 && \
                        git clone https://github.com/harvestcore/cc2'
                }
            },
            {
                'id': 'extract_apis',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2 && \
                        ls -lah && \
                        mv cc2/p2/api . && \
                        mv cc2/p2/mongo .'
                }
            }
        ]
    },
    {
        'flow_id': 'process_data',
        'depends_on': 'extract_apis',
        'tasks': [
            {
                'id': 'process_csv',
                'type': PythonOperator,
                'params': {
                    'python_callable': process_csv
                }
            },
            {
                'id': 'run_mongo_database',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2/mongo && \
                        docker-compose down && \
                        docker-compose up -d'
                }
            }
        ]
    },
    {
        'flow_id': 'test_api',
        'depends_on': 'run_mongo_database',
        'tasks': [
            {
                'id': 'parallel_branch_api',
                'parallel': [
                    {
                        'id': 'train_api_v1_data',
                        'type': BashOperator,
                        'params': {
                            'bash_command': '\
                                cd /tmp/p2/api/v1 && \
                                if test -f "/data/humidity.pkl"; then cp /data/humidity.pkl ./data; fi && \
                                if test -f "/data/temperatures.pkl"; then cp /data/temperatures.pkl ./data; else \
                                    pip3 install -r requirements.txt && python3 train.py && cp *.pkl /data; fi'
                        }
                    },
                    {
                        'id': 'build_test_api_v2',
                        'type': BashOperator,
                        'params': {
                            'bash_command': '\
                                cd /tmp/p2/api/v2 && \
                                docker-compose down && \
                                docker build . -f Dockerfile.test -t test-api-v2:latest'
                        }
                    }
                ]
            }
        ]
    },
    {
        'flow_id': 'test_and_deploy_api_v1',
        'depends_on': 'train_api_v1_data',
        'tasks': [
            {
                'id': 'build_test_api_v1',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2/api/v1 && \
                        docker build . -f Dockerfile.test -t test-api-v1:latest'
                }
            },
            {
                'id': 'test_api_v1',
                'type': BashOperator,
                'params': {
                    'bash_command': 'docker run test-api-v1:latest'
                }
            },
            {
                'id': 'build_image_api_v1',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2/api/v1 && \
                        docker-compose down && \
                        docker-compose build'
                }
            },
            {
                'id': 'run_image_api_v1',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2/api/v1 && \
                        docker-compose up -d'
                }
            }
        ]
    },
    {
        'flow_id': 'test_and_deploy_api_v2',
        'depends_on': 'build_test_api_v2',
        'tasks': [
            {
                'id': 'test_api_v2',
                'type': BashOperator,
                'params': {
                    'bash_command': 'docker run test-api-v2:latest'
                }
            },
            {
                'id': 'build_image_api_v2',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2/api/v2 && \
                        docker-compose down && \
                        docker-compose build'
                }
            },
            {
                'id': 'run_image_api_v2',
                'type': BashOperator,
                'params': {
                    'bash_command': '\
                        cd /tmp/p2/api/v2 && \
                        docker-compose up -d'
                }
            }
        ]
    }
]

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id = 'deploy_v1_v2',
    default_args = DEFAULT_ARGS,
    description = 'Deploy API v1 and v2',
    dagrun_timeout = timedelta(minutes=2),
    schedule_interval = timedelta(days=1),
)

out = generate_airflows(dag, FLOWS)

for flow in out:
    next(out)