from datetime import datetime, timedelta
from textwrap import dedent
# Objeto DAG
from airflow import DAG
# Operadores
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
# Esta es la definicion de DAG, los args son pasados a cada operador
with DAG(
    'airflow_test',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='DAG de prueba',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 8, 21),
    catchup=False,
    tags=['prueba'],
) as dag:
    # t1, t2, t3, t4 y t5 son tareas creadas al instanciar operadores
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    t1.doc_md = dedent(
        """\
    #### Doc de tarea
    Esta es una tarea
    """
    )
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )
    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )
    t4 = DummyOperator(task_id='t4_dummy')
    t5 = DummyOperator(task_id='t5_dummy')

    t1 >> [t2, t3]
    t3 >> t4
    t3 >> t5
