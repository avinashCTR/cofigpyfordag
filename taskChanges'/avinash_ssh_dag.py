from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta
import textwrap

default_args = {
    'owner': 'couture',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries':0,
    'config_group':"config_group_jiomart"
}


# gpu commands
env_command = "conda activate /data/archita/searchengine_nlp"
bash_command = textwrap.dedent(
        f"""
        {env_command} && /app/notebooks/avinash/ipa-task.sh \
        --w2r-path "/data1/searchengine/processed/jiomart/05092024/V6_delta_changes/Unified3VerticalsSelectAttributes/W2RAllVariantsCleaned" \
        --ipa-meta-path "/data1/searchengine/processed/jiomart/accumulateddata/IPATransliterationsAutomaticAccumulator20250117" \
        --w2r-scored-path "/data1/archive/avinash/W2RWithIPATransliterations.parquet" \
        --ipa-meta-output-path "/data1/archive/avinash/IPATransliterationsAutomaticAccumulator_avinash.parquet" \
        --cache-path "/data1/archive/avinash/CACHE" \
        --max-ipa-computations 10000
        """
)

dag = DAG('avinash_ssh_dag', default_args=default_args, schedule_interval=timedelta(days=1))


t1 = SSHOperator(
    task_id="ssh_task",
    ssh_conn_id="ssh_default",
    command=f"""ssh couture@JMNGD1BAE210V10 "{bash_command}" """,
    dag=dag
)
