import uuid
import datetime

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_AZ = 'ru-central1-b'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDXLeWVKZ6U7/il2fGrqWPnGrMRZfWu/z8fO+ViFwMEpAyoZIO+y+gmKi756XCB/JAJk/v2is8TVbFplENZgFnlyqiPf+cbVfbXIGkihE6IE2YZZoUI/hmE3KABdFeICEiPOT/1SAwhdaKoWX2kD5at0ylgyA2yIgFLWWy5HYppKv9kFydvsw69hG1ooUu3ZsOL/xwXgp7BdG1INRv5OE12ohGJGYKwKh2j3s+odRDGoHOsiboKmPk3joUG53205EECEPMd2BYR3ppsH5hA2UyajAFoXTAtRMVGK7P3KrWx0SkUlXdaF6C3lsXJ+fZPrYYBD5nOHicBTMdo7z7F3Zw+z1lnTyrq+XtcZJgWYnVU499p/ka+KJbq0Y9E0vWo2trtOL5MQn/7xoZPD707+KO3YUDS4F1jz3I9p/aMQ9sfWU8rGwznmgd73edCbOZj//zlJ+gSZJSorWcyjoEs238jdEpShekSbN1xANrybBcmN+u4mGYwwN0XS5A3nKaMTzqo/CQx/Xp4Ug4tDGJ2+bcmu2WUy1bpTGKMcnOH1fORGzVuTrdbivJCRT3FRtKsKelWuno8VoNHQHpbzqB+d2t32uoYHv5QnFXbteXC7B/zIn+CqGzyVImNV2TfLeMJ8HCKVVsouXBUAy5SkWrJ7MXKCBSOMgqtnX1qXS9MvcwX/w== tim.serazutdinov@yandex.ru'
YC_DP_SUBNET_ID = 'e2l6qu6l1vsebuneslup'
YC_DP_SA_ID = 'ajegfv6tr4ao25hnk2vb'
YC_BUCKET = 'exam-dataproc-result'

# DAG SETTINGS
with DAG(
        'DATA_INGEST',
        schedule_interval='@daily',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для PySpark-задания',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,       
        subnet_id=YC_DP_SUBNET_ID,            
        s3_bucket=YC_BUCKET,                 
        zone=YC_DP_AZ,                        
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-hdd',
        masternode_disk_size=32,
        computenode_resource_preset='s2.small',
        computenode_disk_type='network-hdd',
        computenode_disk_size=32,
        computenode_count=1, 
        computenode_max_hosts_count=3,
        services=['YARN', 'SPARK'],
        datanode_count=0,
    )

    spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/code/job.py',
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> spark_processing >> delete_spark_cluster