"""
EDA Timing Report Auto-Ingestion DAG

Monitors for new timing reports and automatically:
1. Detects new files
2. Parses timing data
3. Saves to PostgreSQL
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import glob
import os
import requests

default_args = {
    'owner': 'eda-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

def scan_new_reports(**context):
    """Scan for new timing reports"""
    reports_dir = "/opt/airflow/demo_data/reports"
    
    if not os.path.exists(reports_dir):
        print(f"⚠️  Reports directory not found: {reports_dir}")
        return []
    
    files = glob.glob(f"{reports_dir}/timing_report_*.txt")
    files.sort(key=os.path.getmtime, reverse=True)
    
    print(f"📁 Found {len(files)} report files")
    
    # Get latest file
    if files:
        latest = files[0]
        print(f"✅ Latest report: {os.path.basename(latest)}")
        context['ti'].xcom_push(key='latest_report', value=latest)
        return latest
    else:
        print("⚠️  No reports found")
        return None

def parse_timing_report(**context):
    """Parse timing report via FastAPI"""
    ti = context['ti']
    report_file = ti.xcom_pull(task_ids='scan_reports', key='latest_report')
    
    if not report_file:
        print("⚠️  No report file to parse")
        return None
    
    print(f"🔍 Parsing: {report_file}")
    
    # Call FastAPI
    api_url = "http://analysis-api:8000/analysis/timing"
    
    try:
        response = requests.post(
            api_url,
            json={"file_path": report_file},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Parsed successfully:")
            print(f"   Run ID: {result['run_id']}")
            print(f"   Violations: {result['violations']['total']}")
            print(f"   Worst Slack: {result['worst_slack_ns']}ns")
            print(f"   Saved to DB: {result['saved_to_db']}")
            
            ti.xcom_push(key='parse_result', value=result)
            return result
        else:
            print(f"❌ API error: {response.status_code}")
            print(response.text)
            return None
    
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to analysis-api")
        print("   Make sure the service is running")
        return None
    
    except Exception as e:
        print(f"❌ Parse error: {e}")
        return None

def log_result(**context):
    """Log parsing result"""
    ti = context['ti']
    result = ti.xcom_pull(task_ids='parse_report', key='parse_result')
    
    if result:
        violations = result['violations']['total']
        if violations > 0:
            print(f"⚠️  WARNING: {violations} timing violations detected!")
        else:
            print("✅ No violations - design meets timing!")
        
        print(f"💾 Data saved to PostgreSQL (ID: {result.get('db_id')})")
    else:
        print("⚠️  No result to log")

# Define DAG
with DAG(
    dag_id='eda_auto_ingest',
    default_args=default_args,
    description='Auto-ingest EDA timing reports to database',
    schedule=timedelta(minutes=1),  # Changed from schedule_interval (Airflow 3.x)
    start_date=datetime(2026, 1, 20),
    catchup=False,
    tags=['eda', 'timing', 'auto-ingest'],
) as dag:
    
    # Task 1: Scan for new reports
    task_scan = PythonOperator(
        task_id='scan_reports',
        python_callable=scan_new_reports,
    )
    
    # Task 2: Parse timing report
    task_parse = PythonOperator(
        task_id='parse_report',
        python_callable=parse_timing_report,
    )
    
    # Task 3: Log result
    task_log = PythonOperator(
        task_id='log_result',
        python_callable=log_result,
    )
    
    # Define task dependencies
    task_scan >> task_parse >> task_log
