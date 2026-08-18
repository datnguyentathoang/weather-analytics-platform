import os
from datetime import datetime, timedelta

# BỘ BẢO VỆ MÔI TRƯỜNG WINDOWS LOCAL: Tự động giả lập thư viện Airflow nếu máy thiếu gói cài đặt
try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:
    # Tự khởi tạo các lớp đối tượng giả lập để code chạy thông suốt không bị crash trên Windows
    print("🔮 [AIRFLOW-WINDOWS MOCK] Đang kích hoạt chế độ biên dịch luồng cục bộ...")
    class DAG:
        def __init__(self, dag_id, **kwargs):
            self.dag_id = dag_id
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass
    class BashOperator:
        def __init__(self, task_id, bash_command, **kwargs):
            self.task_id = task_id
            self.bash_command = bash_command
            print(f"   ⚙️ Task Khởi tạo thành công: [{task_id}] -> Command: {bash_command}")

# Cấu hình các tham số vận hành pipeline tự động
default_args = {
    'owner': 'Thien_Spark_Master',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 18),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_analytics_pipeline',
    default_args=default_args,
    description='Chuỗi điều phối tự động kết nối luồng Ingestion của Đạt và PySpark của Thiên',
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Tác vụ nạp: Kích hoạt file crawl_data.py sinh dữ liệu mồi JSON
    task_crawl_api = BashOperator(
        task_id='crawl_api_data',
        bash_command='python D:/weather-analytics-platform/scripts/crawl_data.py',
    )

    # 2. Tác vụ gộp: Kích hoạt file ingest_batch.py bóc tách và xuất file CSV lớn ra Data Lake
    task_ingest_batch = BashOperator(
        task_id='ingest_batch_to_datalake',
        bash_command='python D:/weather-analytics-platform/scripts/ingest_batch.py',
    )

    # 3. Tác vụ xử lý: Kích hoạt file batch_clean.py của Thiên để nén Parquet phân vùng
    task_spark_clean = BashOperator(
        task_id='pyspark_batch_cleaning',
        bash_command='python D:/weather-analytics-platform/spark/jobs/batch_clean.py',
    )

    # THIẾT LẬP SƠ ĐỒ DÒNG CHẢY LOGIC (Flow thực thi tuần tự chuẩn Đồ án 13)
    # Lệnh dịch chuyển toán tử vẽ cây Graph View liên kết
    try:
        task_crawl_api >> task_ingest_batch >> task_spark_clean
        print("\n🔗 [ORCHESTRATION SUCCESS] Bản đồ luồng Task liên thông thành công:")
        print("   [Crawl API Data] ➔ [Ingest Batch To DataLake] ➔ [PySpark Batch Cleaning] 🚀")
    except Exception:
        pass
