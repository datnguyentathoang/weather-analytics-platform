import os
import glob
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when

# 1. Khởi tạo cụm Spark Session local tối ưu hóa bộ đệm
spark = SparkSession.builder \
    .appName("Weather-Batch-Processing") \
    .master("local[*]") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

# Cấu hình chính xác đường dẫn hạ tầng Data Lake ổ đĩa cứng
RAW_DATA_PATH = r"D:\weather_datalake\weather-raw\batch"
PROCESSED_DATA_PATH = r"D:\weather_datalake\weather-processed\curated_weather"

def clean_and_curate_batch_win_safe():
    print("⚡ [THIÊN - SPARK] Đang kích hoạt tiến trình xử lý dữ liệu lớn...")
    
    # Quét tệp thông minh bypass triệt để cảnh báo winutils.exe của Hadoop
    csv_files = glob.glob(os.path.join(RAW_DATA_PATH, "*.csv"))
    
    if not csv_files:
        print(f"❌ Không tìm thấy tệp dữ liệu .csv nào trong thư mục: {RAW_DATA_PATH}")
        return
        
    print(f"📖 Thư viện đĩa tìm thấy {len(csv_files)} tệp CSV thô. Tiến hành gộp dữ liệu...")
    
    # Đọc gộp dữ liệu bằng Pandas để tránh đụng độ NativeIO hệ thống
    pandas_df_list = [pd.read_csv(f) for f in csv_files]
    combined_pandas_df = pd.concat(pandas_df_list, ignore_index=True)
    
    # Nạp dữ liệu vào bộ nhớ Spark Session để dọn dẹp logic
    df_raw = spark.createDataFrame(combined_pandas_df)
    print(f"📊 Tổng số bản ghi thô thu nhận vào bộ nhớ Spark: {df_raw.count()} dòng.")

    # 2. XỬ LÝ ĐIỂM KHUYẾT (处理 point 缺少)
    avg_temp = 25.0
    avg_humi = 75.0
    
    df_cleansed = df_raw \
        .withColumn("NhietDo", when(col("NhietDo").isNull(), avg_temp).otherwise(col("NhietDo"))) \
        .withColumn("DoAm", when(col("DoAm").isNull(), avg_humi).otherwise(col("DoAm"))) \
        .withColumn("LuongMua", when(col("LuongMua").isNull(), 0.0).otherwise(col("LuongMua")))

    # 3. CHUẨN HÓA KHÓA PHÂN VÙNG (Partition Keys)
    # Ép chuỗi ThoiGian 'DD-MM-YYYY HH:MM:SS' sang kiểu dữ liệu Date chuẩn
    df_final = df_cleansed \
        .withColumn("obs_date", to_date(col("ThoiGian"), "dd-MM-yyyy HH:mm:ss")) \
        .withColumn("region", col("station_key"))

    # 4. KẾT XUẤT FILE NÉN PARQUET PHÂN VÙNG VƯỢT GIỚI HẠN ĐĨA CỨNG
    print(f"📦 Đang tiến hành nén cột và kết xuất kho Parquet phân vùng...")
    
    final_pandas_df = df_final.toPandas()
    
    # BẺ KHÓA LỖI 1366 PARTITIONS: Tăng trần max_partitions lên hẳn 5000 để PyArrow xử lý mượt mà
    final_pandas_df.to_parquet(
        PROCESSED_DATA_PATH,
        engine="pyarrow",
        compression="snappy",
        partition_cols=["obs_date", "region"],
        max_partitions=5000  # Đảm bảo cân trọn gói 3.5 năm dữ liệu của Đạt không lo sập hệ thống
    )
        
    print(f"✅ [THÀNH CÔNG RỰC RỠ] Đã tạo kho Parquet phân vùng lớn tại: {PROCESSED_DATA_PATH}")

if __name__ == "__main__":
    try:
        clean_and_curate_batch_win_safe()
    finally:
        # Tắt Spark an toàn để giải phóng RAM cho máy tính
        spark.stop()
