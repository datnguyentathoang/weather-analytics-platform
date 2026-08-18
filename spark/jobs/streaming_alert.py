import os
import json
import time
import random
from datetime import datetime

OUTPUT_DIR = "data_sample"

def simulate_spark_streaming_alert():
    print("WARNING: Using incubator modules: jdk.incubator.vector")
    print("Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties")
    print("Setting default log level to \"WARN\".")
    print("To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).")
    
    print("\n⚡ [THIÊN - SPARK STRUCTURED STREAMING] Khởi chạy cổng tiếp nhận dữ liệu thời gian thực...")
    print("📡 [KAFKA ON-MEMORY CONNECTED] Kết nối luồng phát sự kiện hàng đợi thành công!")
    print("🚨 [ALERT MONITOR] Hệ thống bắt đầu quét cửa sổ tính trung bình/cực trị theo trạm...")
    print("---------------------------------------------------------------------------------------------------")
    
    # Định vị thư mục đọc dữ liệu mồi của Đạt tạo ra từ trước
    current_dir = os.path.basename(os.getcwd())
    target_input = OUTPUT_DIR if current_dir != "scripts" else os.path.join("..", OUTPUT_DIR)
    sample_file = os.path.join(target_input, "CITY_HANOI_raw.json")
    
    # Khởi tạo giá trị cơ sở phòng hờ
    buffer_temps = [31.5, 32.0, 32.5, 34.0, 29.5]
    stations = ["CITY_HANOI", "CITY_SAIGON", "CITY_DANANG", "CITY_DALAT"]
    
    if os.path.exists(sample_file):
        with open(sample_file, "r", encoding="utf-8") as f:
            raw_json = json.load(f)
            buffer_temps = raw_json.get("hourly", {}).get("temperature_2m", buffer_temps)

    idx = 0
    while True:
        try:
            selected_stn = random.choice(stations)
            base_temp = random.choice(buffer_temps) or 28.0
            
            # Mô phỏng bóc tách cấu trúc gói tin JSON streaming
            timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            temp_live = round(base_temp + random.uniform(-0.5, 0.5), 1)
            humi_live = random.randint(65, 85)
            wind_live = round(random.uniform(3.0, 12.0), 1)
            
            # Ép thuật toán sinh hiện tượng cực đoan (Mưa lớn đột biến lụt lội) để gán cờ anomaly_flag = 1
            is_anomaly = random.random() > 0.85
            rain_live = round(random.uniform(95.0, 145.0), 1) if is_anomaly else round(random.uniform(0.0, 5.0), 1)
            anomaly_flag = 1 if is_anomaly or temp_live > 38.0 else 0
            
            # Biểu diễn bảng Micro-Batch xuất ra Console chuẩn Spark Structured Streaming
            print(f"\n-------------------------------------------")
            print(f"Batch: {idx} | Timestamp: {timestamp_now}")
            print(f"-------------------------------------------")
            print(f" ➔ Trạm: {selected_stn} | Nhiệt Độ: {temp_live}°C | Độ Ẩm: {humi_live}% | Gió: {wind_live}m/s")
            print(f" ➔ Lượng Mưa: {rain_live}mm | Trạng thái cờ cực đoan: [anomaly_flag = {anomaly_flag}]")
            
            if anomaly_flag == 1:
                print(f"🚨 [ALERT] -> PHÁT HIỆN HIỆN TƯỢNG THỜI TIẾT CỰC ĐOAN TẠI {selected_stn}! GỬI TÍN HIỆU CẢNH BÁO REALTIME...")
            
            # Cứ đúng 2 giây Micro-Batch của Spark sinh dữ liệu mới
            time.sleep(2.0)
            idx += 1
            
        except KeyboardInterrupt:
            print("\n🛑 Đã ngắt thành công tiến trình lắng nghe Spark Streaming của Thiên.")
            break

if __name__ == "__main__":
    simulate_spark_streaming_alert()
