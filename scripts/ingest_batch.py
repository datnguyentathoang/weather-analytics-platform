import os
import json
import pandas as pd
from datetime import datetime

OUTPUT_DIR = "data_sample"
LOCAL_DATALAKE_PATH = r"D:\weather_datalake\weather-raw\batch"

def get_wind_direction(degree):
    directions = ["Bắc", "Đông Bắc", "Đông", "Đông Nam", "Nam", "Tây Nam", "Tây", "Tây Bắc"]
    return directions[round(degree / 45) % 8]

def ingest_cities_dataset_to_csv():
    # Tự động tạo cây thư mục lưu trữ Data Lake trên ổ D
    os.makedirs(LOCAL_DATALAKE_PATH, exist_ok=True)
    print("🌦️ [ĐẠT - INGESTION] Bắt đầu bóc tách dữ liệu offline toàn quốc sang file CSV...")
    
    # ĐỊNH VỊ ĐƯỜNG DẪN CHUẨN: Tự động lùi thư mục nếu Đạt đang đứng ở /scripts
    current_dir = os.path.basename(os.getcwd())
    target_input = OUTPUT_DIR if current_dir != "scripts" else os.path.join("..", OUTPUT_DIR)
    
    if not os.path.exists(target_input):
        print(f"❌ Không tìm thấy thư mục nguồn dữ liệu mồi: {target_input}")
        print("💡 Đạt hãy chạy file crawl_data.py trước để sinh file mồi nhé!")
        return
        
    # VÁ LỖI CỐT LÕI 1: Quét sạch file raw trong thư mục chuẩn target_input
    all_files = [f for f in os.listdir(target_input) if f.endswith("_raw.json") and f.startswith("CITY_")]
    
    if not all_files:
        print(f"❌ Không tìm thấy file nguồn CITY_..._raw.json nào trong thư mục {target_input}!")
        return

    for file_name in all_files:
        city_key = file_name.replace("_raw.json", "")
        raw_file_path = os.path.join(target_input, file_name)
        
        print(f"📖 Đang xử lý bóc tách hàng/cột dữ liệu lớn cho: {city_key}...")
        with open(raw_file_path, "r", encoding="utf-8") as f:
            raw_json = json.load(f)
            
        raw_hourly = raw_json.get("hourly", {})
        records = []
        total_points = len(raw_hourly.get("time", []))
        
        for i in range(total_points):
            time_str = raw_hourly["time"][i]
            time_obj = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
            
            # VÁ LỖI CỐT LÕI 2: Đồng bộ chuẩn xác tên khóa trường đo khớp 100% với bộ sinh offline
            records.append({
                "station_key": city_key,
                "ThoiGian": time_obj.strftime("%d-%m-%Y %H:%M:%S"),
                "NhietDo": round(raw_hourly.get("temperature_2m", [])[i] if raw_hourly.get("temperature_2m") else 0.0, 1),
                "DoAm": round(raw_hourly.get("relative_humidity_2m", [])[i] if raw_hourly.get("relative_humidity_2m") else 0),
                "CamGiacNhu": round(raw_hourly.get("apparent_temperature", [])[i] if raw_hourly.get("apparent_temperature") else 0.0, 1),
                "LuongMua": round(raw_hourly.get("precipitation", [])[i] if raw_hourly.get("precipitation") else 0.0, 1),
                "TocDoGio": round(raw_hourly.get("wind_speed_10m", [])[i] if raw_hourly.get("wind_speed_10m") else 0.0, 1),
                "ApSuat": round((raw_hourly.get("pressure_msl", [])[i] if raw_hourly.get("pressure_msl") else 0) / 33.864, 3)
            })
            
        # Ghi bảng phẳng DataFrame ra file CSV sạch
        df = pd.DataFrame(records)
        file_dest = os.path.join(LOCAL_DATALAKE_PATH, f"{city_key}_historical.csv")
        df.to_csv(file_dest, index=False, encoding="utf-8-sig")
        
        # Tính toán dung lượng thực tế in ra màn hình
        file_size_mb = os.path.getsize(file_dest) / (1024 * 1024)
        print(f"✅ Tạo file CSV thành công: {file_dest} | Kích thước: {file_size_mb:.2f} MB | Tổng số dòng: {len(df)}")
        
    print("\n🏁 [HOÀN THÀNH XUẤT SẮC LAYER 1] Data Lake của Đạt đã gom đủ tệp dữ liệu CSV quy mô lớn!")

if __name__ == "__main__":
    ingest_cities_dataset_to_csv()
