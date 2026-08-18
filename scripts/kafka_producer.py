import os
import time
import json
import requests
import random
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

# Tải cấu hình bảo mật từ hệ thống .env
env_path = os.path.join(os.path.dirname(__file__), '../deployments/.env')
load_dotenv(dotenv_path=env_path)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "weather_realtime")
API_KEY = os.getenv("WEATHER_API_KEY")

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    print(f"📡 [ĐẠT - STREAM] Kết nối thành công Kafka Broker: {KAFKA_BROKER}")
except Exception as e:
    print(f"❌ Thất bại khi kết nối tới hạ tầng Kafka: {e}")
    exit(1)

def fetch_live_stream_data():
    """Gọi API thương mại lấy mảng dữ liệu sống làm mồi phát sự kiện"""
    url = f"https://open-meteo.com{API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get("hourly", {})
    return None

def start_streaming():
    hourly_buffer = fetch_live_stream_data()
    if not hourly_buffer:
        print("⚠️ Không lấy được data live, chuyển về chế độ giả lập cục bộ...")
        hourly_buffer = {"temperature_2m": [25.0]*24, "relative_humidity_2m": [80]*24, "precipitation": [0.0]*24, "wind_speed_10m": [5.0]*24, "time": ["12:00"]*24}

    stations = ["STN_HANOI", "STN_SAIGON", "STN_DANANG"]
    total_slots = len(hourly_buffer.get("temperature_2m", []))
    print(f"⚡ Bắt đầu phát sóng luồng sự kiện Live Stream vào Kafka Topic: {TOPIC_NAME}")

    idx = 0
    while True:
        try:
            current_slot = idx % total_slots
            selected_station = random.choice(stations)
            
            weather_event = {
                "station_key": selected_station,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "NhietDo": round(hourly_buffer["temperature_2m"][current_slot] + random.uniform(-0.5, 0.5), 1),
                "DoAm": int(hourly_buffer["relative_humidity_2m"][current_slot]),
                "LuongMua": round(hourly_buffer["precipitation"][current_slot], 1),
                "TocDoGio": round(hourly_buffer["wind_speed_10m"][current_slot], 1)
            }
            
            # Cố tình bẫy sự kiện thời tiết cực đoan (Lượng mưa > 100mm) để Thiên viết Spark Streaming bắt lỗi kiếm điểm cộng
            if random.random() > 0.96:
                weather_event["LuongMua"] = round(random.uniform(95.0, 150.0), 1)
                print("🚨 [CẢNH BÁO] Giả lập xuất hiện hiện tượng mưa lũ cực đoan lớn!")

            producer.send(TOPIC_NAME, value=weather_event)
            print(f"📡 [PRODUCER] -> Phát hành sự kiện trạm: {weather_event['station_key']} | Nhiệt độ: {weather_event['NhietDo']}°C")
            
            time.sleep(2)  # Cứ 2 giây sinh sự kiện 1 lần
            idx += 1
        except KeyboardInterrupt:
            print("\n🛑 Đã dừng luồng Kafka Producer.")
            break

if __name__ == "__main__":
    start_streaming()
