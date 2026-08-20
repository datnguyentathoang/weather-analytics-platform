import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# 1. Kết nối Kho dữ liệu PostgreSQL
DB_URL = "postgresql://postgres:adminweather123@localhost:5432/weather_db"
engine = create_engine(DB_URL)

print("🔄 [AI/ML] Đang truy vấn dữ liệu từ fact_observation...")
query = """
    SELECT 
        observation_timestamp AS ds,
        temperature AS y,
        station_key
    FROM fact_observation 
    WHERE station_key = 'CITY_HANOI'
    ORDER BY observation_timestamp ASC
"""
df = pd.read_sql(query, engine)

if df.empty:
    print("❌ Không có dữ liệu trong bảng fact_observation!")
    exit()

print(f"✅ Đã tải {len(df)} bản ghi chuỗi thời gian nhiệt độ Hà Nội.")

# 2. Huấn luyện mô hình Prophet
try:
    from prophet import Prophet
    print("🤖 [AI/ML] Khởi chạy mô hình Prophet...")
    
    # Ép kiểu múi giờ để Prophet xử lý chính xác
    df['ds'] = pd.to_datetime(df['ds']).dt.tz_localize(None)
    
    model = Prophet(daily_seasonality=True, weekly_seasonality=True)
    model.fit(df)

    # Dự báo 7 ngày tiếp theo (168 giờ)
    future = model.make_future_dataframe(periods=168, freq='H')
    forecast = model.predict(future)

    # 3. Vẽ biểu đồ dự báo
    fig = model.plot(forecast)
    plt.title("Dự báo Nhiệt độ 7 ngày tới tại Hà Nội (Prophet Model)", fontsize=14)
    plt.xlabel("Thời gian")
    plt.ylabel("Nhiệt độ (°C)")

    os.makedirs("analytics_ai", exist_ok=True)
    output_path = "analytics_ai/temperature_forecast.png"
    plt.savefig(output_path, bbox_inches='tight', dpi=300)
    print(f"📈 [THÀNH CÔNG] Đã lưu biểu đồ dự báo tại: {output_path}")

    # 4. Xuất bảng dự báo ra CSV để phục vụ Dashboard BI
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(168).to_csv(
        "analytics_ai/forecast_results.csv", index=False
    )
    print("💾 Đã xuất kết quả dự báo ra analytics_ai/forecast_results.csv")

except Exception as e:
    print(f"⚠️ Lỗi khi chạy Prophet: {e}\nĐang chuyển sang mô hình RandomForest fallback...")
    
    # Dự phòng với Scikit-Learn
    from sklearn.ensemble import RandomForestRegressor
    df['ds'] = pd.to_datetime(df['ds'])
    df['hour'] = df['ds'].dt.hour
    df['dayofweek'] = df['ds'].dt.dayofweek
    
    X = df[['hour', 'dayofweek']]
    y = df['y']
    
    rf = RandomForestRegressor(n_estimators=100, random_state=42)
    rf.fit(X, y)
    print("✅ Dự báo đơn giản bằng Random Forest hoàn tất!")