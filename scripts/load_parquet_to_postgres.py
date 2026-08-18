import os
import sys
import glob
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Định vị chính xác thư mục gốc dự án
ROOT_DIR = Path(__file__).resolve().parent.parent

# 2. Tìm tệp .env (ưu tiên deployments/.env theo cấu trúc dự án của bạn, sau đó đến .env ở thư mục gốc)
ENV_PATH = ROOT_DIR / "deployments" / ".env"
if not ENV_PATH.exists():
    ENV_PATH = ROOT_DIR / ".env"

if not ENV_PATH.exists():
    print(f"❌ Không tìm thấy tệp .env tại: {ROOT_DIR / 'deployments' / '.env'} hoặc {ROOT_DIR / '.env'}")
    sys.exit(1)

# Nạp các biến môi trường
load_dotenv(dotenv_path=ENV_PATH)

# 3. Lấy 100% cấu hình từ biến môi trường (.env)
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
PARQUET_DIR = os.getenv("PARQUET_DIR")

# 4. Kiểm tra tính đầy đủ của biến môi trường
required_vars = {
    "POSTGRES_HOST": DB_HOST,
    "POSTGRES_PORT": DB_PORT,
    "POSTGRES_USER": DB_USER,
    "POSTGRES_PASSWORD": DB_PASSWORD,
    "POSTGRES_DB": DB_NAME,
    "PARQUET_DIR": PARQUET_DIR,
}

missing_vars = [k for k, v in required_vars.items() if not v]
if missing_vars:
    print(f"❌ [LỖI CẤU HÌNH] Thiếu các biến môi trường trong tệp .env: {', '.join(missing_vars)}")
    sys.exit(1)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def load_parquet_to_staging():
    print("🗄️ [AN - WAREHOUSE] Khởi chạy tiến trình nạp tệp tin Parquet vào PostgreSQL...")
    engine = create_engine(DB_URL)
    
    search_path = os.path.join(PARQUET_DIR, "**", "*.parquet")
    parquet_files = glob.glob(search_path, recursive=True)
    
    if not parquet_files:
        print(f"❌ Không tìm thấy tệp Parquet nào tại đường dẫn: {PARQUET_DIR}")
        return
        
    print(f"📖 Tìm thấy {len(parquet_files)} mảnh phân vùng Parquet. Đang đọc gộp...")
    
    df_list = [pd.read_parquet(f) for f in parquet_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    
    print(f"🚀 Thao tác INSERT {len(combined_df)} dòng dữ liệu vào bảng `staging_weather`...")
    combined_df.to_sql("staging_weather", engine, if_exists="replace", index=False)
    
    print("✅ [THÀNH CÔNG] Dữ liệu Parquet đã nằm gọn gàng tại bảng thô PostgreSQL!")

if __name__ == "__main__":
    load_parquet_to_staging()