import csv
import gzip
import random
import time
from datetime import datetime, timedelta

TOTAL_ROWS = 10_000_000
BATCH_SIZE = 500_000
OUTPUT_FILE = "logs_ch_data.csv.gz"


URLS = [
    "/index.html", "/login", "/profile", "/cart", "/checkout",
    "/api/v1/users", "/api/v1/products", "/api/v1/orders",
    "/static/css/main.css", "/static/js/app.js", "/search"
]
STATUS_CODES = [200, 200, 200, 201, 302, 400, 401, 404, 500, 503]
WEIGHTS = [0.70, 0.10, 0.05, 0.03, 0.02, 0.03, 0.02, 0.03, 0.01, 0.01]

def generate_batch(start_time, batch_size):
    batch = []
    for _ in range(batch_size):
        
        random_seconds = random.randint(0, 30 * 24 * 60 * 60)
        row_time = start_time - timedelta(seconds=random_seconds)
        
        row = [
            row_time.strftime("%Y-%m-%d %H:%M:%S"), 
            random.randint(1000, 9999999),          
            random.choice(URLS),                     
            random.randint(5, 5000),                 
            random.choices(STATUS_CODES, WEIGHTS)[0] 
        ]
        batch.append(row)
    return batch

def main():
    print(f"Запуск генерации {TOTAL_ROWS:,} строк...")
    start_wall_time = time.time()
    base_time = datetime.now()

    
    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t") 
        
        generated = 0
        while generated < TOTAL_ROWS:
            current_batch_size = min(BATCH_SIZE, TOTAL_ROWS - generated)
            batch_data = generate_batch(base_time, current_batch_size)
            writer.writerows(batch_data)
            
            generated += current_batch_size
            print(f"Прогресс: {generated:,} / {TOTAL_ROWS:,} строк записано.")

    end_wall_time = time.time()
    print(f"Успешно завершено за {end_wall_time - start_wall_time:.2f} сек.")
    print(f"Файл сохранен: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
