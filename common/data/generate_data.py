import random
import csv
from datetime import datetime, timedelta

vehicles = [
    ("V1", "Sedan", "A", 2022),
    ("V2", "SUV", "B", 2023),
    ("V3", "Truck", "A", 2021),
    ("V4", "Sedan", "C", 2020),
    ("V5", "SUV", "B", 2022),
]

# Write vehicles.csv
with open("vehicles.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["vehicle_id", "model", "plant", "year"])
    writer.writerows(vehicles)

# Generate telematics data (30 days, every 5 mins)
start_time = datetime(2024, 3, 1)
end_time = start_time + timedelta(days=30)

current = start_time

with open("telematics.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "timestamp", "vehicle_id", "speed",
        "engine_temp", "fuel_level", "lat", "lon"
    ])

    while current < end_time:
        for v in vehicles:
            vehicle_id = v[0]

            speed = random.randint(0, 120)
            engine_temp = random.randint(70, 120)
            fuel_level = random.randint(10, 100)
            lat = round(12.90 + random.random(), 5)
            lon = round(77.50 + random.random(), 5)

            writer.writerow([
                current.strftime("%Y-%m-%d %H:%M:%S"),
                vehicle_id,
                speed,
                engine_temp,
                fuel_level,
                lat,
                lon
            ])

        current += timedelta(minutes=5)

print("Data generated successfully")