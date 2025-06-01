import pandas as pd
import json

daily_data_csv = 'data/daily_data.csv'
output_csv = "data/monthly_data.csv"

with open('data/weather_codes.json', 'r') as file:
    weather_codes = json.load(file)

locations = pd.read_csv(daily_data_csv, nrows=12)
locations = locations.loc[:, ["location_id", "city_name"]]
daily_data = pd.read_csv(daily_data_csv, header=13, parse_dates=["date"])


def generate_monthly_summary(daily_data=daily_data,locations=locations, weather_codes=weather_codes ,output_csv=output_csv):
    

    # Add year and month columns
    daily_data["year"] = daily_data["date"].dt.year
    daily_data["month"] = daily_data["date"].dt.month
    
    
    # Merge with locations
    daily_data = daily_data.merge(locations, on="location_id", how="left")
    
    # Group by location, year, and month
    monthly_chunks = daily_data.groupby(["location_id", "city_name", "year", "month"])
    # print(type(monthly_chunks))

    # Step 1: Define custom function to get mode and count
    def mode_and_count(group):
        # Calculate mode and count
        mode = group["weather_code"].mode().iloc[0]  # Most frequent weather_code
        count = (group["weather_code"] == mode).sum()
        
        # Return a flat DataFrame with the required columns
        return pd.DataFrame({
            # "dominant_weather_code": [mode],
            "dominant_weather_desc": [weather_codes.get(str(mode), "Unknown")],
            "dominant_weather_days": [count]
        })
    
    # Step 2: Aggregate climate stats
    monthly_agg = monthly_chunks.agg({
        "temperature_2m_mean": "mean",
        "temperature_2m_max": "max",
        "temperature_2m_min": "min",
        "wind_speed_10m_max": "max",
        "daylight_duration": "mean",
        "sunshine_duration": "sum",
        "precipitation_sum": "sum",
        "precipitation_hours": "sum"
    }).reset_index()

    monthly_agg = monthly_agg.rename(columns={
        "temperature_2m_mean": "avg_temperature",
        "temperature_2m_max": "max_temperature",
        "temperature_2m_min": "min_temperature",
        "wind_speed_10m_max": "max_wind_speed",
        "daylight_duration": "avg_daylight_duration",
        "sunshine_duration": "total_sunshine_duration",
        "precipitation_sum": "total_precipitation",
        "precipitation_hours": "total_precipitation_hours"
    })
    # print(type(climate_agg))
    # print(climate_agg.head())
    
    # Step 3: Aggregate weather code mode
    desc_agg = monthly_chunks.apply(mode_and_count, include_groups=False).reset_index(drop=True)
    
    # Step 4: Combine both
    monthly_data = pd.concat([monthly_agg, desc_agg], axis=1).reset_index(drop=True)
    
    def generate_narration(row):
        return (
            f"In {row['city_name']} during {row['month']:02d}/{row['year']}, "
            f"the average temperature was {row['avg_temperature']:.2f}°C, "
            f"with highs up to {row['max_temperature']:.2f}°C and lows down to {row['min_temperature']:.2f}°C. "
            f"There were {row['total_precipitation_hours']:.2f} hours of precipitation and total precipitation of {row['total_precipitation']:.2f} mm. "
            f"There were {row['total_sunshine_duration'] / 3600:.2f} hours of sunshine, with an average of {row['avg_daylight_duration'] / 3600:.2f} hours of daylight. "
            f"The most frequent weather was {row['dominant_weather_desc']} occurring on {row['dominant_weather_days']} days."
        )
    
    monthly_summary = pd.DataFrame()
    monthly_summary['description'] = monthly_data.apply(generate_narration, axis=1)
    monthly_summary['city_name'] = monthly_data['city_name']
    monthly_summary['date'] = pd.to_datetime(monthly_data[['year', 'month']].assign(day=1)).dt.to_period('M').astype(str)
    
    #concat to csv monthly_data.csv
    monthly_summary.to_csv(output_csv, mode='a', index=False, header=False)


    # monthly_summary.to_csv("monthly_summary.csv", index=False)

generate_monthly_summary()
print("Monthly summary generated and saved to", output_csv)