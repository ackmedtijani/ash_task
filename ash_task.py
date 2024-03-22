import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



# Function to fetch data from API endpoint
def fetch_data():
    url = 'https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31'  # Replace with your API endpoint
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None


def get_into_data(data):
    if not "hourly" in data:
        raise Exception("Hourly data not found in the result")
    
    hourly_data = data.get("hourly")
    
    if hourly_data and isinstance(hourly_data , dict):
        
        
        df = pd.DataFrame(hourly_data)
        
        if 'time' not in df.columns:
            raise Exception("time column not found in the results")
        
        df["time"] = pd.to_datetime(df["time"])
        
        df = df.groupby(pd.Grouper(key="time", freq= 'D'))
        return df
        
    
    raise Exception("Hourly data not found in the result")
    

def main():
    try:
        data = fetch_data()
        
        if data:
            daily_readings = get_into_data(data)
            
            #Daily aggregate
            df = daily_readings.sum()
            df.index = df.index.strftime('%Y-%m-%d %H:%M:%S')

            table = pa.Table.from_pandas(df)
            pq.write_table(table, 'daily_aggregated_data.parquet')
            print("Data exported to Parquet file successfully.")
            
        else:
            print("Failed to fetch data from API.")
        
    except Exception as e:
        print(str(e))

if __name__ == "__main__":
    main()