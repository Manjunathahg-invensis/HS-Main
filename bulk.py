import concurrent.futures
import pandas as pd
import requests
import time
from tqdm import tqdm

time.sleep(15)
data = pd.read_csv("test.csv", encoding="latin1")
data['Q1'] = None
data['Q2'] = None
data['Q3'] = None
url = "http://localhost:8000/query/"

def get_recommendations(text):
    recommendations =["", "", ""]
    response = requests.post(url, json={"text":text})
    if response.status_code == 200:
        formatted_results = response.json()
        count = 0
        for result in formatted_results:
            recommendations[count] = f"**HS Code:** {result['HS Code']}  **Score:** {result['Score']}%"
            count += 1
            if count == 3:
                break
    return recommendations


def process_row(row):
    index, data = row  
    text = data['Description']  
    recommendations = get_recommendations(text)
    return recommendations


def apply_multiprocessing(df):
    num_threads = 4

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:

        results = list(tqdm(executor.map(process_row, df.iterrows()), total=len(df)))
    

    for i, col_name in enumerate(['Q1', 'Q2', 'Q3']):
        df[col_name] = [result[i] for result in results]
    
    return df

df_with_recommendations = apply_multiprocessing(data)
df_with_recommendations.to_csv("output.csv", index=False)
