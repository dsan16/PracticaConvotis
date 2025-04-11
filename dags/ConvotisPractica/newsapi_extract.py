import requests
import json
import pandas as pd
from datetime import datetime

def extract_news(date_extract):
    API_KEY = "f378cca5de114febbcd676d4e0f398cf"
    URL = "https://newsapi.org/v2/everything"

    params = {
        "q": "nintendo", 
        "from": date_extract.strftime("%Y-%m-%d"),
        "sortBy": "publishedAt",
        "apiKey": API_KEY
    }

    response = requests.get(URL, params=params)
    data = response.json()

    if data.get("status") != "ok":
        raise Exception("Error al consultar NewsAPI")
    
    articles = data.get("articles", [])

    df = pd.DataFrame(articles)
    df.to_csv(f"/tmp/news_data_{date_extract}.csv", index=False) 