import requests
import os
import json
import pandas as pd
from datetime import datetime
import pendulum

def extract_news(date_extract):
    API_KEY = "f378cca5de114febbcd676d4e0f398cf"
    URL = "https://newsapi.org/v2/everything"

    params = {
        "q": "Murcia", 
        "from": pendulum.parse(date_extract).to_date_string(),
        "sortBy": "publishedAt",
        "apiKey": API_KEY
    }

    response = requests.get(URL, params=params)
    data = response.json()

    if data.get("status") != "ok":
        raise Exception("Error al consultar NewsAPI")
    
    articles = data.get("articles", [])


    output_path = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, "news_data.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)