import requests
import json
import pandas as pd
from datetime import datetime

def extract_news():
    API_KEY = "f378cca5de114febbcd676d4e0f398cf"
    URL = "https://newsapi.org/v2/everything"

    params = {
        "q": "nintendo", 
        "from": datetime.today().strftime("%Y-%m-%d"),
        "sortBy": "publishedAt",
        "apiKey": API_KEY
    }

    response = requests.get(URL, params=params)
    data = response.json()

    if data.get("status") != "ok":
        raise Exception("Error al consultar NewsAPI")
    
    articles = data.get("articles", [])
    rows = []
    for article in articles:
        rows.append({
            "sourceName": article.get("source", {}).get("name"),
            "sourceId": article.get("source", {}).get("id"),
            "author": article.get("author"),
            "title": article.get("title"),
            "description": article.get("description"),
            "url": article.get("url"),
            "urlToImage": article.get("urlToImage"),
            "publishedAt": article.get("publishedAt"),
            "content": article.get("content")
        })

        print({
            "sourceName": article.get("source", {}).get("name"),
            "sourceId": article.get("source", {}).get("id"),
            "author": article.get("author"),
            "title": article.get("title"),
            "description": article.get("description"),
            "url": article.get("url"),
            "urlToImage": article.get("urlToImage"),
            "publishedAt": article.get("publishedAt"),
            "content": article.get("content")
        })


    df = pd.DataFrame(rows)
    #df.to_csv("/tmp/news_data.csv", index=False)

extract_news()