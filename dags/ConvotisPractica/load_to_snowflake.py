import pandas as pd
import os
import json
from sqlalchemy import text
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def load_to_snowflake():
    output_path = os.path.join(os.path.dirname(__file__), "data")
    file_path = os.path.join(output_path, "news_data.json")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        for article in data:
            json_str = json.dumps(article, ensure_ascii=False)

            stmt = text("INSERT INTO stg_news (raw_file) SELECT PARSE_JSON(:valor)")

            conn.execute(stmt, {"valor": json_str})
