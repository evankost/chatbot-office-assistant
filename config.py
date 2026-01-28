import os
from dotenv import load_dotenv

load_dotenv()  # load variables from .env into environment

# endpoints
KOBOLDCPP_URL_MAIN   = os.getenv("KOBOLDCPP_URL_MAIN")
KOBOLDCPP_URL_SPARQL = os.getenv("KOBOLDCPP_URL_SPARQL")
KOBOLDCPP_URL_SQL    = os.getenv("KOBOLDCPP_URL_SQL")
SPARQL_ENDPOINT      = os.getenv("SPARQL_ENDPOINT")

# db credentials
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")

# app settings
DB_ENABLED        = True
REQUEST_TIMEOUT_S = 50
KEEPALIVE_S       = 2.0

# derived DSN
DB_DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
