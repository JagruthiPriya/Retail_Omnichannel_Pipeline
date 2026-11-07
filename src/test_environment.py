import sys
import requests
import pandas as pd
import pyspark
import sqlalchemy
import psycopg2
import dotenv

print("========== ENVIRONMENT CHECK ==========")

print("Python Version:", sys.version)
print("PySpark Version:", pyspark.__version__)
print("Pandas Version:", pd.__version__)
print("Requests Version:", requests.__version__)
print("SQLAlchemy Version:", sqlalchemy.__version__)

print("\nAll required libraries imported successfully!")
print("Environment setup completed.")
