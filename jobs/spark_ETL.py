import psycopg2
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, lit, date_format

import datetime
import json
import time
import random
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi-VN,vi;q=0.8,en-US;q=0.5,en;q=0.3',
    'Referer': 'https://tiki.vn/?src=header_tiki',
    'x-guest-token': '8jWSuIDBb2NGVzr6hsUZXpkP1FRin7lY',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}


def get_data(page):
    res = requests.get(f"https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&category=1846&page={page}", headers=headers)
    return res

def get_product_detail(id):
    res = requests.get(f"https://tiki.vn/api/v2/products/{id}", headers=headers)
    return res

def get_reviews(page, id):
    res = requests.get(f'https://tiki.vn/api/v2/reviews?limit=5&include=comments&page={page}&product_id={id}', headers=headers, timeout=10)
    return res

def get_all_product_detail(product_id, batch_size=10, delay=5):
    print(f'GETTING ALL PRODUCT DETAIL BY ID')
    product_detail = []
    
    for i in range(0, len(product_id), batch_size):
        batch_ids = product_id[i:i+batch_size]
        for id in batch_ids:
            try:
                product_detail.append(get_product_detail(id).json())
                print(f'Getting product with ID: {id}')
            except Exception as e:
                print(f"Error getting product with ID {id}: {e}")
        
        # Delay between batches
        print(f"Waiting for {delay} seconds before processing the next batch...")
        time.sleep(delay)
    
    print(f'GETTING ALL PRODUCT DETAIL SUCCESSFUL!!!')
    return product_detail

def get_all_product_id():
    product_id = []
    first_page_data = get_data(1).json()
    last_page = first_page_data['paging']['last_page']

    print(f'GETTING ALL PRODUCTS ID')
    print(f'Quantity page: {last_page}')

    for page in range(1, last_page + 1):
            res = get_data(page)
            if res.status_code == 200:
                res = res.json()
                print(f'request page {page} success!')
            for x in range(0, 40):
                product_id.append(res['data'][x]['id'])
    print(f'GETTING ALL PRODUCT IDs SUCCESSFUL!!!')
    return product_id

def get_all_product_reviews(product_id):
    print(f'GETTING ALL PRODUCT REVIEWS BY PRODUCT_ID')
    first_page_data = get_reviews(1, product_id).json()
    last_page = first_page_data['paging']['last_page']
    print(f'Quantity page: {last_page}')

    product_reviews = []
    for page in range(1, last_page + 1):
        product_reviews.extend(get_reviews(page, product_id).json()['data'])
        print(f'request page {page} success!')

    print(f'GETTING ALL PRODUCT REVIEWS SUCCESSFUL!!!')
    return product_reviews

def get_all_reviews_all_product(products_id, batch_size=10, delay=5):
    all_reviews = []
    
    for i in range(0, len(products_id), batch_size):
        batch_ids = products_id[i:i+batch_size]
        for id in batch_ids:
            try:
                all_reviews.extend(get_all_product_reviews(id))
                print(f'Getting reviews for product ID: {id}')
            except Exception as e:
                print(f"Error getting reviews for product ID {id}: {e}")
        
        # Delay between batches
        print(f"Waiting for {delay} seconds before processing the next batch...")
        time.sleep(delay)
    
    print(f'GETTING ALL REVIEWS FOR ALL PRODUCTS SUCCESSFUL!!!')
    return all_reviews

def create_all_table_if_not_exists():
    create_all_tables_sql = """
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id BIGINT PRIMARY KEY,
            name VARCHAR,
            price FLOAT,
            list_price FLOAT,
            original_price FLOAT,
            description VARCHAR,
            category_id BIGINT,
            brand_name VARCHAR,
            category_name VARCHAR
        );

        CREATE TABLE IF NOT EXISTS dim_sellers (
            seller_id BIGINT PRIMARY KEY,
            seller_link VARCHAR,
            seller_logo VARCHAR,
            seller_name VARCHAR,
            store_id BIGINT
        );

        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id BIGINT PRIMARY KEY,
            name VARCHAR,
            fullname VARCHAR,
            region VARCHAR,
            created_time TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dim_date (
            date_id BIGINT PRIMARY KEY,    
            date DATE,                    
            year INT,
            month INT,
            day INT,
            weekday VARCHAR(10),          
            day_of_week INT,         
            week_of_year INT,
            quarter INT,
            is_weekend BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS fact_daily_products_prices (
            product_id BIGINT,
            price FLOAT,
            list_price FLOAT,
            original_price FLOAT,
            stock_qty BIGINT,
            quantity_sold BIGINT,
            seller_id BIGINT,
            date_id BIGINT
        );

        CREATE TABLE IF NOT EXISTS fact_reviews (
            review_id BIGINT PRIMARY KEY,
            customer_id BIGINT,
            product_id BIGINT,
            seller_id BIGINT,
            rating BIGINT,
            thank_count BIGINT,
            date_id BIGINT
        );
        """
    
    for query in create_all_tables_sql.split(";"):
        if query.strip():
            spark.sql(query)


def upsert_dim_to_pg(df, table_name, schema='public'):
    jdbc_url = "jdbc:postgresql://db:5432/tiki_db"
    jdbc_properties = {
        "user": "username",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    primary_keys = {
        "dim_products": "product_id",
        "dim_sellers": "seller_id",
        "dim_customers": "customer_id",
        "fact_reviews": "review_id"
    }

    full_table_name = f"{schema}.{table_name}"
    primary_key_col = primary_keys.get(table_name, "id")

    # Write DataFrame to a staging table
    staging_table_name = f"{schema}.{table_name}_staging"
    df.write.jdbc(url=jdbc_url, table=staging_table_name, mode="overwrite", properties=jdbc_properties)

    # Perform upsert using raw SQL
    with psycopg2.connect(jdbc_url, user="username", password="password") as conn:
        with conn.cursor() as cur:
            upsert_sql = f"""
            INSERT INTO {full_table_name} ({', '.join(df.columns)})
            SELECT {', '.join(df.columns)}
            FROM {staging_table_name} AS staging
            ON CONFLICT ({primary_key_col})
            DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != primary_key_col])};
            """
            cur.execute(upsert_sql)
            conn.commit()

    # Drop the staging table
    with psycopg2.connect(jdbc_url, user="username", password="password") as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {staging_table_name}")
            conn.commit()


# Write to database postgres on site Neon.tech
# Define the write function
def write_fact_to_pg(df, table_name, schema='public'):
    jdbc_url = "jdbc:postgresql://db:5432/tiki_db"
    jdbc_properties = {
        "user": "username",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    # Dictionary to map table names to their primary key columns

    # Handle single primary key
    if table_name == "fact_price_daily":
        df.write.jdbc(url=jdbc_url, table='fact_price_daily', mode="append", properties=jdbc_properties)
    else:
        upsert_dim_to_pg(df, "fact_reviews")


def add_dim_date():
    jdbc_url = "jdbc:postgresql://db:5432/tiki_db"
    jdbc_properties = {
        "user": "username",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    current_date = datetime.date.today()

    dim_date_df = spark.read.jdbc(url=jdbc_url, table='dim_date', properties=jdbc_properties)

    if dim_date_df.filter(col("date") == lit(current_date)).count() == 0:
        date_id = int(current_date.strftime("%Y%m%d"))
        new_row = spark.createDataFrame(
            [(date_id,
            current_date,
            current_date.year,
            current_date.month,
            current_date.day,
            current_date.strftime("%A"),
            current_date.isoweekday(),
            current_date.isocalendar()[1],
            (current_date.month - 1) // 3 + 1,
            1 if current_date.isoweekday() in [6, 7] else 0)],
            ["date_id", "date", "year", "month", "day", "weekday", "day_of_week", "week_of_year", "quarter", "is_weekend"]
        )

        new_row.write.jdbc(url=jdbc_url, table='dim_date', mode="append", properties=jdbc_properties)
    return date_id

def add_dim_date_from_timestamp(created_at_timestamp):
    jdbc_url = "jdbc:postgresql://db:5432/tiki_db"
    jdbc_properties = {
        "user": "username",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    created_at_datetime = datetime.utcfromtimestamp(from_unixtime(created_at_timestamp))
    dim_date_df = spark.read.jdbc(url=jdbc_url, table='dim_date', properties=jdbc_properties)

    year = created_at_datetime.year
    month = created_at_datetime.month
    day = created_at_datetime.day
    weekday = created_at_datetime.strftime('%A')  # Ví dụ: 'Monday'
    day_of_week = created_at_datetime.weekday() + 1  # Monday = 1, Sunday = 7
    week_of_year = created_at_datetime.isocalendar()[1]
    quarter = (month - 1) // 3 + 1  # Tính toán quý
    is_weekend = 1 if created_at_datetime.isoweekday() in [6, 7] else 0
    date_id = int(f"{year:04d}{month:02d}{day:02d}")

    if dim_date_df.filter(dim_date_df.date_id == date_id).count() == 0:
        new_row = [(date_id, created_at_datetime.date(), year, month, day, weekday, day_of_week, week_of_year, quarter, is_weekend)]
        new_date_df = spark.createDataFrame(new_row, schema=["date_id", "date", "year", "month", "day", "weekday", "day_of_week", "week_of_year", "quarter", "is_weekend"])
        new_date_df.write.jdbc(url=jdbc_url, table='dim_date', mode="append", properties=jdbc_properties)
    
    return date_id

    
if __name__ == "__main__":
     # starting spark session
    spark = SparkSession.builder\
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.2") \
    .appName("tiki")\
    .getOrCreate()

    # get all product ids
    product_id = get_all_product_id()
    product_id_20_samples = random.sample(product_id, 10)
    #get all product detail from ids (20 samples )
    product_detali = get_all_product_detail(product_id_20_samples)
    # all ids
    # product_detali = get_all_product_detail(product_id)

    # Chuyển danh sách các dictionary thành một RDD của các JSON objects
    rdd = spark.sparkContext.parallelize([json.dumps(product) for product in product_detali])

    # Đọc RDD dưới dạng JSON
    df = spark.read.json(rdd)

    # Hiển thị schema và dữ liệu
    df.printSchema()

    df_2 = df.select('id', 'brand', 'categories', 'name', 'price', 'list_price', 'original_price', 'description', 'current_seller', 'quantity_sold', 'stock_item')
    
    # DataFrame chứa thông tin sản phẩm
    dim_products_df = df_2.select(
                            col("id").alias('product_id'),
                            col("name"), 
                            col("price"), 
                            col("list_price"), 
                            col("original_price"), 
                            col("description"), 
                            col('categories.id').alias('category_id'),
                            col('brand.name').alias('brand_name'),
                            col('categories.name').alias('category_name')
                        ).distinct()

    # Daily prices
    fact_daily_products_prices_df = df_2.select(
        col("id").alias('product_id'),
        col("price"), 
        col("list_price"), 
        col("original_price"),
        col('stock_item.qty').alias('stock_qty'),
        col("quantity_sold"),
        col('current_seller.id').alias('seller_id')
    ).withColumn("date_id", add_dim_date())

    # # DataFrame chứa thông tin người bán
    dim_sellers_df = df_2.select(col('current_seller.id').alias('seller_id'),
                            col('current_seller.link').alias('seller_link'),
                            col('current_seller.logo').alias('seller_logo'),
                            col('current_seller.name').alias('seller_name'),
                            col('current_seller.store_id').alias('store_id')).distinct()


    # get all reviews of all products
    # all_reviews = get_all_reviews_all_product(product_id)

    # get all reviews of 20 sample products
    all_reviews = get_all_reviews_all_product(product_id_20_samples)

    rdd_review = spark.sparkContext.parallelize([json.dumps(rv) for rv in all_reviews])

    # Đọc RDD dưới dạng JSON
    df_rv = spark.read.json(rdd_review)

    # Hiển thị schema và dữ liệu
    df_rv.printSchema()

    dim_customers_df = df_rv.select(
        col('created_by.id').alias('customer_id'), 
        col('created_by.name').alias('name'),
        col('created_by.full_name').alias('fullname'),
        col('created_by.region').alias('region'),
        from_unixtime(col('created_by.created_time')).cast('timestamp').alias('created_time')
    ).distinct()

    fact_review_df = df_rv.select(
        col('id').alias('review_id'),
        col('customer_id'),
        col('product_id'),
        col('seller.id').alias('seller_id'),
        col('rating'),
        col('thank_count'),
    ).withColumn("date_id", add_dim_date_from_timestamp(col('created_at')))
    
    create_all_table_if_not_exists()

    # Write into pgsql
    upsert_dim_to_pg(dim_products_df, 'dim_products')
    upsert_dim_to_pg(dim_sellers_df, 'dim_sellers')
    upsert_dim_to_pg(dim_customers_df, 'dim_customers')
    
    write_fact_to_pg(fact_daily_products_prices_df, 'fact_price_daily')
    write_fact_to_pg(fact_review_df, 'fact_reviews')
