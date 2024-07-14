import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import json

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

def get_all_product_detail(product_id):
    print(f'GETTING ALL PRODUCT DETAIL BY ID')
    product_detail = []
    for id in product_id:
        product_detail.append(get_product_detail(id).json())
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

def get_all_reviews_all_product(products_id):
    all_reviews = []
    for id in products_id:
        all_reviews.extend(get_all_product_reviews(id))
    return all_reviews

# Write to database postgres on site Neon.tech
# Define the write function
def write_to_pg(df, table_name):
    jdbc_url = "jdbc:postgresql://tiki_batch-db-1:5433/tiki_db"
    jdbc_properties = {
        "user": "username",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    
    if table_name == "products_prices":
        df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=jdbc_properties)
    else:
        existing_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
        df_new = df.join(existing_df, df.id == existing_df.id, "left_anti")
        df_new.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=jdbc_properties)

    
if __name__ == "__main__":
    # get all product ids
    product_id = get_all_product_id()

    #get all product detail from ids
    product_detali = get_all_product_detail(product_id)

    # starting spark session
    spark = SparkSession.builder\
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.2") \
    .appName("titki")\
    .getOrCreate()

    # Chuyển danh sách các dictionary thành một RDD của các JSON objects
    rdd = spark.sparkContext.parallelize([json.dumps(product) for product in product_detali])

    # Đọc RDD dưới dạng JSON
    df = spark.read.json(rdd)

    # Hiển thị schema và dữ liệu
    df.printSchema()

    df_2 = df.select('id', 'brand', 'categories', 'name', 'price', 'list_price', 'original_price', 'description', 'current_seller', 'quantity_sold', 'stock_item')
    
    # DataFrame chứa thông tin sản phẩm
    products_df = df_2.select(
                            col("id").alias('id'),
                            col("name"), 
                            col("price"), 
                            col("list_price"), 
                            col("original_price"), 
                            col("description"), 
                            col("brand.id").alias('brand_id'), 
                            col('categories.id').alias('category_id')
                        ).distinct()

    #daily prices
    products_prices_df = df_2.select(
                                col("id").alias('product_id'),
                                col("price"), 
                                col("list_price"), 
                                col("original_price")
                            ).withColumn("current_date", datetime.today())

    # DataFrame chứa thông tin thương hiệu
    brands_df = df_2.select(col('brand.id').alias('brand_id'), 
                            col('brand.name').alias('brand_name'),
                            col('brand.slug').alias('brand_slug')).distinct()

    # # DataFrame chứa thông tin danh mục
    categories_df = df_2.select(col('categories.id').alias('category_id'),
                                col('categories.is_leaf').alias('category_is_leaf'),
                                col('categories.name').alias('category_name')).distinct()

    # # DataFrame chứa thông tin người bán
    sellers_df = df_2.select(col('current_seller.id').alias('seller_id'),
                            col('current_seller.link').alias('seller_link'),
                            col('current_seller.logo').alias('seller_logo'),
                            col('current_seller.name').alias('seller_name'),
                            col('current_seller.store_id').alias('store_id')).distinct()

    sellers_products_df = df_2.select(col('current_seller.id').alias('seller_id'),
                                    col('current_seller.product_id').alias('product_id')).distinct()

    # # DataFrame chứa thông tin kho hàng
    stock_items_df = df_2.select(col('id').alias('product_id'),
                                col('stock_item.max_sale_qty').alias('stock_max_sale_qty'),
                                col('stock_item.min_sale_qty').alias('stock_min_sale_qty'),
                                col('stock_item.preorder_date').alias('stock_preorder_date'),
                                col('stock_item.qty').alias('stock_qty')).distinct()

    # get all reviews of all products
    all_reviews = get_all_reviews_all_product(product_id)

    rdd_review = spark.sparkContext.parallelize([json.dumps(rv) for rv in all_reviews])

    # Đọc RDD dưới dạng JSON
    df_rv = spark.read.json(rdd_review)

    # Hiển thị schema và dữ liệu
    df_rv.printSchema()

    comments_df = df_rv.selectExpr("id", "title", "created_at", "rating", "product_id", "status", "customer_id")

    customers_df = df_rv.select(col('created_by.id').alias('customer_id'), 
                         col('created_by.name').alias('name'),
                         col('created_by.full_name').alias('fullname'),
                         col('created_by.region').alias('region'),
                         col('created_by.created_time').alias('created_time'))
    
    # Write into pgsql
    write_to_pg(products_df, 'products')
    write_to_pg(products_prices_df, 'products_prices')
    write_to_pg(brands_df, 'brands')
    write_to_pg(categories_df, 'categories')
    write_to_pg(sellers_df, 'sellers')
    write_to_pg(sellers_products_df, 'sellers_products')
    write_to_pg(stock_items_df, 'stock_items')
    write_to_pg(comments_df, 'comments')
    write_to_pg(customers_df, 'customers')
