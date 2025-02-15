import clickhouse_connect
import pandas as pd
from tqdm import tqdm
import time
import json
import os
from dotenv import load_dotenv

load_dotenv()

## Config
HOST = os.getenv("Host")
PORT = os.getenv("PORT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

clickhouse_config = {
    'Host': 'HOST',
    'Port': 'PORT',
    'User': 'USER',
    'Password': 'PASSWORD',
}


def connect_to_clickhouse(host='serever', port=8124, username='user', password='passwords'):
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password
    )
    return client




## Query function
def get_main_categories(lst_shop_base_id):
    """
    Truy vấn tối ưu lấy cả 2 trường categories__id_2 và categories__id_2.
    - Với categories__id_2: nếu cate thứ 2 có doanh thu ≥ 50% của cate đầu thì lấy cả cate đầu và cate thứ 2.
    - Với categories__id_2: nếu cate thứ 2 có doanh thu ≥ 30% của cate đầu thì lấy cả cate đầu và cate thứ 2.
    """
    query = f"""
            WITH
        revenue_data AS (
            SELECT
                shop_base_id,
                ifNull(categories__id_2, '') AS categories__id_2,
                SUM(
                    arraySum(x -> x.3, arrayFilter(x -> (x.1 > '2024-01-01'), order_revenue_arr))
                ) AS total_revenue_sum
            FROM
                analytics.products
            WHERE
                shop_base_id IN {lst_shop_base_id}
            GROUP BY
                shop_base_id, categories__id_2
            ORDER BY
                total_revenue_sum DESC
        )
        SELECT
            shop_base_id,
            topK(2)(categories__id_2) AS top_categories
        FROM revenue_data
        GROUP BY shop_base_id       
    """
    return query




def get_revenue_categories_by_shop(lst_shop_base_id):
    """
    Truy vấn lấy doanh thu cao nhất của 2 ngành hàng cấp 2 đối với từng shop
    """
    query = f"""
WITH revenue_categories AS (
    SELECT
        shop_base_id,
        IFNULL(categories__id_3, '') AS categories__id_3,
        SUM(
            arraySum(x -> x.3, arrayFilter(x -> x.1 > '2024-01-01', order_revenue_arr))
        ) AS total_revenue_sum
    FROM analytics.products
    WHERE shop_base_id IN {lst_shop_base_id}
    GROUP BY shop_base_id, categories__id_3
),
sorted_revenue AS (
    SELECT
        shop_base_id,
        arrayZip(
            groupArray(categories__id_3),
            groupArray(total_revenue_sum)
        ) AS zipped_data,
        arraySort(x -> -x.2, arrayZip(groupArray(categories__id_3), groupArray(total_revenue_sum))) AS sorted_data
    FROM revenue_categories
    GROUP BY shop_base_id
)
SELECT
    shop_base_id,
    arrayMap(x -> x.1, arraySlice(sorted_data, 1, 2)) AS lst_categories_2,
    arrayMap(x -> x.2, arraySlice(sorted_data, 1, 2)) AS lst_revenue
FROM sorted_revenue
ORDER BY shop_base_id  
    """
    return query



def get_revenue_categories_by_shop(lst_shop_base_id):
    """
    Truy vấn lấy doanh thu cao nhất của 2 ngành hàng cấp 2 đối với từng shop
    """
    query = f"""
WITH revenue_categories AS (
    SELECT
        shop_base_id,
        IFNULL(categories__id_3, '') AS categories__id_3,
        SUM(
            arraySum(x -> x.3, arrayFilter(x -> x.1 > '2024-01-01', order_revenue_arr))
        ) AS total_revenue_sum
    FROM analytics.products
    WHERE shop_base_id IN {lst_shop_base_id}
    GROUP BY shop_base_id, categories__id_3
),
sorted_revenue AS (
    SELECT
        shop_base_id,
        arrayZip(
            groupArray(categories__id_3),
            groupArray(total_revenue_sum)
        ) AS zipped_data,
        arraySort(x -> -x.2, arrayZip(groupArray(categories__id_3), groupArray(total_revenue_sum))) AS sorted_data
    FROM revenue_categories
    GROUP BY shop_base_id
)
SELECT
    shop_base_id,
    arrayMap(x -> x.1, arraySlice(sorted_data, 1, 2)) AS lst_categories_2,
    arrayMap(x -> x.2, arraySlice(sorted_data, 1, 2)) AS lst_revenue
FROM sorted_revenue
ORDER BY shop_base_id  
    """
    return query



def get_max_revenue_and_product_by_shop(lst_shop_base_id):
    """
    Truy vấn lấy doanh thu cao nhất và sản phẩm có doanh thu cao nhất theo từng shop. Kết quả trả về 3 trường
    """
    query = f"""
    SELECT
        shop_base_id,
        topKWeighted(1)(
            product_name,
            arrayReduce('sum', arrayMap(x -> if(x.1 > '2024-01-01', x.3, 0), order_revenue_arr))
        ) AS top_categories,
        SUM(
            arraySum(x -> x.3, arrayFilter(x -> (x.1 > '2024-01-01'), order_revenue_arr))
        ) AS total_revenue_sum
    FROM analytics.products
    WHERE shop_base_id IN {lst_shop_base_id}
    GROUP BY shop_base_id;
    """
    return query
    









## Transform Pandas
def return_main_categories(lst_website):
    """
    Hàm này nhận 1 danh sách website, chuyển đổi thành danh sách shop_base_id và thực hiện query lấy kết quả.
    Trả về tuple gồm 2 list: (shop_base_id, categories_3)
    """
    lst_shop_base_id  = [shop_base_id.split("/")[-1] for shop_base_id in lst_website]
    lst_shop_base_id = [f"1__{shop_base_id}" for shop_base_id in lst_shop_base_id]
    
    result = client.query(get_main_categories(lst_shop_base_id))
    
    if result.result_set:
        shop_base_ids = []
        categories_3 = []

        for row in result.result_set:
            shop_id, top_categories = row
            shop_base_ids.append(str(shop_id))
            categories_3.append(str(top_categories))

        return shop_base_ids, categories_3
    else:
        return [], []



def return_max_revenue_and_product_by_shop(lst_website):
    """
    Hàm này nhận 1 danh sách website, chuyển đổi thành danh sách shop_base_id và thực hiện query lấy kết quả.
    Trả về tuple gồm 2 list: (shop_base_id, categories_3)
    """
    lst_shop_base_id  = [shop_base_id.split("/")[-1] for shop_base_id in lst_website]
    lst_shop_base_id = [f"1__{shop_base_id}" for shop_base_id in lst_shop_base_id]
    
    # Thực hiện query trên ClickHouse
    result = client.query(get_revenue_categories_by_shop(lst_shop_base_id))
    
    # Xử lý kết quả trả về
    if result.result_set:
        shop_base_ids = []
        lst_categories_2 = []
        lst_revenue = []

        for row in result.result_set:
            shop_id, top_categories,reveue = row
            shop_base_ids.append(str(shop_id))
            lst_categories_2.append(str(top_categories))
            lst_revenue.append(str(reveue))

        return shop_base_ids, lst_categories_2,lst_revenue
    else:
        return [], []
    
    
def return_revenue_categories_lv2_by_shop(lst_website):
    """
    Hàm này nhận 1 danh sách website, chuyển đổi thành danh sách shop_base_id và thực hiện query lấy kết quả.
    Trả về tuple gồm 2 list: (shop_base_id, categories_3)
    """
    lst_shop_base_id  = [shop_base_id.split("/")[-1] for shop_base_id in lst_website]
    lst_shop_base_id = [f"1__{shop_base_id}" for shop_base_id in lst_shop_base_id]
    
    # Thực hiện query trên ClickHouse
    result = client.query(get_revenue_categories_by_shop(lst_shop_base_id))
    
    # Xử lý kết quả trả về
    if result.result_set:
        shop_base_ids = []
        lst_products = []
        lst_revenue = []

        for row in result.result_set:
            shop_id, top_product,reveue = row
            shop_base_ids.append(str(shop_id))
            lst_products.append(str(top_product))
            lst_revenue.append(str(reveue))

        return shop_base_ids, lst_products,lst_revenue
    else:
        return [], []



if __name__ == '__main__':
    
    ## Acess db
    client = connect_to_clickhouse(
        host=clickhouse_config['Host'],
        port=clickhouse_config['Port'],
        username=clickhouse_config['User'],
        password=clickhouse_config['Password']
    )


    ## Handle file excel
    df = pd.read_excel(r"C:\Users\ADMIN\Downloads\shopee_30k.xlsx")
    df = df.head(2610)
    print(df.shape)

    output_file = r"C:\Users\ADMIN\Downloads\json_15k_head_shopee_first_v2.jsonl"
    batch_size = 50  # batch size
    df['website'] = df['website'].astype(str)
    websites = df['website'].tolist()

    all_data = []
    
    with open(output_file, 'a', encoding='utf-8') as f:
        for i in tqdm(range(0, len(websites), batch_size), total=len(websites)//batch_size + 1, desc="Processing batches"):
            batch_websites = websites[i:i + batch_size]
            
            shop_base_ids, categories_3_list = return_main_categories(batch_websites)
            shop_ids_revenue, categories_2_list, revenue_list = return_max_revenue_and_product_by_shop(batch_websites)
            shop_ids_lv2, top_products, revenue_lv2 = return_revenue_categories_lv2_by_shop(batch_websites)
            
            for shop_base_id, categories_3 in zip(shop_base_ids, categories_3_list):
                row_data = {
                    'shop_base_id': shop_base_id,
                    'categories_cat_2': categories_3,
                    'categories_lv2': '',
                    'revenue_lv2': '',
                    'top_product': '',
                    'max_revenue': ''
                }
                
                if shop_base_id in shop_ids_revenue:
                    idx = shop_ids_revenue.index(shop_base_id)
                    row_data['categories_lv2'] = categories_2_list[idx]
                    row_data['max_revenue'] = revenue_list[idx]
                
                if shop_base_id in shop_ids_lv2:
                    idx = shop_ids_lv2.index(shop_base_id)
                    row_data['top_product'] = top_products[idx]
                    row_data['revenue_lv2'] = revenue_lv2[idx]
                
                all_data.append(row_data)
                f.write(json.dumps(row_data) + '\n')
    
    df_output = pd.DataFrame(all_data)
    df_output.to_excel(r"C:\Users\ADMIN\Downloads\updated_shopee_data.xlsx", index=False)
    print("Done")
