from prefect import flow

from tasks.extract_shopify_apps import extract_shopify_apps
from tasks.load_shopify_apps import compare_sha_keys, load_stg_shopify_apps


@flow(name="Shopify apps")
def shopify_apps_flow():
    url = 'https://apps.shopify.com/search?q=returns+and+exchanges&page=1'
    data = extract_shopify_apps(url)
    new_data = compare_sha_keys(data)
    load_stg_shopify_apps(new_data)