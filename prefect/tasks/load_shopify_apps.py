from prefect_sqlalchemy import SqlAlchemyConnector
from prefect import task, get_run_logger


@task(retries=2, retry_delay_seconds=30)
def compare_sha_keys(source_data: list[dict]) -> list[dict]:
    """Takes the sha256_surrogate_key field from source data and uses it to query
    the sink; returns records that have no matches with the sink data.
    
    Args:
        source_data (list[dict]): List of dictionaries from the Shopify app scraper
    
    Returns:
        records_to_write (list[dict]): Filtered list of dictionaries from Shopify app
            scraper that are not found in the sink.
    """
    logger = get_run_logger()
    src_sha_keys = [ i['sha256_surrogate_key'] for i in source_data ]
    with SqlAlchemyConnector.load("neon-postgres") as database_block:
        wh_sha_keys = database_block.fetch_all("""
            SELECT sha256_surrogate_key FROM stg_shopify_apps
            WHERE sha256_surrogate_key IN :hash_keys
            """, { "hash_keys": tuple(src_sha_keys) })
        wh_sha_keys = [ key for single_tuple in wh_sha_keys for key in single_tuple ]

        records_to_write = []
        for record in source_data:
            if record['sha256_surrogate_key'] in wh_sha_keys:
                logger.info(f"No change in app listing for {record['app_name']}")
                continue
            else:
                records_to_write.append(record)
        
        print(records_to_write)
        return(records_to_write)


@task(retries=2, retry_delay_seconds=30)
def load_stg_shopify_apps(data: list[dict]):
    """Loads data into stg_shopify_apps table.
    
    Args:
        data (list[dict]): List of filtered records from the Shopify app scraper.
    """
    with SqlAlchemyConnector.load("neon-postgres") as database_block:
        logger = get_run_logger()
        database_block.execute_many("""
            INSERT INTO stg_shopify_apps (
                app_name,
                app_url,
                ranking,
                average_rating,
                total_reviews,
                is_ad,
                sha256_surrogate_key
            ) VALUES (
                :app_name,
                :app_url,
                :ranking,
                :average_rating,
                :total_reviews,
                :is_ad,
                :sha256_surrogate_key
            );""", seq_of_parameters=data)
        logger.info(f"Successfully wrote {len(data)} records into table stg_shopify_apps")
        return
