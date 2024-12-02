import re
import random
import hashlib
from playwright.sync_api import sync_playwright, expect
from prefect import task, flow, get_run_logger
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_github import GitHubCredentials
from prefect.runner.storage import GitRepository

@task(retries=2, retry_delay_seconds=60)
def test_shopify_app_page(url: str) -> bool:
    """Visits a Shopify app page and asserts a valid HTML structure.
    Args:
        url (str): URL to visit. Should start with 'https://apps.shopify.com/search?q='

    Returns:
        test_result (bool)
    """
    logger = get_run_logger()
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url)

            expect(page.locator('div.search-results-component')).to_be_visible()
            logger.info("Search results loaded.")
            
            cards = page.locator('div[data-controller="app-card"]').all()
            assert len(cards) > 1
            logger.info("More than 1 result found.")

            cards_to_sample = [ random.randint(0, len(cards)-1) for _ in range(3) ]
            logger.info(f"Sampling results {cards_to_sample}")
            for i in cards_to_sample:
                expect(cards[i]).to_have_attribute('data-app-card-name-value', re.compile(r'.+'))
                expect(cards[i]).to_have_attribute('data-app-card-app-link-value', re.compile(r'.+'))
                expect(cards[i]).to_have_attribute('data-app-card-intra-position-value', re.compile(r'.+'))
                card_spans = cards[i].locator('span.tw-sr-only').all()
                rating = card_spans[0].evaluate_handle("node => node.parentElement").evaluate("node => node.textContent")
                assert re.match(r"\d\.\d.*", rating.strip()) is not None, "Rating locator failed test; should conform to \d\.\d.*"
                expect(card_spans[1]).to_have_text(re.compile(r".* total reviews"))

            logger.info("Passed all tests.")
            test_result = True
            return test_result
        
        except Exception as e:
            logger.debug(f"Task test_shopify_app_page failed with exception {e}")
            test_result = False
            return test_result


@task(retries=2, retry_delay_seconds=60)
def extract_shopify_apps(url: str) -> list:
    """Visits a Shopify app page and extracts app details listed on the page.
    
    Args:
        url (str): URL to visit. Should start with 'https://apps.shopify.com/search?q='

    Returns:
        data (list[dict]): List of dictionaries of app details.
    """
    logger = get_run_logger()
    data = []
    
    if test_shopify_app_page(url):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url)

            # Wait until search results container is present
            page.wait_for_selector('div.search-results-component')
            search_results = page.query_selector('div.search-results-component')

            # Select all the app cards
            app_cards = search_results.query_selector_all('div[data-controller="app-card"]')
            
            # Extract information from app cards
            for card in app_cards:
                app_name = card.get_attribute("data-app-card-name-value").strip()
                app_url = card.get_attribute("data-app-card-app-link-value").split("?")[0]
                ranking = int(card.get_attribute('data-app-card-intra-position-value'))
                spans = card.query_selector_all('span.tw-sr-only')
                average_rating_text = spans[0].evaluate_handle("node => node.parentElement").evaluate("node => node.textContent")
                average_rating = re.search(r"\d+\.\d+", average_rating_text)
                average_rating = float(average_rating.group()) if average_rating else None
                total_reviews = spans[1].text_content().strip()
                is_ad = 'surface_type=search_ad' in card.get_attribute("data-app-card-app-link-value")

                scraped_data = {
                    "app_name": app_name,
                    "app_url": app_url,
                    "ranking": ranking,
                    "average_rating": average_rating,
                    "total_reviews": total_reviews,
                    "is_ad": is_ad,
                }
                # Add hashing for comparison with warehouse
                sha256_surrogate_key = hashlib.sha256(str(scraped_data).encode("utf-8")).hexdigest()
                scraped_data["sha256_surrogate_key"] = sha256_surrogate_key

                data.append(scraped_data)
        logger.info(f"Extracted {len(data)} records from a Shopify app page.")
        return data

    else:
        logger.warning("Function test_shopify_app_page failed.")
        return


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
        
        print(f"Preparing to write {len(records_to_write)} records.")
        return records_to_write


@task(retries=2, retry_delay_seconds=30)
def load_stg_shopify_apps(data: list[dict]):
    """Loads data into stg_shopify_apps table.
    
    Args:
        data (list[dict]): List of filtered records from the Shopify app scraper.
    """
    with SqlAlchemyConnector.load("neon-postgres") as database_block:
        logger = get_run_logger()
        if len(data) > 0:
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
        else:
            logger.info("No data to write.")
            return


@flow(name="Shopify apps")
def shopify_apps_flow():
    url = 'https://apps.shopify.com/search?q=returns+and+exchanges&page=1'
    data = extract_shopify_apps(url)
    new_data = compare_sha_keys(data)
    load_stg_shopify_apps(new_data)


if __name__ == "__main__":
    github_repo = GitRepository(
            url="https://github.com/dylanlim-jy/shopify-ranker",
            credentials=GitHubCredentials.load("dylan-github-pat")
        )

    flow.from_source(
        source=github_repo,
        entrypoint="prefect/shopify_apps.py:shopify_apps_flow",
    ).deploy(
        name="shopify-apps-flow",
        work_pool_name="process-work-pool",
        cron="0 0,6,12,18 * * *"
    )