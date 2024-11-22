from ..flows.shopify_apps import shopify_apps_flow

def deploy_shopify_apps_flow():
    shopify_apps_flow.deploy(
        name="shopify-apps",
        work_pool_name="docker-pool",
        cron="0 0,6,12,18 * * *",
        push=False
    )
    return

if __name__ == "__main__":
    deploy_shopify_apps_flow()