from prefect import flow, get_run_logger
from prefect_github import GitHubCredentials
from prefect.runner.storage import GitRepository

@flow
def test_flow():
    logger = get_run_logger()
    logger.info("Testing a flow")
    return


if __name__ == "__main__":
    github_repo = GitRepository(
            url="https://github.com/dylanlim-jy/shopify-ranker",
            credentials=GitHubCredentials.load("dylan-github-pat")
        )

    flow.from_source(
        source=github_repo,
        entrypoint="prefect/test_flow.py:test_flow",
    ).deploy(
        name="test-private-github-deploy",
        work_pool_name="docker-pool",
        cron="* * * * *"
    )