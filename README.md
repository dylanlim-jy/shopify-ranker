This is a project to track changes for Shopify app listings of a specific set of keywords. It will use Playwright, Prefect, Neon, and dbt.

## Overview

1. Test if the Shopify app page has the HTML structure that we are expecting.
2. Write a Playwright scraper to extract each app listing's data:

- App store URL
- App name
- Ranking
- Average rating
- Total reviews
- Ad status

3. Check against Neon to see if records exist and are different.
4. Only write different records into Neon.
5. Using dbt, transform the source data into biggest changes for ratings and number of reviews in day, month, and year timescales.
6. Visualise the results (TBD)

## Deployment notes

`docker build -t shopify-apps-worker .`
`docker run -d --env-file .env --name saw-container shopify-apps-worker`
