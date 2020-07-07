# gharchive - spark
1. Process can be scheduled in airflow
1. Process gharchive data downloaded from https://www.gharchive.org/
1. Download the newest GH events (current day - 1)
1. Transform data to get following fields: stars per repo/user, forks per repo/user, issues per repo/user, pull requests per repo/user.
1. Save output as parquet  


