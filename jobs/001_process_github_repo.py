from base.start_spark import spark_app
import sys
import os
import logging as log

log.basicConfig(level="INFO")

spark = spark_app(app_name='userAgregate')
spark_session = spark.spark_start()


def extract():
    global df
    spark_out_path = os.path.join(sys.argv[1], "*.json")
    df = spark_session.read.json(spark_out_path)
    log.info("extracted raw data")

def transform():
    global result
    df.createOrReplaceTempView("allcols")

    issues_df = spark_session.sql("""
    SELECT
    payload.issue.id issue_id,
    repo.id repo_id,
    payload.issue.title issue_title
    FROM allcols 
    where type = 'IssuesEvent' and payload.action = 'opened'
    """)
    log.info("issues_df created")

    pr_df = spark_session.sql("""
    SELECT 
    payload.pull_request.id pr_id,
    repo.id repo_id,
    payload.pull_request.title title
    FROM allcols 
    where type = 'PullRequestEvent' and payload.action = 'opened'
    """)
    log.info("pr_df created")

    starred_df = spark_session.sql("""
    SELECT 
    distinct
    actor.id login_id,
    repo.id repo_id
    FROM allcols
    where type = 'WatchEvent' and payload.action = 'started'
    """)
    log.info("starred_df created")

    forked_df = spark_session.sql("""
    SELECT
    distinct
    actor.id login_id, 
    repo.id repo_id
    FROM allcols 
    WHERE type = 'ForkEvent'
    """)
    log.info("forked_df created")

    project_df = spark_session.sql("""
    SELECT DISTINCT 
    repo.id repo_id, 
    repo.name repo_name
    FROM allcols 
    """)
    log.info("project_df created")

    project_df.createOrReplaceTempView("project_df")
    forked_df.createOrReplaceTempView("forked_df")
    starred_df.createOrReplaceTempView("starred_df")
    pr_df.createOrReplaceTempView("pr_df")
    issues_df.createOrReplaceTempView("issues_df")
    log.info("temp views created")

    result = spark_session.sql("""
    WITH forked AS (
    SELECT 
    p.repo_id repo_id, 
    p.repo_name repo_name,
    count(1) number_of_unique_forks
    FROM project_df as p
    JOIN forked_df f on p.repo_id = f.repo_id
    GROUP BY p.repo_id, p.repo_name
    ),
    issues AS (
    SELECT 
    p.repo_id repo_id, 
    p.repo_name repo_name,
    count(1) number_of_issues
    FROM project_df as p
    JOIN issues_df i on p.repo_id = i.repo_id
    GROUP BY p.repo_id, p.repo_name
    ),
    starred AS (
    SELECT 
    p.repo_id repo_id, 
    p.repo_name repo_name,
    count(1) number_of_stars
    FROM project_df as p
    JOIN starred_df s on p.repo_id = s.repo_id
    GROUP BY p.repo_id, p.repo_name
    ),
    pr AS (
    SELECT 
    p.repo_id repo_id, 
    p.repo_name repo_name,
    count(1) number_of_pr
    FROM project_df as p
    JOIN pr_df pr on p.repo_id = pr.repo_id
    GROUP BY p.repo_id, p.repo_name
    )
    SELECT p.*, 
    i.number_of_issues,
    f.number_of_unique_forks,
    pr.number_of_pr,
    s.number_of_stars
    FROM project_df p
    LEFT JOIN issues i on i.repo_id = p.repo_id
    LEFT JOIN forked f on f.repo_id = p.repo_id
    LEFT JOIN pr on pr.repo_id = p.repo_id
    LEFT JOIN starred s on s.repo_id = p.repo_id
    """)
    log.info("result created")

def load():
    result.write.mode("overwrite").parquet(os.path.join(sys.argv[1], "activity_per_repo.parquet"))
    log.info("parquet created")

def main():
    extract()
    transform()
    load()

if __name__ == '__main__':
    main()
