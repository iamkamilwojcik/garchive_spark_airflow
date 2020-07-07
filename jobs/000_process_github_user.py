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
    actor.id login_id,
    count(1) as cnt
    FROM allcols 
    where type = 'IssuesEvent' and payload.action = 'opened'
    GROUP BY actor.id
    """)
    log.info("issues_df created")


    pr_df = spark_session.sql("""
    SELECT 
    actor.id login_id,
    count(1) as cnt
    FROM allcols 
    where type = 'PullRequestEvent' and payload.action = 'opened'
    GROUP BY actor.id
    """)
    log.info("pr_df created")


    starred_df = spark_session.sql("""
    SELECT DISTINCT
    actor.id login_id,
    count(1) as cnt
    FROM allcols
    where type = 'WatchEvent' and payload.action = 'started'
    GROUP BY actor.id
    """)
    log.info("starred_df created")

    user_df = spark_session.sql("""
    SELECT DISTINCT 
    actor.id login_id,
    actor.login login_name 
    FROM allcols""")
    log.info("user_df created")

    user_df.createOrReplaceTempView("user_df")
    starred_df.createOrReplaceTempView("starred_df")
    pr_df.createOrReplaceTempView("pr_df")
    issues_df.createOrReplaceTempView("issues_df")
    log.info("temp views created")

    result = spark_session.sql("""
    SELECT 
    u.*,
    s.cnt s_cnt, 
    pr.cnt pr_cnt, 
    i.cnt i_cnt
    FROM user_df u
    LEFT JOIN starred_df s on s.login_id = u.login_id
    LEFT JOIN pr_df pr on pr.login_id = u.login_id
    LEFT JOIN issues_df i on i.login_id = u.login_id
    """)
    log.info("result created")


def load():
    result.write.mode("overwrite").parquet(os.path.join(sys.argv[1], "activity_per_user.parquet"))
    log.info("parquet created")

def main():
    extract()
    transform()
    load()

if __name__ == '__main__':
    main()