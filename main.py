from google.colab import auth
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from IPython.display import display, HTML
from google.cloud import bigquery

# Authenticate user
auth.authenticate_user()
# View plots
%matplotlib inline

#variable dictionary
dc = {}
dc['project_id'] = XXXXXXXX
dc['dataset_name'] = XXXXXXXX
dc['table_name'] = 'ga_sessions_*'
dc['start_date'] = '20180101' 
dc['end_date'] = '20200101' 
dc['output_table'] = 'attribute_explorer'
dc['billing_project_id'] = XXXXXXXX
dc['billing_dataset_name'] = XXXXXXXX

#feature/attribute tuple list
features = [
  # time-based
  ("Time/Date", "Hour of Day","CAST(EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime) AT TIME ZONE 'Asia/Hong_Kong') as STRING)"),
  ("Time/Date", "Period of Day", "IF(EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime) AT TIME ZONE 'Asia/Hong_Kong') >= 12, 'PM', 'AM')"),
  ("Time/Date", "Day of Week", "CAST(EXTRACT(DAYOFWEEK FROM parse_date('%Y%m%d', sessions.date)) AS STRING)"),
  ("Time/Date", "Week of Year", "CAST(EXTRACT(WEEK FROM parse_date('%Y%m%d', sessions.date)) AS STRING)"),
  ("Time/Date", "Month", "CAST(EXTRACT(MONTH FROM parse_date('%Y%m%d', sessions.date)) AS STRING)"),
  ("Time/Date", "Quarter", "CAST(EXTRACT(QUARTER FROM parse_date('%Y%m%d', sessions.date)) AS STRING)"),
  # source-based	
  ("Origin", "Channel","sessions.channelGrouping"),
  ("Origin", "Medium","sessions.trafficSource.medium"),
  ("Origin", "Source","sessions.trafficSource.source"),
  ("Origin", "Source-Medium","concat(sessions.trafficSource.source,'-',sessions.trafficSource.medium)"),
  ("Origin", "Referral Path","sessions.trafficSource.referralPath"),
  #("Origin", "Landing Page","CONCAT(hits.page.hostname, hits.page.pagePath)"),
  # GoogleAds-based
  ("Ads", "Ads Content", "sessions.trafficSource.adContent"),
  ("Ads", "Ads Campaign ID", "CAST(sessions.trafficSource.adwordsClickInfo.campaignId AS STRING)"),
  ("Ads", "Ads Network Type", "sessions.trafficSource.adwordsClickInfo.adNetworkType"),
  # Behaviour-based
  ("Behaviour", "User Type", "if(sessions.visitNumber = 1,'new_user','returning_user')"),
  ("Behaviour", "Social Engagement", "sessions.socialEngagementType"),
  #("Behaviour", "Page Views", "CAST(sessions.totals.pageviews AS STRING)"),
  #("Behaviour", "Session Duration Mins", "CAST(totals.timeOnSite/60 AS STRING)"),
  # tech-based
  ("User Agent", "Device Type","sessions.device.deviceCategory"),
  ("User Agent", "Operating System","sessions.device.operatingSystem"),
  ("User Agent", "Browser","sessions.device.browser"),
  ("User Agent", "Language","sessions.device.language"),
  ("User Agent", "Screen Pixels (e5)","case when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 100 then '>100' when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 90 then '90-99' when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 80 then '80-89' when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 70 then '70-79' when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 60 then '60-69' when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 50 then '50-59' when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 40 then '40-49' when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 30 then '30-39' when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 20 then '20-29'       when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 10 then '10-19' when IF(ARRAY_LENGTH(SPLIT(sessions.device.screenResolution,'x')) = 2,ROUND(CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(0)] AS INT64) * CAST(SPLIT(sessions.device.screenResolution,'x')[OFFSET(1)] AS INT64)/100000), Null) >= 0  then  '0-9' else 'wtf' end"),
  # geo-based
  ("Geo", "Country","sessions.geoNetwork.country"),
  ("Geo", "Region","sessions.geoNetwork.region"),
  ("Geo", "City","sessions.geoNetwork.city")
  ]

# Page views, session duration,  New / returning user, socially-engaged? 

#render start query
def create_clause(dc):
  fq = '''CREATE TABLE `{billing_project_id}.{billing_dataset_name}.{output_table}` AS
       '''.format(**dc)
  return fq

#render subquery
def feature_subquery(dc):
  fq = '''
  SELECT
    category, 
    feature,
    attribute,     
    conversions, 
    sessions,
    conversion_rate,
    ROUND(( SUM(conversions) OVER() / SUM(sessions) OVER() )*100,2) AS baseline_conversion_rate,
    ROUND((conversion_rate - ((SUM(conversions) OVER()/SUM(sessions) OVER())*100) ) / ((SUM(conversions) OVER()/SUM(sessions) OVER())*100)*100,2) AS conversion_rate_uplift,
    ROUND((sessions/SUM(sessions) OVER())*100,2) AS segment_size_percent,
    SUM(conversions) OVER() AS total_conversions,
    SUM(sessions) OVER() AS total_sessions        
    
  FROM (
    SELECT
      "{category}" AS category,
      "{feature}" AS feature,
      {attribute} AS attribute,         
      SUM(IFNULL(sessions.totals.transactions,0)) AS conversions,
      COUNT(sessions.visitStartTime) AS sessions,
      ROUND((SUM(IFNULL(sessions.totals.transactions,0))/COUNT(sessions.visitStartTime))*100,2) AS conversion_rate      
    FROM
    `{project_id}.{dataset_name}.{table_name}` as sessions,
      UNNEST(hits) AS hits
    WHERE
      hits.hitNumber = 1
      AND date >= '{start_date}'
      AND date <= '{end_date}'
    GROUP BY
      1,2,3 )
    '''.format(**dc)
  
  return fq

#render final query 
def final_query(dc, features):
    fq = create_clause(dc)
    for i in features: 
      dc['category'] = i[0]
      dc['feature'] = i[1]
      dc['attribute'] = i[2]
      if (features.index(i) + 1) < len(features):
        union_val = '''UNION ALL'''
      else: 
        union_val = '''ORDER BY (conversion_rate - ((SUM(conversions) OVER()/SUM(sessions) OVER())*100) ) / ((SUM(conversions) OVER()/SUM(sessions) OVER())*100) DESC  '''       
      fq = fq + feature_subquery(dc) + union_val
      
    return fq

query = final_query(dc, features)
