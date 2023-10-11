#%%
import os
import pandas as pd
import google.auth
from google.cloud import bigquery
from google.oauth2 import service_account, credentials
from slack_sdk import WebClient
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
import tempfile

channel_id = 'C051CKRNNRZ'
linh_id = "U047QMYB9TQ"
bot_auth_token = 'xoxb-3458316355-4272711174212-TYd7W16TXdGA0AXYujddJBET'
slack_client = WebClient(token = bot_auth_token)

SCOPE = ["https://www.googleapis.com/auth/cloud-platform" , "https://www.googleapis.com/auth/drive",] 
credentials, project_id = google.auth.default(scopes=SCOPE)

datalake_repo = bigquery.Client(project='prj-ts-p-analytic-8057', credentials = credentials)

# adc = "application_default_credentials.json"
# creds = credentials.Credentials.from_authorized_user_file(
#         adc,
#         scopes = ["https://www.googleapis.com/auth/cloud-platform" , "https://www.googleapis.com/auth/drive",] ,)
# datalake_repo = bigquery.Client(project='prj-ts-p-analytic-8057', credentials = creds)

key_path = "avay_creds.json"
creds_avay = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],)
client_avay = bigquery.Client(credentials=creds_avay, project='avay-a9925')

dhour = datetime.now().strftime("%d-%m-%Y-%H")


file_name = f'temp_only_cost_{dhour}.csv'
root = os.path.dirname(os.path.abspath(__file__))
def _path_file(file_name):
	file_path = '/tmp/' + file_name
	file_path = os.path.join(root, file_path)
	return file_path

file_path = _path_file(file_name)


#%%
def upddate_cost_pmk():
    #remove old data
    remove_data = ("delete from `prj-ts-p-analytic-8057.da_adhoc.evo_mkt_inhouse_cost` where 1=1")
    query = datalake_repo.query(remove_data)
    results = query.result()

    #insert new data
    cam_info = datalake_repo.query(
    f"""
        select distinct
        'GOOGLE' as mkt_channel_subgroup,
        'PERFORMANCE MARKETING' as mkt_channel_group,
        'Digital MKT' as unit,
        cast(cam.campaign_id as string) as campaignid,
        cast(customer_id as string) as mkt_channel,
        campaign_name as campaign
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_Campaign_4217645990` cam
        inner join
        (select campaign_id, max(_PARTITIONTIME) as max_date
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_Campaign_4217645990` group by 1) max
        on cam.campaign_id = max.campaign_id and cam._PARTITIONTIME = max.max_date
        """).result().to_arrow().to_pandas()

    adgroup_info = datalake_repo.query(
    f"""
        select distinct
        cast(ad.ad_group_id as string) as adgroupid,
        ad_group_name as adgroup
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_AdGroup_4217645990` ad
        inner join
        (select ad_group_id, max(_partitiontime) as max_date
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_AdGroup_4217645990` group by 1) max
        on ad.ad_group_id = max.ad_group_id and ad._partitiontime = max.max_date
        """).result().to_arrow().to_pandas()

    ggads = datalake_repo.query(
    f"""
        with
    all_camp as (
    select
        segments_date as date,
        campaign_id,
        sum(metrics_clicks) as clicks,
        sum(metrics_impressions) as impressions,
        sum(metrics_cost_micros) as cost
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_CampaignBasicStats_4217645990`
        where customer_id in (8131246892, 3154428833) and segments_date >='2023-04-01' and campaign_id != 19419678859
        group by 1,2)
    , camp_ad as (
    select
        segments_date as date,
        campaign_id,
        sum(metrics_clicks) as clicks,
        sum(metrics_impressions) as impressions,
        sum(metrics_cost_micros) as cost
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_AdGroupBasicStats_4217645990`
        where customer_id in (8131246892, 3154428833) and segments_date >='2023-04-01' and campaign_id != 19419678859
        group by 1,2)
    , camp_no_ad as (
        select
        all_camp.date,
        all_camp.campaign_id,
        0 as ad_group_id,
        all_camp.clicks,
        all_camp.impressions,
        all_camp.cost
        from all_camp left join camp_ad using (date, campaign_id)
        where camp_ad.impressions != all_camp.impressions),
    total_camp as (
        select
        segments_date as date,
        campaign_id,
        ad_group_id,
        sum(metrics_clicks) as clicks,
        sum(metrics_impressions) as impressions,
        sum(metrics_cost_micros) as cost
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_AdGroupBasicStats_4217645990`
        where customer_id in (8131246892, 3154428833) and segments_date >='2023-04-01'  and campaign_id != 19419678859
        group by 1,2,3
        union distinct
        select *
        from camp_no_ad)
        , search_camp as (
        select
        segments_date as date,
        campaign_id,
        ad_group_id,
        sum(metrics_clicks) as clicks_search,
        sum(metrics_impressions) as impressions_search,
        sum(metrics_cost_micros) as cost_search
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_KeywordBasicStats_4217645990`
        where customer_id in (8131246892, 3154428833) and segments_date >= '2023-04-01'
        group by 1,2,3)
        ,criteria_name as (
        select distinct
        ad_group_criterion_criterion_id,
        campaign_id,
        ad_group_id,
        ad_group_criterion_keyword_text as Criteria
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_Keyword_4217645990`)
        , criteria_perf as (
        select
        segments_date as date,
        campaign_id,
        ad_group_id,
        ad_group_criterion_criterion_id,
        sum(metrics_clicks) as clicks,
        sum(metrics_impressions) as impressions,
        sum(metrics_cost_micros) as cost
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_KeywordBasicStats_4217645990`
        where customer_id in (8131246892, 3154428833) and segments_date >= '2023-04-01'
        group by 1,2,3,4)
        , map_key as (
        select
        date,
        cast(campaign_id as string) as campaignid,
        cast(ad_group_id as string) as adgroupid,
        coalesce(criteria,cast(ad_group_criterion_criterion_id as string)) as criteria,
        clicks,
        impressions,
        cost as cost
        from criteria_perf
        left join criteria_name using (campaign_id, ad_group_id, ad_group_criterion_criterion_id))
        , discovery_camp as (
        select 
        segments_date as date,
        cast(campaign_id as string) as campaignid,
        cast(ad_group_id as string) as adgroupid,
        cast(ad_group_ad_ad_id as string) as criteria,
        sum(metrics_clicks) as clicks,
        sum(metrics_impressions) as impressions,
        sum(metrics_cost_micros) as cost,
        from `prj-ts-p-etl-infra-45d9.adwords_5990.p_ads_AdBasicStats_4217645990`
        where true and campaign_id = 19419678859 and segments_date >= '2023-04-01'
        group by 1,2,3,4
        )
        , final as (
        select
        date,
        cast(campaign_id as string) as campaignid,
        cast(ad_group_id as string) as adgroupid,
        'No keyword' as criteria,
        clicks - coalesce(clicks_search,0) as clicks,
        impressions - coalesce(impressions_search,0) as impressions,
        cost - coalesce(cost_search,0) as cost
        from total_camp left join search_camp using (date,campaign_id, ad_group_id)
        where cost <> coalesce(cost_search,0)
        union all
        select
        * from map_key
        union all
        select
        * from discovery_camp
        )
        
        select *
        from final
        """).result().to_arrow().to_pandas()
    
    ggads['cost'] = ggads['cost']/1000000*23500

    faceads = client_avay.query(
    f"""
        select
            Date_start as date,
            'FACEBOOK' as mkt_channel_subgroup,
            'PERFORMANCE MARKETING' as mkt_channel_group,
            account_name as mkt_channel,
            'Digital MKT' as unit,
            campaign_id as campaignid,
            campaign_name as campaign,
            adset_id as adgroupid,
            adset_name as adgroup,
            case when campaign_name = 'ChatEVO_Traffic' then 'ChatEVO_Ad0104' else ad_name end as criteria,
            sum(clicks) as clicks,
            sum(spend) as cost,
            sum(impressions) as impressions
        from `avay-a9925.dwh.ad_insights`
        where account_id != '809675919382650'
        group by 1,2,3,4,5,6,7,8,9,10
        """).result().to_arrow().to_pandas()   

    #read missinging data from facebook
    fbads_jun = pd.read_csv('facebook_ad_jun_23.csv')
    fbads_sep = pd.read_csv('facebook_ad_sep_21.csv')
    fbads_missing = pd.concat([fbads_jun, fbads_sep])

    fbads_missing['date']  = pd.to_datetime(fbads_missing['date']).dt.date
    fbads_missing['mkt_channel_subgroup'] = 'FACEBOOK'
    fbads_missing['mkt_channel_group'] = 'PERFORMANCE MARKETING'
    fbads_missing['mkt_channel'] = 'neo'
    fbads_missing['unit'] = 'Digital MKT'
    fbads_missing['campaignid'] = None
    fbads_missing['adgroupid'] = None

    fbads_missing= fbads_missing[['date', 'mkt_channel_subgroup', 'mkt_channel_group', 'mkt_channel', 'unit', 'campaignid', 'campaign', 'adgroupid', 'adgroup', 'criteria', 'clicks', 'cost', 'impressions']]
    faceads = pd.concat([faceads, fbads_missing])

    #mapping google & goole before April 2023
    ggads_apr = pd.read_csv('ggads_before_april.csv')
    ggads_apr['campaignid'] = ggads_apr['campaignid'].astype(str)
    ggads_apr['adgroupid'] = ggads_apr['adgroupid'].astype(str)
    ggads_apr['date']  = pd.to_datetime(ggads_apr['date']).dt.date
    ggads_apr['cost']  = ggads_apr['cost']*23500

    ggads_all = pd.concat([ggads,ggads_apr])

    mapggads = ggads_all.merge(cam_info, how = 'left', on = 'campaignid').merge(adgroup_info, how= 'left', on = 'adgroupid')
    mapggads = mapggads[['date', 'mkt_channel_subgroup', 'mkt_channel_group', 'mkt_channel', 'unit', 'campaignid', 'campaign',
                        'adgroupid', 'adgroup', 'criteria', 'clicks', 'cost', 'impressions']]
    
    #mapping google & face
    map_cost = pd.concat([mapggads, faceads])
    map_cost['mkt_channel'] = map_cost['mkt_channel'].astype(str)
    map_cost['clicks'] = map_cost['clicks'].astype(float).round(0)
    map_cost['cost'] = map_cost['cost'].astype(float).round(8)
    map_cost['impressions'] = map_cost['impressions'].astype(float).round(0)
    map_cost['campaign_map'] = np.where(map_cost['mkt_channel_subgroup'] == 'GOOGLE', map_cost['campaignid'], map_cost['campaign'])
    map_cost['adgroup_map'] = np.where(map_cost['mkt_channel_subgroup'] == 'GOOGLE', map_cost['adgroupid'], map_cost['adgroup'])
    
    map_cost.to_csv(file_path, index=False)
    schema = [
        bigquery.SchemaField('date', 'DATE'),
        bigquery.SchemaField('mkt_channel_subgroup', 'STRING'),
        bigquery.SchemaField('mkt_channel_group', 'STRING'),
        bigquery.SchemaField('mkt_channel', 'STRING'),
        bigquery.SchemaField('unit', 'STRING'),
        bigquery.SchemaField('campaignid', 'STRING'),
        bigquery.SchemaField('campaign', 'STRING'),
        bigquery.SchemaField('adgroupid', 'STRING'),
        bigquery.SchemaField('adgroup', 'STRING'),
        bigquery.SchemaField('criteria', 'STRING'),
        bigquery.SchemaField('clicks', 'BIGNUMERIC'),
        bigquery.SchemaField('cost', 'BIGNUMERIC'),
        bigquery.SchemaField('impressions', 'BIGNUMERIC'),
        bigquery.SchemaField('campaign_map', 'STRING'),
        bigquery.SchemaField('adgroup_map', 'STRING'),
    ]

    dataset_id = 'da_adhoc'
    table_id = 'evo_mkt_inhouse_cost'
    table_ref = datalake_repo.dataset(dataset_id).table(table_id)
    # table = datalake_repo.get_table(table_ref)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1

    with open(file_path, 'rb') as source_file:
        job = datalake_repo.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for the job to complete.
    text = f'Loaded {job.output_rows} rows into {dataset_id}.{table_id}.'
    return text


#%%
def main():
    try: 
        text = upddate_cost_pmk()
        
        slack_client.chat_postMessage(channel=channel_id, text=f'Successfully: {text}')
    except Exception as e: 
        slack_client.chat_postMessage(channel=channel_id, text=f'<@{linh_id}>: {str(e)}')
    return f'Job done!'


# %%
main()
# %%
