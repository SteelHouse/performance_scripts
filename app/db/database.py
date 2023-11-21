import asyncio
import asyncpg
from contextlib import asynccontextmanager
import os
from datetime import datetime
from pytz import UTC

from ..utils.logging_config import logger


async def create_read_pool():
    USER = os.environ.get("READ_DB_USER")
    PASSWORD = os.environ.get("READ_DB_PASSWORD")
    DATABASE = os.environ.get("READ_DB_DATABASE")
    HOST = os.environ.get("READ_DB_HOST")
    PORT = os.environ.get("READ_DB_PORT")

    read_database_credentials = {
        'user': USER, 'password': PASSWORD,
        'database': DATABASE, 'host': HOST, 'port': PORT
    }

    safe_credentials = {k: v if k != 'password' else '******' for k, v in read_database_credentials.items()}

    logger.info(f"Creating read pool | {safe_credentials}")
    return await asyncpg.create_pool(**read_database_credentials)

async def create_write_pool():

    USER = os.environ.get("WRITE_DB_USER")
    PASSWORD = os.environ.get("WRITE_DB_PASSWORD")
    DATABASE = os.environ.get("WRITE_DB_DATABASE")
    HOST = os.environ.get("WRITE_DB_HOST")
    PORT = os.environ.get("WRITE_DB_PORT")


    write_database_credentials = {
        'user': USER, 'password': PASSWORD,
        'database': DATABASE, 'host': HOST, 'port': PORT
    }

    safe_credentials = {k: v if k != 'password' else '******' for k, v in write_database_credentials.items()}

    logger.info(f"Creating write pool | {safe_credentials}")
    return await asyncpg.create_pool(**write_database_credentials)





def make_date_time_with_tz(str_time):
    return UTC.localize(datetime.strptime(str_time, '%Y-%m-%d %H:%M:%S')) if str_time else None



async def get_valid_bx_campaign_group_mappings(read_pool):
    
    query = f'''
    SELECT vcg.campaign_group_id,
        vcg.name                                                                                    AS campaign_group_name,
        vcg.advertiser_id,
        vcg.company_name                                                                            AS advertiser_name,
        vcg.time_zone,
        vcg.flight_start_time,
        vcg.flight_end_time,
        vcg.budget,
        bam.beeswax_advertiser_id,
        bcgm.partner_id                                                                             AS beeswax_campaign_id,
        bam.beeswax_advertiser_id IS NOT NULL AND bcgm.partner_id IS NOT NULL                                  AS beeswax_campaign_present,
        CONCAT('https://steelhouse.beeswax.com/advertisers/', bam.beeswax_advertiser_id, '/campaigns/', bcgm.partner_id, '/edit?tab=overview') AS beeswax_campaign_url
    FROM dso.valid_campaign_groups vcg
            LEFT JOIN sync.beeswax_cgroup_mapping bcgm ON bcgm.steelhouse_id = vcg.campaign_group_id
            LEFT JOIN beeswax.advertiser_mappings bam ON vcg.advertiser_id = bam.advertiser_id
    ORDER BY vcg.advertiser_id, vcg.campaign_group_id;
    '''
    logger.info(f"Executing query | {query}")
    async with read_pool.acquire() as conn:
        return await conn.fetch(query)


async def get_valid_bx_campaign_mappings(read_pool):
      
    query = f'''
    SELECT vc.campaign_id,
        vc.name                                                                                     AS campaign_name,
        vc.campaign_group_id,
        bcm.line_item_id                                                                            AS beeswax_line_item_id,
        bam.beeswax_advertiser_id IS NOT NULL AND bcgm.partner_id IS NOT NULL AND bcm.line_item_id IS NOT NULL AS beeswax_line_item_present,
                    CONCAT('https://steelhouse.beeswax.com/advertisers/', bam.beeswax_advertiser_id, '/campaigns/', bcgm.partner_id,
                '/line_items/', bcm.line_item_id, '/edit?tab=summary')                               AS beeswax_line_item_url
    FROM dso.valid_campaigns vc
            LEFT JOIN sync.beeswax_campaign_mapping bcm ON bcm.steelhouse_id = vc.campaign_id
            LEFT JOIN sync.beeswax_cgroup_mapping bcgm ON bcgm.steelhouse_id = vc.campaign_group_id
            LEFT JOIN beeswax.advertiser_mappings bam ON vc.advertiser_id = bam.advertiser_id
    ORDER BY vc.advertiser_id, vc.campaign_group_id, vc.campaign_id;
    '''
    logger.info(f"Executing query | {query}")
    async with read_pool.acquire() as conn:
        return await conn.fetch(query)



async def get_bx_campaign_group_mappings(read_pool, campaign_groups):

    query = f'''
    
SELECT bam.beeswax_advertiser_id IS NOT NULL AND bcgm.partner_id IS NOT NULL AS beeswax_campaign_present,
       CONCAT('https://steelhouse.beeswax.com/advertisers/', bam.beeswax_advertiser_id, '/campaigns/', bcgm.partner_id,
              '/edit?tab=overview')                                          AS beeswax_campaign_url
FROM public.campaign_groups cg
         LEFT JOIN sync.beeswax_cgroup_mapping bcgm ON bcgm.steelhouse_id = cg.campaign_group_id
         LEFT JOIN beeswax.advertiser_mappings bam ON cg.advertiser_id = bam.advertiser_id
WHERE cg.campaign_group_id IN ({','.join(str(campaign_group) for campaign_group in campaign_groups)}
ORDER BY cg.advertiser_id, cg.campaign_group_id;
'''
    logger.info(f"Executing query | {query}")
    async with read_pool.acquire() as conn:
        return await conn.fetch(query)



async def get_bx_campaign_mappings(read_pool, campaigns):

    query = f'''

SELECT bam.beeswax_advertiser_id IS NOT NULL AND bcgm.partner_id IS NOT NULL AND
       bcm.line_item_id IS NOT NULL                                  AS beeswax_line_item_present,
       CONCAT('https://steelhouse.beeswax.com/advertisers/', bam.beeswax_advertiser_id, '/campaigns/', bcgm.partner_id,
              '/line_items/', bcm.line_item_id, '/edit?tab=summary') AS beeswax_line_item_url
FROM public.campaigns c
         LEFT JOIN sync.beeswax_campaign_mapping bcm ON bcm.steelhouse_id = c.campaign_id
         LEFT JOIN sync.beeswax_cgroup_mapping bcgm ON bcgm.steelhouse_id = c.campaign_group_id
         LEFT JOIN beeswax.advertiser_mappings bam ON c.advertiser_id = bam.advertiser_id
WHERE c.campaign_id IN ({','.join(str(campaign) for campaign in campaigns)}
ORDER BY c.advertiser_id, c.campaign_group_id, c.campaign_id;
    '''
    logger.info(f"Executing query | {query}")
    async with read_pool.acquire() as conn:
        return await conn.fetch(query)


