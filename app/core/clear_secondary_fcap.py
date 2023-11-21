import asyncio
import json

from itertools import islice
from ..utils.logging_config import logger
from ..db.database import  create_read_pool, create_write_pool
from ..bx import client as bx
import copy

import uuid
import os


async def get_campaign_mappings(read_pool, campaigns):

    query = f'''
    SELECT c.campaign_id,
        c.name                                                                                     AS campaign_name,
        c.campaign_group_id,
        bcm.line_item_id                                                                            AS beeswax_line_item_id,
        bam.beeswax_advertiser_id IS NOT NULL AND bcgm.partner_id IS NOT NULL AND bcm.line_item_id IS NOT NULL AS beeswax_line_item_present,
                    CONCAT('https://steelhouse.beeswax.com/advertisers/', bam.beeswax_advertiser_id, '/campaigns/', bcgm.partner_id,
                '/line_items/', bcm.line_item_id, '/edit?tab=summary')                               AS beeswax_line_item_url
    FROM public.campaigns c
            LEFT JOIN sync.beeswax_campaign_mapping bcm ON bcm.steelhouse_id = c.campaign_id
            LEFT JOIN sync.beeswax_cgroup_mapping bcgm ON bcgm.steelhouse_id = c.campaign_group_id
            LEFT JOIN beeswax.advertiser_mappings bam ON c.advertiser_id = bam.advertiser_id
    WHERE c.campaign_id IN ({','.join([str(campaign) for campaign in campaigns])})
    ORDER BY c.advertiser_id, c.campaign_group_id, c.campaign_id;
    '''

    # query = f'''
    # SELECT c.campaign_id,
    #     c.name                                                                                     AS campaign_name,
    #     c.campaign_group_id,
    #     bcm.line_item_id                                                                            AS beeswax_line_item_id,
    #     bam.beeswax_advertiser_id IS NOT NULL AND bcgm.partner_id IS NOT NULL AND bcm.line_item_id IS NOT NULL AS beeswax_line_item_present,
    #                 CONCAT('https://steelhouse.beeswax.com/advertisers/', bam.beeswax_advertiser_id, '/campaigns/', bcgm.partner_id,
    #             '/line_items/', bcm.line_item_id, '/edit?tab=summary')                               AS beeswax_line_item_url
    # FROM public.campaigns c
    #         LEFT JOIN sync.beeswax_campaign_mapping bcm ON bcm.steelhouse_id = c.campaign_id
    #         LEFT JOIN sync.beeswax_cgroup_mapping bcgm ON bcgm.steelhouse_id = c.campaign_group_id
    #         LEFT JOIN beeswax.advertiser_mappings bam ON c.advertiser_id = bam.advertiser_id
    #         join adhoc.campaigns_to_clear_secondary_fcaps_2023_11_02 adhoc on adhoc.campaign_id = c.campaign_id
    # ORDER BY c.advertiser_id, c.campaign_group_id, c.campaign_id;
    # '''

    


    async with read_pool.acquire() as conn:
        return await conn.fetch(query)
    
async def clear_funnel(write_pool, campaign_id):
    async with write_pool.acquire() as conn:

        campaign = await conn.fetch(f'''
        SELECT campaign_id, campaign_template_id, funnel_level
        FROM public.campaigns
        WHERE campaign_id = {campaign_id}
        ''')


        await conn.execute(f'''
        UPDATE public.campaigns
        SET funnel_level = NULL
        WHERE campaign_id = {campaign_id}
        ''')

        updated_campaign = await conn.fetch(f'''
        SELECT campaign_id, campaign_template_id, funnel_level
        FROM public.campaigns
        WHERE campaign_id = {campaign_id}
        ''')

        logger.info(f"Updated campaign | campaign_id={campaign_id} | previous={campaign} | updated={updated_campaign}")



async def handle_campaign(session, read_pool, write_pool, campaign):
    logger.info(f"Handling Campaign | campaign={campaign}")

    campaign_id = campaign["campaign_id"]
    beeswax_line_item_id = campaign["beeswax_line_item_id"]

    


    try:

        beeswax_line_item = await bx.get_beeswax_line_item(session, campaign_id, beeswax_line_item_id)

        current_fcaps = beeswax_line_item["frequency_caps"]

        dco_caps = [cap for cap in current_fcaps["limits"] if cap["impressions"] == 1]

        payload = {
            "frequency_caps": {
                "id_type": current_fcaps["id_type"],
                "use_fallback": current_fcaps["use_fallback"],
                'id_vendor': current_fcaps["id_vendor"],
                'id_vendor_id': current_fcaps["id_vendor_id"],
                "limits": dco_caps
            }
        }

        logger.info(f"Updating line item | campaign_id={campaign_id} | beeswax_line_item_id={beeswax_line_item_id} | current_fcaps={current_fcaps} | payload={payload}")
        
        updated_line_item = await bx.update_beeswax_line_item(session, campaign_id, beeswax_line_item_id, payload)
        
        updated_fcaps = updated_line_item["frequency_caps"]
        
        logger.info(f"Updated line item | campaign_id={campaign_id} | beeswax_line_item_id={beeswax_line_item_id} | previous={current_fcaps} | updated={updated_fcaps}")
        
        logger.info(f"Clearing funnel | campaign_id={campaign_id}")
        
        await clear_funnel(write_pool, campaign_id)
        
        logger.info(f"Successfully cleared funnel | campaign_id={campaign_id}")
    except Exception as e:
        logger.error(f"Failed to update line item | campaign_id={campaign_id} | beeswax_line_item_id={beeswax_line_item_id} | error={e}")
    

    







async def main():


    impacted_campaigns = [
220260, 223004
        ]


    jira_ticket = 'https://mntn.atlassian.net/browse/PER-1889'
    
    read_pool = await create_read_pool()
    write_pool = await create_write_pool()
    
    campaigns = await get_campaign_mappings(read_pool, impacted_campaigns)

    valid_campaigns = []

    for campaign in campaigns:
        if not campaign["beeswax_line_item_present"]:
            logger.error(f"Skipping campaign because beeswax_line_item_id is not present | campaign={campaign}")
        else:
            valid_campaigns.append(campaign)

    max_runner = os.environ.get("MAX_RUNNER", 10)
    sem = asyncio.Semaphore(max_runner)

    logger.info(f"Clearing secondary frequency caps for campaigns | campaigns={len(campaigns)} | valid_campaigns={len(valid_campaigns)} | jira_ticket={jira_ticket}")

    async with bx.get_session() as session:

        tasks = []
        for campaign in valid_campaigns:

            tasks.append(bounded_gather(sem, handle_campaign, session, read_pool, write_pool, campaign))
        
        await asyncio.gather(*tasks)

    logger.info(f"Successfully cleared secondary frequency caps")

    await read_pool.close()
    await write_pool.close()


async def bounded_gather(sem, task_func, *args):
    await sem.acquire()  # Try to acquire the semaphore
    try:
        result = await task_func(*args)
        return result
    finally:
        sem.release()  # Release the semaphore so that another task can start

    

if __name__=='__main__':
    from dotenv import load_dotenv
    load_dotenv(".env")    
    asyncio.run(main())