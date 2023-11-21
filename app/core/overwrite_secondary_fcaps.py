import asyncio
import json

from itertools import islice
from ..utils.logging_config import logger
from ..db.database import  create_read_pool
from ..bx import client as bx
import copy

import uuid
import os


async def get_campaign_mappings(read_pool, campaigns):

    async with read_pool.acquire() as conn:
        return await conn.fetch(f'''
    SELECT vc.campaign_id, c.campaign_template_id,
        vc.name                                                                                     AS campaign_name,
        vc.campaign_group_id,
        bcm.line_item_id                                                                            AS beeswax_line_item_id,
        bam.beeswax_advertiser_id IS NOT NULL AND bcgm.partner_id IS NOT NULL AND bcm.line_item_id IS NOT NULL AS beeswax_line_item_present,
                    CONCAT('https://steelhouse.beeswax.com/advertisers/', bam.beeswax_advertiser_id, '/campaigns/', bcgm.partner_id,
                '/line_items/', bcm.line_item_id, '/edit?tab=summary')                               AS beeswax_line_item_url
    FROM dso.valid_campaigns vc
            join public.campaigns c on c.campaign_id = vc.campaign_id
            LEFT JOIN sync.beeswax_campaign_mapping bcm ON bcm.steelhouse_id = vc.campaign_id
            LEFT JOIN sync.beeswax_cgroup_mapping bcgm ON bcgm.steelhouse_id = vc.campaign_group_id
            LEFT JOIN beeswax.advertiser_mappings bam ON vc.advertiser_id = bam.advertiser_id
    WHERE vc.campaign_id IN ({','.join([str(campaign) for campaign in campaigns])})
    ORDER BY vc.advertiser_id, vc.campaign_group_id, vc.campaign_id;

    ''')




async def handle_campaign(session, read_pool, campaign):
    logger.info(f"Handling Campaign | campaign={campaign}")

    campaign_id = campaign["campaign_id"]
    beeswax_line_item_id = campaign["beeswax_line_item_id"]
    campaign_template_id = campaign["campaign_template_id"]

    beeswax_line_item = await bx.get_beeswax_line_item(session, campaign_id, beeswax_line_item_id)

    current_fcaps = beeswax_line_item["frequency_caps"]

    

    dco_caps = [cap for cap in current_fcaps["limits"] if cap["impressions"] == 1]

    logger.info(f"Current FCaps | campaign_id={campaign_id} | beeswax_line_item_id={beeswax_line_item_id} | current_fcaps={current_fcaps} | dco_caps={dco_caps}")

    # MT+ : IP cap 25 for 30 days - template 23

    # MT : IP cap 50 for 30 days - template 15

    
    if campaign_template_id == 15:
        dco_caps.append({
            "impressions": 50,
            "duration": 2592000
        })
    
    if campaign_template_id == 23:
        dco_caps.append({
            "impressions": 25,
            "duration": 2592000
        })
   

    payload = {
        "frequency_caps": {
            "id_type": 'IP_ADDR',
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






async def main():

    impacted_campaigns = [153550, 157488, 171632, 171633, 172630, 172631, 201804, 201805, 214404, 214405]

    read_pool = await create_read_pool()
    
    campaigns = await get_campaign_mappings(read_pool, impacted_campaigns)

    valid_campaigns = []

    for campaign in campaigns:
        if not campaign["beeswax_line_item_present"]:
            logger.error(f"Skipping campaign because beeswax_line_item_id is not present | campaign={campaign}")
        else:
            valid_campaigns.append(campaign)

    max_runner = os.environ.get("MAX_RUNNER", 10)
    sem = asyncio.Semaphore(max_runner)
    async with bx.get_session() as session:

        tasks = []
        for campaign in valid_campaigns:

            tasks.append(bounded_gather(sem, handle_campaign, session, read_pool, campaign))
        
        await asyncio.gather(*tasks)

    logger.info(f"Successfully updated line items")

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