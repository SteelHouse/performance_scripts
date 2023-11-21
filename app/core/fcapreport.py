import asyncio


from ..utils.async_utils import bounded_gather
from ..utils.logging_config import logger
from ..db.database import  create_read_pool
from ..bx import client as bx


import uuid
import os


async def get_campaign_mappings(read_pool):

    query = f'''
    SELECT c.campaign_id,
        c.name                                                                                     AS campaign_name,
        c.campaign_group_id,
        c.advertiser_id,
        a.company_name,
        bcm.line_item_id                                                                            AS beeswax_line_item_id,
        bam.beeswax_advertiser_id IS NOT NULL AND bcgm.partner_id IS NOT NULL AND bcm.line_item_id IS NOT NULL AS beeswax_line_item_present,
                    CONCAT('https://steelhouse.beeswax.com/advertisers/', bam.beeswax_advertiser_id, '/campaigns/', bcgm.partner_id,
                '/line_items/', bcm.line_item_id, '/edit?tab=summary')                               AS beeswax_line_item_url
    FROM public.campaigns c
            JOIN public.advertisers a ON c.advertiser_id = a.advertiser_id
            JOIN dso.valid_campaigns vc ON vc.campaign_id = c.campaign_id
            LEFT JOIN sync.beeswax_campaign_mapping bcm ON bcm.steelhouse_id = c.campaign_id
            LEFT JOIN sync.beeswax_cgroup_mapping bcgm ON bcgm.steelhouse_id = c.campaign_group_id
            LEFT JOIN beeswax.advertiser_mappings bam ON c.advertiser_id = bam.advertiser_id
    ORDER BY c.advertiser_id, c.campaign_group_id, c.campaign_id;
    '''
    

    async with read_pool.acquire() as conn:
        return await conn.fetch(query)
    


async def get_line_item(session, campaign):

    # logger.info(f"Getting line item | campaign={campaign}")
    
    beeswax_line_item_id = campaign["beeswax_line_item_id"]
    campaign_id = campaign["campaign_id"]
    campaign_group_id = campaign["campaign_group_id"]
    advertiser_id = campaign["advertiser_id"]
    company_name = campaign["company_name"]
    campaign_name = campaign["campaign_name"]
    beeswax_line_item_url = campaign["beeswax_line_item_url"]

    try:
        beeswax_line_item = await bx.get_beeswax_line_item(session, campaign_id, beeswax_line_item_id)

        current_fcaps = beeswax_line_item["frequency_caps"]
        id_type = current_fcaps["id_type"]
        limits = current_fcaps["limits"]

        logger.info(f"{advertiser_id}|{company_name}|{campaign_group_id}|{campaign_id}|{campaign_name}|{beeswax_line_item_id}|{id_type}|{limits}|{beeswax_line_item_url}")
    
    except Exception as e:
        logger.error(f"Failed to get beeswax line item | campaign={campaign} | error={e}")
        return


async def main():

    read_pool = await create_read_pool()
    
    campaigns = await get_campaign_mappings(read_pool)

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
            tasks.append(bounded_gather(sem, get_line_item, session, campaign))
        
        await asyncio.gather(*tasks)

    await read_pool.close()






if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv(".env")    
    asyncio.run(main())