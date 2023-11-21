import asyncio
import json

from itertools import islice
from ..utils.logging_config import logger
from ..db.database import get_campaign_group_mappings, get_campaign_mappings,save_manual_campaign_group_flight_update, save_manual_campaign_flight_update
from ..bx import client as bx


import uuid
import os

async def handle_campaign_group(session, read_pool, write_pool, campaign_group, campaigns, transaction_id, batch_id):
    
    logger.info(f"Handling Campaign Group | campaign_group={campaign_group} | transaction_id={transaction_id} | batch_id={batch_id}")

    campaign_group_id = campaign_group["campaign_group_id"]
    beeswax_campaign_id = campaign_group["beeswax_campaign_id"]
    flight_budget = campaign_group["budget"]
    flight_start_time = campaign_group["flight_start_time"].strftime('%Y-%m-%d %H:%M:%S')
    flight_end_time = campaign_group["flight_end_time"].strftime('%Y-%m-%d %H:%M:%S')

    
    beeswax_campaign = await bx.get_beeswax_campaign(session, campaign_group_id, beeswax_campaign_id, transaction_id, batch_id)

    bx_flight_budget = beeswax_campaign["campaign_budget"]
    bx_flight_start_time = beeswax_campaign["start_date"]
    bx_flight_end_time = beeswax_campaign["end_date"]

    if bx_flight_start_time != flight_start_time or bx_flight_end_time != flight_end_time:

        logger.info(f"Beeswax campaign flight times don't match MNTN campaign group flight times | campaign_group_id={campaign_group_id} | beeswax_campaign_id={beeswax_campaign_id} | flight_start_time={flight_start_time} | flight_end_time={flight_end_time} | bx_flight_start_time={bx_flight_start_time} | bx_flight_end_time={bx_flight_end_time} | transaction_id={transaction_id} | batch_id={batch_id}")

        # await update_beeswax_campaign_times(session, campaign_group_id, beeswax_campaign_id, flight_start_time, flight_end_time, transaction_id, batch_id)

        updated_beeswax_campaign = await bx.get_beeswax_campaign(session, campaign_group_id, beeswax_campaign_id, transaction_id, batch_id)
        
        await save_manual_campaign_group_flight_update(write_pool, campaign_group_id, beeswax_campaign_id, bx_flight_budget, bx_flight_start_time, bx_flight_end_time, flight_budget, flight_start_time, flight_end_time, "Updated flight times", transaction_id, batch_id)

        if updated_beeswax_campaign["start_date"] != flight_start_time or updated_beeswax_campaign["end_date"] != flight_end_time:        
            logger.error(f"Failed to update flight times for campaign group | campaign_group_id={campaign_group_id} | beeswax_campaign_id={beeswax_campaign_id} | transaction_id={transaction_id} | batch_id={batch_id}")
        else:
            # await save_manual_campaign_group_flight_update(write_pool, campaign_group_id, beeswax_campaign_id, bx_flight_budget, bx_flight_start_time, bx_flight_end_time, flight_budget, flight_start_time, flight_end_time, "Updated flight times", transaction_id, batch_id)
            logger.info(f"Successfully updated flight times for campaign group | campaign_group_id={campaign_group_id} | beeswax_campaign_id={beeswax_campaign_id} | transaction_id={transaction_id} | batch_id={batch_id}")
    else:
        logger.info(f"Beeswax flight times campaign match MNTN campaign group flight times, no updated needed | campaign_group_id={campaign_group_id} | beeswax_campaign_id={beeswax_campaign_id} | flight_start_time={flight_start_time} | flight_end_time={flight_end_time} | bx_flight_start_time={bx_flight_start_time} | bx_flight_end_time={bx_flight_end_time} | transaction_id={transaction_id} | batch_id={batch_id}")


    tasks = [handle_campaign(session, read_pool, write_pool, flight_start_time, flight_end_time, campaign, transaction_id, batch_id) for campaign in campaigns]

    await asyncio.gather(*tasks)  


async def handle_campaign(session, read_pool, write_pool, flight_start_time, flight_end_time, campaign, transaction_id, batch_id):
    logger.info(f"Handling Campaign | campaign={campaign} | transaction_id={transaction_id} | batch_id={batch_id}")

    campaign_id = campaign["campaign_id"]
    beeswax_line_item_id = campaign["beeswax_line_item_id"]

    beeswax_line_item = await bx.get_beeswax_line_item(session, campaign_id, beeswax_line_item_id, transaction_id, batch_id)

    bx_flight_start_time = beeswax_line_item["start_date"]
    bx_flight_end_time = beeswax_line_item["end_date"]

    if bx_flight_start_time != flight_start_time or bx_flight_end_time != flight_end_time:

        logger.info(f"Beeswax line item flight times don't match MNTN campaign flight times | campaign_id={campaign_id} | beeswax_line_item_id={beeswax_line_item_id} | flight_start_time={flight_start_time} | flight_end_time={flight_end_time} | bx_flight_start_time={bx_flight_start_time} | bx_flight_end_time={bx_flight_end_time} | transaction_id={transaction_id} | batch_id={batch_id}")

        # updated_line_item = await bx.update_beeswax_line_item_times(session, campaign_id, beeswax_line_item_id, flight_start_time, flight_end_time, transaction_id, batch_id)
        updated_line_item = await bx.get_beeswax_line_item(session, campaign_id, beeswax_line_item_id, transaction_id, batch_id)

        await save_manual_campaign_flight_update(write_pool, campaign_id, beeswax_line_item_id, bx_flight_start_time, bx_flight_end_time, flight_start_time, flight_end_time, "Updated flight times", transaction_id, batch_id)

        if updated_line_item["start_date"] != flight_start_time or updated_line_item["end_date"] != flight_end_time:
            logger.error(f"Failed to update flight times for campaign | campaign_id={campaign_id} | beeswax_line_item_id={beeswax_line_item_id} | transaction_id={transaction_id} | batch_id={batch_id}")
        else:
            # await save_manual_campaign_flight_update(write_pool, campaign_id, beeswax_line_item_id, bx_flight_start_time, bx_flight_end_time, flight_start_time, flight_end_time, "Updated flight times", transaction_id, batch_id)
            logger.info(f"Successfully updated flight times for campaign | campaign_id={campaign_id} | beeswax_line_item_id={beeswax_line_item_id} | transaction_id={transaction_id} | batch_id={batch_id}")
    else:
        logger.info(f"Beeswax line item flight times match MNTN campaign flight times, no updated needed | campaign_id={campaign_id} | beeswax_line_item_id={beeswax_line_item_id} | flight_start_time={flight_start_time} | flight_end_time={flight_end_time} | bx_flight_start_time={bx_flight_start_time} | bx_flight_end_time={bx_flight_end_time} | transaction_id={transaction_id} | batch_id={batch_id}")




async def sync_flight_times(read_pool, write_pool, batch_id):

    logger.info(f"Starting syncing of flight times | batch_id={batch_id}")

    campaign_group_ids = [int(id) for id in os.environ.get("CAMPAIGN_GROUP_IDS").split(",") if id]
    logger.info(f"operating on campaign_group_ids={campaign_group_ids}")
     
    campaign_group_to_campaigns = {}

    
    campaign_groups = await get_campaign_group_mappings(read_pool, campaign_group_ids)
    
    campaigns = await get_campaign_mappings(read_pool, campaign_group_ids)

    for campaign_group in campaign_groups:
        if not campaign_group["beeswax_campaign_present"]:
            logger.error(f"Skipping campaign group because beeswax_campaign_id is not present | campaign_group={campaign_group} | batch_id={batch_id}")
        else:
            campaign_group_to_campaigns[campaign_group["campaign_group_id"]] = {"campaign_group": campaign_group, "campaigns": []}

    for campaign in campaigns:
        if not campaign["beeswax_line_item_present"]:
            logger.error(f"Skipping campaign because beeswax_line_item_id is not present | campaign={campaign} | batch_id={batch_id}")
        else:
            campaign_group_to_campaigns[campaign["campaign_group_id"]]["campaigns"].append(campaign)

    max_runner = os.environ.get("MAX_RUNNER", 10)
    sem = asyncio.Semaphore(max_runner)
    async with bx.get_session() as session:

        tasks = []

        # for value in islice(campaign_group_to_campaigns.values(), 20):
        for value in campaign_group_to_campaigns.values():

            transaction_id = uuid.uuid4()
            campaign_group = value["campaign_group"]
            campaigns = value["campaigns"]

            tasks.append(bounded_gather(sem, handle_campaign_group, session, read_pool, write_pool, campaign_group, campaigns, transaction_id, batch_id))
        
        await asyncio.gather(*tasks)

    logger.info(f"Successfully synced flight times for all campaigns and campaign groups | batch_id={batch_id}")

async def bounded_gather(sem, task_func, *args):
    await sem.acquire()  # Try to acquire the semaphore
    try:
        result = await task_func(*args)
        return result
    finally:
        sem.release()  # Release the semaphore so that another task can start

    

if __name__=='__main__':
    batch_id = uuid.uuid4()


    asyncio.run(sync_flight_times(batch_id))