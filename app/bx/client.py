import aiohttp
import asyncio
import os
from contextlib import asynccontextmanager
from ..utils.logging_config import logger

async def get_cookies():
    
    EMAIL = os.environ.get("BX_EMAIL")
    PASSWORD = os.environ.get("BX_PASSWORD")

    payload = {"email":EMAIL, "password":PASSWORD, "keep_logged_in":True}

    async with aiohttp.ClientSession() as session:
        async with session.post('https://steelhouse.api.beeswax.com/rest/authenticate', json=payload) as resp:
            cookies = resp.cookies
            return cookies

@asynccontextmanager
async def get_session():
    cookies = await get_cookies()
    async with aiohttp.ClientSession(cookies=cookies) as session:
        try:
            yield session
        finally:
            await session.close()


async def get_advertiser(session, advertiser_id, beeswax_advertiser_id):
    async with session.get(f'https://steelhouse.api.beeswax.com/rest/v2/advertisers/{beeswax_advertiser_id}') as resp:
        if resp.status == 200:
            beeswax_advertiser = await resp.json()
            return beeswax_advertiser
        else:
            logger.error(f"Failed to get beeswax advertiser | beeswax_advertiser_id={beeswax_advertiser_id}")
            return None

async def update_advertiser(session, advertiser_id, beeswax_advertiser_id, payload):
    async with session.post(f'https://steelhouse.api.beeswax.com/rest/v2/advertisers/{beeswax_advertiser_id}', json=payload) as resp:
        if resp.status == 200:
            beeswax_advertiser = await resp.json()
            return beeswax_advertiser
        else:
            logger.error(f"Failed to update beeswax advertiser | beeswax_advertiser_id={beeswax_advertiser_id}")
            return None

async def get_beeswax_delivery_modifier(session, campaign_id, beeswax_line_item_id, beeswax_delivery_modifier_id):
    async with session.get(f'https://steelhouse.api.beeswax.com/rest/v2/delivery-modifiers/{beeswax_delivery_modifier_id}') as resp:
        if resp.status == 200:
            beeswax_delivery_modifier = await resp.json()
            return beeswax_delivery_modifier
        else:
            logger.error(f"Failed to get beeswax delivery_modifier | beeswax_delivery_modifier_id={beeswax_delivery_modifier_id}")
            return None

async def update_beeswax_delivery_modifier(session, campaign_id, beeswax_line_item_id, beeswax_delivery_modifier_id, payload):
    async with session.post(f'https://steelhouse.api.beeswax.com/rest/v2/delivery-modifiers/{beeswax_delivery_modifier_id}', json=payload) as resp:
        if resp.status == 200:
            beeswax_delivery_modifier = await resp.json()
            return beeswax_delivery_modifier
        else:
            logger.error(f"Failed to update beeswax delivery_modifier | beeswax_delivery_modifier_id={beeswax_delivery_modifier_id}")
            return None


async def get_targeting_expression(session, campaign_id, beeswax_line_item_id, beeswax_targeting_expression_id):
    async with session.get(f'https://steelhouse.api.beeswax.com/rest/v2/targeting-expressions/{beeswax_targeting_expression_id}') as resp:
        if resp.status == 200:
            beeswax_targeting_expression = await resp.json()
            return beeswax_targeting_expression
        else:
            logger.error(f"Failed to get beeswax targeting_expression | beeswax_targeting_expression_id={beeswax_targeting_expression_id}")
            return None

async def update_targeting_expression(session, campaign_id, beeswax_line_item_id, beeswax_targeting_expression_id, payload):
    async with session.post(f'https://steelhouse.api.beeswax.com/rest/v2/targeting-expressions/{beeswax_targeting_expression_id}', json=payload) as resp:
        if resp.status == 200:
            beeswax_targeting_expression = await resp.json()
            return beeswax_targeting_expression
        else:
            logger.error(f"Failed to update beeswax targeting_expression | beeswax_targeting_expression_id={beeswax_targeting_expression_id}")
            return None



async def get_beeswax_campaign(session, campaign_group_id, beeswax_campaign_id):
    async with session.get(f'https://steelhouse.api.beeswax.com/rest/campaign/{beeswax_campaign_id}') as resp:
        if resp.status == 200:
            beeswax_campaign = await resp.json()
            return beeswax_campaign["payload"][0]
        else:
            logger.error(f"Failed to get beeswax campaign | beeswax_campaign_id={beeswax_campaign_id}")
            return None

async def update_beeswax_campaign(session, campaign_group_id, beeswax_campaign_id, payload):
    async with session.put(f'https://steelhouse.api.beeswax.com/rest/campaign/{beeswax_campaign_id}', json=payload) as resp:
        if resp.status == 200:
            beeswax_campaign = await resp.json()
            return beeswax_campaign
        else:
            logger.error(f"Failed to get beeswax campaign | beeswax_campaign_id={beeswax_campaign_id}")
            return None



async def get_beeswax_line_item(session, campaign_id, beeswax_line_item_id):
    async with session.get(f'https://steelhouse.api.beeswax.com/rest/v2/line-items/{beeswax_line_item_id}') as resp:
        if resp.status == 200:
            beeswax_line_item = await resp.json()
            return beeswax_line_item
        else:
            logger.error(f"Failed to get beeswax line_item | beeswax_line_item_id={beeswax_line_item_id}")
            return None

async def update_beeswax_line_item(session, campaign_id, beeswax_line_item_id, payload):
    async with session.patch(f'https://steelhouse.api.beeswax.com/rest/v2/line-items/{beeswax_line_item_id}', json=payload) as resp:
        if resp.status == 200:
            beeswax_line_item = await resp.json()
            return beeswax_line_item
        else:
            logger.error(f"Failed to update beeswax line_item | beeswax_line_item_id={beeswax_line_item_id}")
            return None
