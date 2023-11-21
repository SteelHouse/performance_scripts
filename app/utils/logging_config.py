import logging
from datetime import datetime, timezone

# Create a custom formatter
formatter = logging.Formatter('%(asctime)s UTC | %(levelname)s | %(filename)s:%(lineno)d:%(funcName)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Set up the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a console handler and set the level to debug
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
logging.Formatter.converter = lambda *args: datetime.utcnow().replace(tzinfo=timezone.utc).timetuple()