import aiohttp_jinja2
import logging

log = logging.getLogger(__name__)


@aiohttp_jinja2.template('index.jinja2')
async def index(request):
    return {}
