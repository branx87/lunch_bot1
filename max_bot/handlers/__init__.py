from max_bot.handlers.start import router as start_router
from max_bot.handlers.registration import router as reg_router
from max_bot.handlers.menu import router as menu_router
from max_bot.handlers.orders import router as orders_router
from max_bot.handlers.reports import router as reports_router
from max_bot.handlers.common import router as common_router


def setup_routers(dp):
    """Register all Max bot routers with the dispatcher."""
    dp.include_routers(
        start_router,
        reg_router,
        orders_router,
        menu_router,
        reports_router,
        common_router,
    )
