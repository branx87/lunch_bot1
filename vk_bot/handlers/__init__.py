from vk_bot.handlers.start import start_labeler
from vk_bot.handlers.registration import reg_labeler
from vk_bot.handlers.menu import menu_labeler
from vk_bot.handlers.orders import orders_labeler
from vk_bot.handlers.reports import reports_labeler
from vk_bot.handlers.common import common_labeler


def setup_labelers(labeler):
    """Register all VK bot labelers."""
    labeler.load(start_labeler)
    labeler.load(reg_labeler)
    labeler.load(orders_labeler)
    labeler.load(menu_labeler)
    labeler.load(reports_labeler)
    labeler.load(common_labeler)
