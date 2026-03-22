"""FSM states for VK bot multi-step flows."""
from vkbottle import BaseStateGroup


class Registration(BaseStateGroup):
    PHONE = "phone"
    FULL_NAME = "full_name"
    LOCATION = "location"
