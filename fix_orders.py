from database import db
from models import Menu, BotSetting
from database import db
from sqlalchemy import text

with db.get_session() as session:
    # Исправить последовательность для menu
    session.execute(text("SELECT setval('menu_id_seq', (SELECT MAX(id) FROM menu))"))
    session.commit()
    print("✅ Последовательность ID исправлена")

# Включить заказы
with db.get_session() as session:
    # Проверить текущее состояние
    setting = session.query(BotSetting).filter_by(setting_name='orders_enabled').first()
    print(f"Текущее состояние заказов: {setting.setting_value if setting else 'Не найдено'}")
    
    # Включить заказы
    if setting:
        setting.setting_value = 'true'
    else:
        setting = BotSetting(setting_name='orders_enabled', setting_value='true')
        session.add(setting)
    
    session.commit()
    print("Заказы включены!")

with db.get_session() as session:
    # Найти существующее меню на субботу
    saturday_menu = session.query(Menu).filter(Menu.day == 'Суббота').first()
    
    if saturday_menu:
        # Обновить существующее меню
        saturday_menu.first_course = 'Борщ'
        saturday_menu.main_course = 'Курица с рисом'
        saturday_menu.salad = 'Овощной салат'
        print("✅ Меню на субботу ОБНОВЛЕНО!")
    else:
        # Создать новое меню
        menu = Menu(
            day='Суббота',
            first_course='Борщ',
            main_course='Курица с рисом', 
            salad='Овощной салат'
        )
        session.add(menu)
        print("✅ Меню на субботу СОЗДАНО!")
    
    session.commit()