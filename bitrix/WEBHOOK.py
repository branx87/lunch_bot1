# ##WEBHOOK.py
import requests
import json

WEBHOOK_URL = ""

# 1. ЗАПРОС СПИСКА ВСЕХ ПОДРАЗДЕЛЕНИЙ
print("Запрашиваю подразделения...")
dep_response = requests.get(WEBHOOK_URL + 'department.get')
dep_data = dep_response.json()

dept_dict = {}
dept_parent_dict = {}  # Будем хранить родительские ID

if 'result' in dep_data:
    for dept in dep_data['result']:
        dept_id_key = str(dept['ID'])
        dept_dict[dept_id_key] = dept['NAME']
        # Сохраняем родителя для каждого отдела (если он есть)
        dept_parent_dict[dept_id_key] = str(dept.get('PARENT', '')) 
    print(f"Получено {len(dept_dict)} подразделений")
else:
    print("Ошибка при запросе отделов:", dep_data)

# Функция для построения полного пути отдела
def get_full_department_name(dept_id, dept_dict, dept_parent_dict):
    """Рекурсивно строит полное название отдела с учетом иерархии"""
    if not dept_id or dept_id not in dept_dict:
        return 'Не указано'
    
    name_parts = [dept_dict[dept_id]]
    parent_id = dept_parent_dict.get(dept_id)
    
    # Поднимаемся вверх по иерархии, пока есть родители
    while parent_id and parent_id in dept_dict:
        name_parts.append(dept_dict[parent_id])
        parent_id = dept_parent_dict.get(parent_id)
    
    # Разворачиваем массив, чтобы начать с верхнего уровня
    return ' -> '.join(reversed(name_parts))

# 2. ЗАПРОС СПИСКА СОТРУДНИКОВ
print("Запрашиваю сотрудников...")
params = {'FILTER[USER_TYPE]': 'employee'}
user_response = requests.get(WEBHOOK_URL + 'user.get', params=params)
user_data = user_response.json()

result_list = []
if 'result' in user_data:
    users = user_data['result']
    print(f"Получено {len(users)} сотрудников")

    for user in users:
        dept_id_list = user.get('UF_DEPARTMENT', [])
        # Берем первый отдел из списка (основной отдел сотрудника)
        dept_id = str(dept_id_list[0]) if dept_id_list else None

        # Используем новую функцию для получения полного пути
        dept_name_full = get_full_department_name(dept_id, dept_dict, dept_parent_dict)
        
        # Также получаем просто название для обратной совместимости
        dept_name_simple = dept_dict.get(dept_id, 'Не указано')

        employee_info = {
            'ID': user['ID'],
            'ФИО': f"{user.get('LAST_NAME', '')} {user.get('NAME', '')}".strip(),
            'Должность': user.get('WORK_POSITION', 'Не указана'),
            'Подразделение': dept_name_simple,
            'Подразделение_полное': dept_name_full,  # НОВОЕ ПОЛЕ!
            'ID_Подразделения': dept_id_list,
            'Активен': user.get('ACTIVE', False)
        }
        result_list.append(employee_info)

    print("\nИтоговый список сотрудников:")
    print(json.dumps(result_list, ensure_ascii=False, indent=2))

else:
    print("Ошибка при запросе пользователей:", user_data)