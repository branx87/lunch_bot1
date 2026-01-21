@echo off
echo Проверка базы данных...
python -c "import os; from pathlib import Path; p=Path('data/lunch_bot.db'); print(f'Размер: {p.stat().st_size} байт'); print(f'Изменен: {p.stat().st_mtime}')"
pause