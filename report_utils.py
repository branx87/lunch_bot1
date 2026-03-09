# ##report_utils.py
import os
import logging

logger = logging.getLogger(__name__)


def ensure_reports_dir(report_type: str = 'accounting') -> str:
    """Создает папку для отчетов если ее нет и возвращает путь к ней"""
    base_dir = os.path.dirname(os.path.abspath(__file__))

    if report_type == 'provider':
        reports_dir = os.path.join(base_dir, 'data', 'reports', 'provider_reports')
    elif report_type == 'admin':
        reports_dir = os.path.join(base_dir, 'data', 'reports', 'admin_reports')
    else:
        reports_dir = os.path.join(base_dir, 'data', 'reports', 'accounting_reports')

    os.makedirs(reports_dir, exist_ok=True)

    # Очищаем старые отчеты (оставляем 5 последних)
    report_files = sorted(
        [f for f in os.listdir(reports_dir) if f.endswith('.xlsx')],
        key=lambda x: os.path.getmtime(os.path.join(reports_dir, x)),
        reverse=True
    )
    for old_file in report_files[5:]:
        try:
            os.remove(os.path.join(reports_dir, old_file))
        except Exception as e:
            logger.error(f"Ошибка удаления старого отчета {old_file}: {e}")

    return reports_dir
