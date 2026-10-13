# ##handlers/commands.py
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from admin import handle_sync_bitrix
from database import db
from models import User, BitrixMapping
from config import CONFIG
from datetime import datetime
from time_config import TIME_CONFIG
from backup_manager import backup_manager

async def notifications_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Включение уведомлений"""
    user = db.session.query(User).filter(
        User.telegram_id == update.effective_user.id
    ).first()
    
    if user:
        user.notifications_enabled = True
        db.session.commit()
        await update.message.reply_text("🔔 Уведомления включены!")
    else:
        await update.message.reply_text("❌ Пользователь не найден")

async def notifications_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отключение уведомлений"""
    user = db.session.query(User).filter(
        User.telegram_id == update.effective_user.id
    ).first()
    
    if user:
        user.notifications_enabled = False
        db.session.commit()
        await update.message.reply_text("🔕 Уведомления отключены!")
    else:
        await update.message.reply_text("❌ Пользователь не найден")

async def check_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка синхронизированных пользователей с Bitrix24"""
    # Используем SQLAlchemy для получения синхронизированных пользователей
    synced_users = db.session.query(User, BitrixMapping).join(
        BitrixMapping, 
        User.id == BitrixMapping.local_id
    ).filter(
        BitrixMapping.local_type == 'user'
    ).all()
    
    if not synced_users:
        await update.message.reply_text("Нет синхронизированных пользователей.")
        return
    
    msg = "Сопоставленные сотрудники:\n" + "\n".join(
        f"{user.id}: {user.full_name} → Bitrix ID {mapping.bitrix_id}" 
        for user, mapping in synced_users
    )
    await update.message.reply_text(msg)

async def manual_sync_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для ручной отправки заказов"""
    user_id = update.effective_user.id
    
    # 🔥 ПРОВЕРКА АДАПТИРОВАНА ПОД ВАШ CONFIG
    if not CONFIG.master_admin_id or user_id != CONFIG.master_admin_id:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    from config import logger
    from bitrix.sync import BitrixSync
    
    logger.info(f"Главный админ {user_id} запустил ручную отправку заказов")
    
    try:
        async with BitrixSync() as sync:
            
            # Проверяем наличие неотправленных заказов
            pending_info = await sync.get_pending_orders_info()
            
            if pending_info['count'] == 0:
                await update.message.reply_text(
                    "✅ Нет заказов для отправки\n\n"
                    f"Дата: {pending_info['date']}"
                )
                return
            
            # Уведомляем о начале отправки
            status_msg = await update.message.reply_text(
                f"🔄 Начинаю отправку {pending_info['count']} заказов...\n"
                "⏳ Подождите..."
            )
            
            # Запускаем отправку
            success = await sync._push_to_bitrix()
            
            # Получаем результат
            new_pending = await sync.get_pending_orders_info()
            sent_count = pending_info['count'] - new_pending['count']
            
            if success:
                result_text = (
                    f"✅ УСПЕШНО ОТПРАВЛЕНО!\n\n"
                    f"📤 Заказов отправлено: {sent_count}\n"
                    f"📅 Дата: {pending_info['date']}\n"
                    f"⏰ Время: {datetime.now(TIME_CONFIG.TIMEZONE).strftime('%H:%M:%S')}"
                )
            else:
                result_text = (
                    f"⚠️ ЧАСТИЧНО ВЫПОЛНЕНО\n\n"
                    f"✅ Отправлено: {sent_count}\n"
                    f"❌ Не отправлено: {new_pending['count']}\n"
                    f"📅 Дата: {pending_info['date']}\n\n"
                    f"🔍 Проверьте логи для деталей."
                )
            
            await status_msg.edit_text(result_text)
        
    except Exception as e:
        logger.error(f"Ошибка ручной отправки заказов: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при отправке:\n\n{str(e)}"
        )

async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для ручного создания резервной копии базы данных"""
    user_id = update.effective_user.id

    # Проверяем права администратора
    if user_id not in CONFIG.admin_ids:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return

    from config import logger

    logger.info(f"Администратор {user_id} запустил создание резервной копии БД")

    try:
        # Проверяем наличие аргументов
        upload_to_cloud = True
        if context.args and context.args[0] == '--local':
            upload_to_cloud = False

        # Уведомляем о начале создания бекапа
        status_msg = await update.message.reply_text(
            "🔄 Начинаю создание резервной копии базы данных...\n"
            "⏳ Это может занять несколько минут..."
        )

        # Создаем бекап
        backup_path = await backup_manager.create_backup(upload_to_cloud=upload_to_cloud)

        if backup_path:
            # Получаем информацию о бекапе
            backup_size = backup_path.stat().st_size / 1024 / 1024  # МБ

            result_text = (
                "✅ РЕЗЕРВНАЯ КОПИЯ СОЗДАНА!\n\n"
                f"📦 Файл: {backup_path.name}\n"
                f"📊 Размер: {backup_size:.2f} МБ\n"
                f"📁 Папка: {backup_path.parent}\n"
                f"⏰ Время: {datetime.now(TIME_CONFIG.TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}\n"
            )

            if upload_to_cloud:
                if backup_manager.yadisk_client:
                    result_text += "\n☁️ Бекап загружен на Яндекс.Диск"
                else:
                    result_text += "\n⚠️ Яндекс.Диск не настроен (только локальная копия)"
            else:
                result_text += "\n💾 Создана только локальная копия"

            # Добавляем статистику бекапов
            status = backup_manager.get_backup_status()
            result_text += f"\n\n📋 Всего локальных бекапов: {len(status['local_backups'])}"
            if status['yandex_disk_configured']:
                result_text += f"\n☁️ Бекапов на Яндекс.Диске: {len(status['cloud_backups'])}"

            await status_msg.edit_text(result_text)

        else:
            await status_msg.edit_text(
                "❌ Не удалось создать резервную копию\n\n"
                "Проверьте логи для деталей."
            )

    except Exception as e:
        logger.error(f"Ошибка при создании бекапа: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при создании резервной копии:\n\n{str(e)}"
        )

async def restore_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для просмотра списка доступных бекапов для восстановления"""
    user_id = update.effective_user.id

    # Только главный админ может восстанавливать БД
    if not CONFIG.master_admin_id or user_id != CONFIG.master_admin_id:
        await update.message.reply_text("❌ Только главный администратор может восстанавливать базу данных")
        return

    try:
        backups = backup_manager.list_available_backups()

        msg = "📦 ДОСТУПНЫЕ БЕКАПЫ ДЛЯ ВОССТАНОВЛЕНИЯ\n"
        msg += "=" * 35 + "\n\n"

        # Локальные бекапы
        msg += f"💾 Локальные ({len(backups['local'])}):\n"
        if backups['local']:
            for i, backup in enumerate(backups['local'][:7], 1):  # Последние 7
                msg += f"  {i}. {backup['name']}\n"
                msg += f"     {backup['size_mb']:.2f} МБ | {backup['created']}\n"
        else:
            msg += "  Нет локальных бекапов\n"

        msg += "\n"

        # Бекапы на Яндекс.Диске
        if backups['cloud']:
            msg += f"☁️ Яндекс.Диск ({len(backups['cloud'])}):\n"
            for backup in backups['cloud']:
                msg += f"  • {backup['name']}\n"
                msg += f"    {backup['size_mb']:.2f} МБ | {backup['created']}\n"
        elif backup_manager.yadisk_client:
            msg += "☁️ Яндекс.Диск: нет бекапов\n"
        else:
            msg += "☁️ Яндекс.Диск: не настроен\n"

        msg += "\n⚠️ ВНИМАНИЕ: Восстановление перезапишет ВСЕ данные!\n\n"
        msg += "💡 Команды:\n"
        msg += "  /restore <номер> - восстановить из локального\n"
        msg += "  /restore cloud:<имя> - восстановить из облака\n"
        msg += "  /restore - показать этот список\n"

        await update.message.reply_text(msg)

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def restore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для восстановления базы данных из бекапа"""
    user_id = update.effective_user.id

    # Только главный админ может восстанавливать БД
    if not CONFIG.master_admin_id or user_id != CONFIG.master_admin_id:
        await update.message.reply_text("❌ Только главный администратор может восстанавливать базу данных")
        return

    from config import logger
    from pathlib import Path

    # Если нет аргументов - показываем список
    if not context.args:
        await restore_list_command(update, context)
        return

    arg = context.args[0]

    # Проверяем подтверждение
    confirm = len(context.args) > 1 and context.args[1] == '--confirm'

    try:
        backups = backup_manager.list_available_backups()
        backup_path = None

        # Восстановление из облака
        if arg.startswith('cloud:'):
            cloud_name = arg[6:]  # убираем 'cloud:'

            # Ищем файл в облаке
            found = next((b for b in backups['cloud'] if b['name'] == cloud_name), None)
            if not found:
                await update.message.reply_text(
                    f"❌ Бекап '{cloud_name}' не найден на Яндекс.Диске\n\n"
                    "Используйте /restore для просмотра списка"
                )
                return

            if not confirm:
                await update.message.reply_text(
                    f"⚠️ ПОДТВЕРЖДЕНИЕ ВОССТАНОВЛЕНИЯ\n\n"
                    f"📦 Файл: {cloud_name}\n"
                    f"📊 Размер: {found['size_mb']:.2f} МБ\n"
                    f"📅 Создан: {found['created']}\n\n"
                    f"⚠️ ВСЕ ТЕКУЩИЕ ДАННЫЕ БУДУТ УДАЛЕНЫ!\n\n"
                    f"Для подтверждения отправьте:\n"
                    f"/restore cloud:{cloud_name} --confirm"
                )
                return

            # Скачиваем с облака
            status_msg = await update.message.reply_text(
                f"☁️ Скачивание бекапа с Яндекс.Диска...\n"
                f"📦 {cloud_name}"
            )

            backup_path = await backup_manager.download_from_cloud(cloud_name)
            if not backup_path:
                await status_msg.edit_text("❌ Не удалось скачать бекап с Яндекс.Диска")
                return

            await status_msg.edit_text(
                f"✅ Бекап скачан\n"
                f"🔄 Начинаю восстановление..."
            )

        # Восстановление из локального по номеру
        else:
            try:
                index = int(arg) - 1
                if index < 0 or index >= len(backups['local']):
                    await update.message.reply_text(
                        f"❌ Неверный номер бекапа. Доступно: 1-{len(backups['local'])}\n\n"
                        "Используйте /restore для просмотра списка"
                    )
                    return

                selected = backups['local'][index]
                backup_path = Path(selected['path'])

                if not confirm:
                    await update.message.reply_text(
                        f"⚠️ ПОДТВЕРЖДЕНИЕ ВОССТАНОВЛЕНИЯ\n\n"
                        f"📦 Файл: {selected['name']}\n"
                        f"📊 Размер: {selected['size_mb']:.2f} МБ\n"
                        f"📅 Создан: {selected['created']}\n\n"
                        f"⚠️ ВСЕ ТЕКУЩИЕ ДАННЫЕ БУДУТ УДАЛЕНЫ!\n\n"
                        f"Для подтверждения отправьте:\n"
                        f"/restore {arg} --confirm"
                    )
                    return

            except ValueError:
                await update.message.reply_text(
                    "❌ Укажите номер бекапа или cloud:<имя>\n\n"
                    "Примеры:\n"
                    "  /restore 1 - восстановить из локального #1\n"
                    "  /restore cloud:backup_monday.sql.gz - из облака\n\n"
                    "Используйте /restore для просмотра списка"
                )
                return

        # Выполняем восстановление
        logger.info(f"🔄 Главный админ {user_id} запустил восстановление из {backup_path}")

        status_msg = await update.message.reply_text(
            "🔄 ВОССТАНОВЛЕНИЕ БАЗЫ ДАННЫХ\n\n"
            f"📦 Файл: {backup_path.name}\n"
            "⏳ Это может занять несколько минут...\n\n"
            "⚠️ НЕ ВЫКЛЮЧАЙТЕ БОТА!"
        )

        result = await backup_manager.restore_backup(backup_path, confirm=True)

        if result['success']:
            await status_msg.edit_text(
                f"✅ ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО!\n\n"
                f"📦 Файл: {result['details']['backup_file']}\n"
                f"⏰ Время: {result['details']['restored_at']}\n\n"
                f"ℹ️ Рекомендуется перезапустить бота"
            )
        else:
            await status_msg.edit_text(
                f"❌ ОШИБКА ВОССТАНОВЛЕНИЯ\n\n"
                f"{result['message']}"
            )

    except Exception as e:
        logger.error(f"Ошибка восстановления: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Критическая ошибка: {str(e)}")


async def backup_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для просмотра статуса бекапов"""
    user_id = update.effective_user.id

    # Проверяем права администратора
    if user_id not in CONFIG.admin_ids:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return

    try:
        status = backup_manager.get_backup_status()

        msg = "📊 СТАТУС РЕЗЕРВНЫХ КОПИЙ\n"
        msg += "=" * 30 + "\n\n"

        # Информация о Яндекс.Диске
        if status['yandex_disk_configured']:
            msg += "☁️ Яндекс.Диск: подключен\n\n"
        else:
            msg += "☁️ Яндекс.Диск: не настроен\n"
            msg += "ℹ️ Укажите YANDEX_DISK_TOKEN в .env\n\n"

        # Локальные бекапы
        msg += f"💾 Локальные бекапы ({len(status['local_backups'])}):\n"
        if status['local_backups']:
            for backup in status['local_backups'][:5]:  # Показываем последние 5
                msg += f"  • {backup['name']}\n"
                msg += f"    {backup['size_mb']:.2f} МБ, {backup['created']}\n"
        else:
            msg += "  Нет локальных бекапов\n"

        msg += "\n"

        # Бекапы на Яндекс.Диске
        if status['yandex_disk_configured']:
            msg += f"☁️ Бекапы на Яндекс.Диске ({len(status['cloud_backups'])}):\n"
            if status['cloud_backups']:
                for backup in status['cloud_backups']:
                    msg += f"  • {backup['name']}\n"
                    msg += f"    {backup['size_mb']:.2f} МБ, {backup['created']}\n"
            else:
                msg += "  Нет бекапов на облаке\n"

        msg += "\n💡 Команды:\n"
        msg += "  /backup - создать резервную копию\n"
        msg += "  /backup --local - только локальная копия\n"

        await update.message.reply_text(msg)

    except Exception as e:
        await update.message.reply_text(
            f"❌ Ошибка при получении статуса:\n\n{str(e)}"
        )

def setup(application):
    application.add_handler(CommandHandler("notifications_on", notifications_on))
    application.add_handler(CommandHandler("notifications_off", notifications_off))
    application.add_handler(CommandHandler('sync_bitrix', handle_sync_bitrix))
    application.add_handler(CommandHandler("checksync", check_sync))
    application.add_handler(CommandHandler("manual_sync", manual_sync_command))
    application.add_handler(CommandHandler("backup", backup_command))
    application.add_handler(CommandHandler("backup_status", backup_status_command))
    application.add_handler(CommandHandler("restore", restore_command))

# Для обратной совместимости
def setup_commands(app):
    setup(app)