#!/usr/bin/env python
"""
Единый файл запуска для Flask приложения и Telegram бота
"""
import os
import sys
import threading
import asyncio
from pathlib import Path

# Добавляем путь к модулю dauricrm
BASE_DIR = Path(__file__).parent
dauricrm_path = BASE_DIR / "dauricrm"
sys.path.insert(0, str(dauricrm_path))

# Меняем рабочую директорию на dauricrm для правильной работы относительных путей
original_cwd = os.getcwd()
os.chdir(dauricrm_path)

try:
    # Импортируем Flask приложение
    import app as flask_app
    app = flask_app.app
    db = flask_app.db
    
    # Импортируем функцию запуска бота
    import telegram_bot
    bot_main = telegram_bot.main
finally:
    # Возвращаем оригинальную рабочую директорию
    os.chdir(original_cwd)


def run_flask():
    """Запуск Flask приложения"""
    print("=" * 50)
    print("Запуск Flask приложения...")
    print("=" * 50)
    # Убеждаемся, что мы в правильной директории
    os.chdir(dauricrm_path)
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    with app.app_context():
        db.create_all()
    
    # Определяем настройки для локального/серверного запуска
    is_local = sys.platform == 'win32' or os.getenv('FLASK_ENV') == 'development'
    if is_local:
        host = '127.0.0.1'
        debug = True
        print("[MAIN] Flask запускается в режиме разработки на http://127.0.0.1:5000")
    else:
        host = '0.0.0.0'
        debug = False
        print("[MAIN] Flask запускается в production режиме на http://0.0.0.0:5000")
    
    # Запускаем Flask в главном потоке - это позволяет делиться event loop с ботом
    app.run(debug=debug, host=host, port=5000, use_reloader=False, threaded=True)


def run_bot():
    """Запуск Telegram бота"""
    print("=" * 50)
    print("Запуск Telegram бота...")
    print("=" * 50)
    # Убеждаемся, что мы в правильной директории
    os.chdir(dauricrm_path)
    # Создаем новый event loop для бота в отдельном потоке
    # Для Linux нужно использовать SelectorEventLoop без signal handlers
    if sys.platform != 'win32':
        # Используем SelectorEventLoop, который не требует signal handlers
        loop = asyncio.SelectorEventLoop()
    else:
        loop = asyncio.new_event_loop()
    
    asyncio.set_event_loop(loop)
    
    # Сохраняем loop в глобальную переменную модуля telegram_bot
    import telegram_bot
    telegram_bot._bot_loop = loop
    print(f"[MAIN] Event loop установлен для бота: {loop}, закрыт: {loop.is_closed()}")
    
    # Отключаем signal handlers для этого потока
    import signal
    if sys.platform != 'win32':
        # В отдельном потоке signal handlers не работают, поэтому игнорируем ошибки
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, signal.SIG_DFL)
            except (ValueError, OSError):
                pass
    
    try:
        # Запускаем бота в этом event loop
        loop.run_until_complete(bot_main())
    except Exception as e:
        print(f"\nОшибка при запуске бота: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            # Закрываем все pending tasks
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except:
            pass
        loop.close()


if __name__ == '__main__':
    print("=" * 50)
    print("Запуск системы управления продажами")
    print("Flask приложение + Telegram бот")
    print("=" * 50)
    print()
    
    # Определяем, нужно ли запускать бота (на сервере - да, локально - опционально)
    is_local = sys.platform == 'win32' or os.getenv('FLASK_ENV') == 'development'
    run_bot_flag = os.getenv('RUN_BOT', 'true').lower() == 'true'
    
    if run_bot_flag:
        # Запускаем бота в отдельном потоке
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()
        
        # Небольшая задержка для инициализации бота и event loop
        import time
        time.sleep(2)
        print("[MAIN] Бот запущен, проверяем event loop...")
        
        # Проверяем, что event loop установлен
        import telegram_bot
        if telegram_bot._bot_loop:
            print(f"[MAIN] ✅ Event loop бота доступен: {telegram_bot._bot_loop}")
            print(f"[MAIN] Event loop закрыт: {telegram_bot._bot_loop.is_closed()}")
        else:
            print("[MAIN] ⚠️ Event loop бота еще не установлен")
    else:
        print("[MAIN] Бот отключен (установите RUN_BOT=true для запуска)")
    
    # Запускаем Flask в главном потоке
    try:
        run_flask()
    except KeyboardInterrupt:
        print("\n" + "=" * 50)
        print("Остановка приложения...")
        print("=" * 50)
        sys.exit(0)
