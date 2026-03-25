import asyncio
import os
import sys
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.filters import Command
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, BigInteger, Boolean
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base, relationship
from sqlalchemy.sql import func
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

_msk_tz = None
try:
    from zoneinfo import ZoneInfo
    try:
        _msk_tz = ZoneInfo("Europe/Moscow")
    except Exception:
        pass  # на Windows может не быть базы часовых поясов — используем UTC+3
except ImportError:
    pass

def _today_msk():
    """Текущая дата по Москве (для проверки дней рождения и напоминаний)."""
    if _msk_tz is not None:
        return datetime.now(_msk_tz).date()
    return (datetime.utcnow() + timedelta(hours=3)).date()

# Токен бота



# Настройка базы данных (та же, что и в app.py — sales.db в папке приложения)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URI = f'sqlite:///{os.path.join(BASE_DIR, "sales.db")}'

# URL веб-приложения для ссылки "Открыть приложение" в заявках (без слэша в конце)
APP_BASE_URL = os.environ.get('APP_BASE_URL', 'https://www.dauri-adm.ru').rstrip('/')
PENDING_REQUESTS_URL = f"{APP_BASE_URL}/pending_requests"
engine = create_engine(DATABASE_URI, connect_args={"check_same_thread": False})
Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()

# Определяем модели напрямую (копируем из app.py)
class City(Base):
    __tablename__ = 'city'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)

class Employee(Base):
    __tablename__ = 'employee'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)

class Investor(Base):
    __tablename__ = 'investor'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)


class Client(Base):
    __tablename__ = 'client'
    id = Column(Integer, primary_key=True)
    full_name = Column(String(200), nullable=False)
    phone = Column(String(50), nullable=True)
    instagram = Column(String(100), nullable=True)
    telegram = Column(String(100), nullable=True)
    email = Column(String(120), nullable=True)
    birth_date = Column(DateTime, nullable=True)  # Дата рождения (в SQLite хранится как date)


class ExpenseType(Base):
    __tablename__ = 'expense_type'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)

class Sale(Base):
    __tablename__ = 'sale'
    id = Column(Integer, primary_key=True)
    photo = Column(String(200))
    product_name = Column(String(200), nullable=False)
    reference = Column(String(200))
    item_year = Column(Integer, nullable=True)
    buy_price = Column(Float, nullable=False)
    sell_price = Column(Float, nullable=False)
    bonus = Column(Float, default=0.0)  # Бонус менеджера за продажу (итого)
    initial_bonus = Column(Float, default=0.0)  # Бонус, указанный менеджером при создании заявки
    komplektatsiya = Column('komplektatsiya', String(50), nullable=True)  # Комплектация: Полный комплект, только часы и т.д.
    komissionnyy = Column(Boolean, default=False, nullable=False)  # Комиссионный товар
    city_id = Column(Integer, ForeignKey('city.id'), nullable=False)
    employee_id = Column(Integer, ForeignKey('employee.id'), nullable=False)
    investor_id = Column(Integer, ForeignKey('investor.id'), nullable=True)
    client_id = Column(Integer, ForeignKey('client.id'), nullable=True)
    date = Column(DateTime, default=datetime.utcnow)
    month_reminder_sent_at = Column(DateTime, nullable=True)  # Когда отправлено напоминание "месяц после продажи"
    
    city = relationship('City')
    employee = relationship('Employee')
    investor = relationship('Investor')
    client = relationship('Client')


class SaleAdditionalBonus(Base):
    __tablename__ = 'sale_additional_bonus'
    id = Column(Integer, primary_key=True)
    sale_id = Column(Integer, ForeignKey('sale.id'), nullable=False)
    employee_id = Column(Integer, ForeignKey('employee.id'), nullable=False)
    amount = Column(Float, nullable=False)
    sale = relationship('Sale')
    employee = relationship('Employee')


class ManualBonus(Base):
    __tablename__ = 'manual_bonus'
    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey('employee.id'), nullable=False)
    amount = Column(Float, nullable=False)
    date = Column(DateTime, nullable=False)
    comment = Column(String(300), nullable=True)
    created_at = Column(DateTime)
    employee = relationship('Employee')


class Expense(Base):
    __tablename__ = 'expense'
    id = Column(Integer, primary_key=True)
    sale_id = Column(Integer, ForeignKey('sale.id'), nullable=False)
    expense_type_id = Column(Integer, ForeignKey('expense_type.id'), nullable=False)
    amount = Column(Float, nullable=False)
    comment = Column(String(200))
    
    expense_type = relationship('ExpenseType')

class StockItem(Base):
    __tablename__ = 'stock_item'
    id = Column(Integer, primary_key=True)
    city_id = Column(Integer, ForeignKey('city.id'), nullable=False)
    investor_id = Column(Integer, ForeignKey('investor.id'), nullable=True)
    product_name = Column(String(200), nullable=False)
    reference = Column(String(200))
    item_year = Column(Integer, nullable=True)
    buy_price = Column(Float, nullable=False)
    expected_sell_price = Column(Float, nullable=False)
    quantity = Column(Integer, default=1)
    photo = Column(String(200))
    komplektatsiya = Column(String(50), nullable=True)
    komissionnyy = Column(Boolean, default=False, nullable=False)
    date_added = Column(DateTime, default=datetime.utcnow)
    sold = Column(Boolean, default=False, nullable=False)
    
    city = relationship('City')
    investor = relationship('Investor')

class StockExpense(Base):
    __tablename__ = 'stock_expense'
    id = Column(Integer, primary_key=True)
    stock_item_id = Column(Integer, ForeignKey('stock_item.id'), nullable=False)
    expense_type_id = Column(Integer, ForeignKey('expense_type.id'), nullable=False)
    amount = Column(Float, nullable=False)
    comment = Column(String(200))
    
    expense_type = relationship('ExpenseType')

class PendingSale(Base):
    __tablename__ = 'pending_sale'
    id = Column(Integer, primary_key=True)
    photo = Column(String(200))
    product_name = Column(String(200), nullable=False)
    reference = Column(String(200))
    item_year = Column(Integer, nullable=True)
    buy_price = Column(Float, nullable=False)  # 0 = не указано (менеджер не ввёл), админ допишет на сайте/в боте
    sell_price = Column(Float, nullable=False)
    bonus = Column(Float, default=0.0)  # Бонус менеджера за продажу (итого)
    initial_bonus = Column(Float, default=0.0)  # Бонус, указанный менеджером при создании заявки
    komplektatsiya = Column('komplektatsiya', String(50), nullable=True)
    komissionnyy = Column(Boolean, default=False, nullable=False)  # Комиссионный товар
    city_id = Column(Integer, ForeignKey('city.id'), nullable=False)
    employee_id = Column(Integer, ForeignKey('employee.id'), nullable=False)
    investor_id = Column(Integer, ForeignKey('investor.id'), nullable=True)
    date = Column(DateTime, default=datetime.utcnow)
    stock_id = Column(Integer, ForeignKey('stock_item.id'), nullable=True)
    telegram_message_id = Column(Integer, nullable=True)
    telegram_chat_id = Column(BigInteger, nullable=True)
    status = Column(String(20), default='pending', nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    city = relationship('City')
    employee = relationship('Employee')
    investor = relationship('Investor')

class PendingSaleAdditionalBonus(Base):
    __tablename__ = 'pending_sale_additional_bonus'
    id = Column(Integer, primary_key=True)
    pending_sale_id = Column(Integer, ForeignKey('pending_sale.id'), nullable=False)
    employee_id = Column(Integer, ForeignKey('employee.id'), nullable=False)
    amount = Column(Float, nullable=False)
    pending_sale = relationship('PendingSale')
    employee = relationship('Employee')


class PendingSaleExpense(Base):
    __tablename__ = 'pending_sale_expense'
    id = Column(Integer, primary_key=True)
    pending_sale_id = Column(Integer, ForeignKey('pending_sale.id'), nullable=False)
    expense_type_id = Column(Integer, ForeignKey('expense_type.id'), nullable=False)
    amount = Column(Float, nullable=False)
    comment = Column(String(200))
    
    expense_type = relationship('ExpenseType')

class PendingStock(Base):
    __tablename__ = 'pending_stock'
    id = Column(Integer, primary_key=True)
    photo = Column(String(200))
    product_name = Column(String(200), nullable=False)
    reference = Column(String(200))
    item_year = Column(Integer, nullable=True)
    buy_price = Column(Float, nullable=True)  # Менеджер не указывает — админ дописывает в Telegram
    expected_sell_price = Column(Float, nullable=False)
    quantity = Column(Integer, default=1)
    komplektatsiya = Column('komplektatsiya', String(50), nullable=True)
    komissionnyy = Column(Boolean, default=False, nullable=False)  # Комиссионный товар
    city_id = Column(Integer, ForeignKey('city.id'), nullable=False)
    investor_id = Column(Integer, ForeignKey('investor.id'), nullable=True)  # Для общего стока
    # Поля для комиссионного стока (имя и телефон владельца)
    client_full_name = Column(String(200), nullable=True)  # Имя владельца для комиссионного стока
    client_phone = Column(String(50), nullable=True)  # Телефон владельца для комиссионного стока
    telegram_message_id = Column(Integer, nullable=True)
    telegram_chat_id = Column(BigInteger, nullable=True)
    status = Column(String(20), default='pending', nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    city = relationship('City')
    investor = relationship('Investor')

class PendingStockExpense(Base):
    __tablename__ = 'pending_stock_expense'
    id = Column(Integer, primary_key=True)
    pending_stock_id = Column(Integer, ForeignKey('pending_stock.id'), nullable=False)
    expense_type_id = Column(Integer, ForeignKey('expense_type.id'), nullable=False)
    amount = Column(Float, nullable=False)
    comment = Column(String(200))
    
    expense_type = relationship('ExpenseType')

class SaleApproval(Base):
    __tablename__ = 'sale_approval'
    id = Column(Integer, primary_key=True)
    sale_id = Column(Integer, ForeignKey('sale.id'), nullable=False)
    telegram_message_id = Column(Integer, nullable=True)
    telegram_chat_id = Column(BigInteger, nullable=True)
    status = Column(String(20), default='pending', nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String(80), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    name = Column(String(100), nullable=False)
    is_admin = Column(Boolean, default=False, nullable=False)
    
    def check_password(self, password):
        from werkzeug.security import check_password_hash
        return check_password_hash(self.password_hash, password)

class BotAdmin(Base):
    __tablename__ = 'bot_admin'
    id = Column(Integer, primary_key=True)
    chat_id = Column(BigInteger, unique=True, nullable=False)
    user_id = Column(Integer, ForeignKey('user.id'), nullable=True)  # Связь с User
    employee_id = Column(Integer, ForeignKey('employee.id'), nullable=True)  # Связь с Employee (для менеджеров)
    username = Column(String(200), nullable=True)
    first_name = Column(String(200), nullable=True)
    last_name = Column(String(200), nullable=True)
    added_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True, nullable=False)
    is_manager = Column(Boolean, default=False, nullable=False)  # True если менеджер, False если админ
    
    user = relationship('User')
    employee = relationship('Employee')

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Патч для отключения signal handlers в отдельном потоке
_original_add_signal_handler = None
def _patch_signal_handlers():
    """Патч для отключения signal handlers в отдельном потоке"""
    global _original_add_signal_handler
    if _original_add_signal_handler is None:
        _original_add_signal_handler = asyncio.AbstractEventLoop.add_signal_handler
        
        def patched_add_signal_handler(self, sig, callback, *args):
            """Патч для отключения signal handlers в отдельном потоке"""
            try:
                return _original_add_signal_handler(self, sig, callback, *args)
            except (ValueError, RuntimeError) as e:
                if "set_wakeup_fd" in str(e) or "signal" in str(e).lower() or "main thread" in str(e).lower():
                    # Игнорируем ошибку - signal handlers не нужны в отдельном потоке
                    return None
                raise
        
        asyncio.AbstractEventLoop.add_signal_handler = patched_add_signal_handler

# Состояние для заполнения полей заявки на сток: {chat_id: {"pending_stock_id": int, "message_id": int}}
stock_completion_state = {}

# Состояние для ввода цены покупки по заявке на продажу: {chat_id: {"pending_sale_id": int, "message_id": int}}
sale_buy_price_state = {}

# Состояние для авторизации: {chat_id: {"step": "login"|"password", "username": str}}
auth_state = {}


def get_db_session():
    """Получить сессию базы данных"""
    return Session()


def _sale_keyboard(pending_sale):
    """Клавиатура для заявки на продажу: только Бонус (остальные действия только на сайте)."""
    rows = [
        [InlineKeyboardButton(text="💰 Бонус", callback_data=f"bonus_sale_{pending_sale.id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_pending_sale_notification(pending_sale_id: int):
    """Отправить уведомление о новой временной заявке всем администраторам"""
    print(f"[BOT] Попытка отправить уведомление о заявке #{pending_sale_id}")
    db = get_db_session()
    
    try:
        # Получаем всех активных администраторов (не менеджеров)
        admins = db.query(BotAdmin).filter(BotAdmin.is_active == True, BotAdmin.is_manager == False).all()
        print(f"[BOT] Найдено активных администраторов: {len(admins)}")
        if not admins:
            print("[BOT] ❌ Нет активных администраторов. Используйте команду /start в боте.")
            return None
        
        # Загружаем временную заявку из базы данных
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale:
            print(f"Временная заявка с ID {pending_sale_id} не найдена в базе данных")
            return None
        
        # Получаем связанные данные
        city = db.query(City).filter(City.id == pending_sale.city_id).first()
        employee = db.query(Employee).filter(Employee.id == pending_sale.employee_id).first()
        investor = db.query(Investor).filter(Investor.id == pending_sale.investor_id).first() if pending_sale.investor_id else None
        
        # Получаем расходы
        expenses = db.query(PendingSaleExpense).filter(PendingSaleExpense.pending_sale_id == pending_sale.id).all()
        expense_types = {et.id: et.name for et in db.query(ExpenseType).all()}
        
        # Формируем текст сообщения
        message_text = f"🆕 <b>Новая заявка</b> 📦 <b>К ПРОДАЖЕ</b>\n<a href='{PENDING_REQUESTS_URL}'>Открыть приложение</a>\n\n"
        message_text += f"<b>Товар:</b> {pending_sale.product_name}\n"
        message_text += f"<b>Цена покупки:</b> {f'{pending_sale.buy_price:.2f}' if (pending_sale.buy_price is not None and pending_sale.buy_price != 0) else '— допишет админ'}\n"
        message_text += f"<b>Цена продажи:</b> {pending_sale.sell_price:.2f}\n"
        message_text += f"<b>Город:</b> {city.name if city else 'Не указан'}\n"
        message_text += f"<b>Сотрудник:</b> {employee.name if employee else 'Не указан'}\n"
        
        if investor:
            message_text += f"<b>Инвестор:</b> {investor.name}\n"
        
        # Дата (перемещена под инвестора)
        sale_date = pending_sale.date.strftime('%d.%m.%Y %H:%M') if isinstance(pending_sale.date, datetime) else str(pending_sale.date)
        message_text += f"<b>Дата:</b> {sale_date}\n"
        
        # Расходы
        if expenses:
            message_text += f"\n<b>Расходы:</b>\n"
            total_expenses = 0
            for exp in expenses:
                exp_type_name = expense_types.get(exp.expense_type_id, 'Неизвестно')
                message_text += f"  • {exp_type_name}: {exp.amount:.2f}"
                if exp.comment:
                    message_text += f" ({exp.comment})"
                message_text += "\n"
                total_expenses += exp.amount
            message_text += f"<b>Итого расходов:</b> {total_expenses:.2f}\n"
        
        # Бонус менеджера
        bonus = pending_sale.bonus if pending_sale.bonus else 0.0
        if bonus > 0:
            message_text += f"<b>Бонус менеджера:</b> {bonus:.2f}\n"
        
        # Чистая прибыль (с учетом бонуса; если нет цены покупки — не считаем)
        if pending_sale.buy_price is not None and pending_sale.buy_price != 0:
            profit = pending_sale.sell_price - pending_sale.buy_price - sum(e.amount for e in expenses) - bonus
            message_text += f"\n<b>Чистая прибыль:</b> {profit:.2f}\n"
        else:
            message_text += f"\n<b>Чистая прибыль:</b> — (допишите цену покупки на сайте или в боте)\n"
        
        keyboard = _sale_keyboard(pending_sale)
        
        # Отправляем сообщение всем администраторам
        uploads_dir = os.path.join(BASE_DIR, "uploads")
        sent_messages = []
        
        for admin in admins:
            try:
                print(f"[BOT] Отправка уведомления администратору chat_id={admin.chat_id}")
                if pending_sale.photo and os.path.exists(os.path.join(uploads_dir, pending_sale.photo)):
                    photo_path = os.path.join(uploads_dir, pending_sale.photo)
                    photo = FSInputFile(photo_path)
                    sent_message = await bot.send_photo(
                        chat_id=admin.chat_id,
                        photo=photo,
                        caption=message_text,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
                    print(f"[BOT] ✅ Уведомление с фото отправлено администратору chat_id={admin.chat_id}")
                else:
                    sent_message = await bot.send_message(
                        chat_id=admin.chat_id,
                        text=message_text,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
                sent_messages.append(sent_message)
            except Exception as e:
                print(f"[BOT] ❌ Ошибка при отправке уведомления администратору chat_id={admin.chat_id}: {e}")
                import traceback
                traceback.print_exc()
        
        # Сохраняем ID сообщения в заявке (берем первое успешное сообщение)
        if sent_messages:
            pending_sale.telegram_message_id = sent_messages[0].message_id
            pending_sale.telegram_chat_id = sent_messages[0].chat.id
            db.commit()
            return sent_messages[0].message_id
        
        if not sent_messages:
            print(f"[BOT] ⚠️ Не удалось отправить уведомление ни одному администратору")
        return None
        
    except Exception as e:
        print(f"[BOT] ❌ Критическая ошибка при отправке уведомления: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        return None
    finally:
        db.close()


@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start - запрашивает авторизацию"""
    global auth_state
    
    chat_id = message.chat.id
    
    # Проверяем, авторизован ли пользователь
    db = get_db_session()
    try:
        existing_user = db.query(BotAdmin).filter(BotAdmin.chat_id == chat_id, BotAdmin.is_active == True).first()
        if existing_user:
            if existing_user.is_manager:
                await message.answer(
                    "👋 Добро пожаловать, менеджер!\n\n"
                    "Вы будете получать уведомления о начисленных бонусах.\n\n"
                    "Используйте /stop чтобы отключить уведомления."
                )
            else:
                await message.answer(
                    "🤖 Добро пожаловать, администратор!\n\n"
                    "Все новые заявки (на продажу и на сток) будут приходить сюда.\n"
                    "Используйте кнопки для принятия или отклонения заявок.\n\n"
                    "Используйте /stop чтобы отключить уведомления."
                )
            db.close()
            return
    except:
        pass
    finally:
        db.close()
    
    # Если не авторизован, запрашиваем логин
    auth_state[chat_id] = {"step": "login"}
    await message.answer(
        "🔐 Авторизация в боте\n\n"
        "Введите ваш логин:"
    )


@dp.message(Command("stop"))
async def cmd_stop(message: Message):
    """Обработчик команды /stop - деактивирует пользователя"""
    db = get_db_session()
    
    try:
        chat_id = message.chat.id
        admin = db.query(BotAdmin).filter(BotAdmin.chat_id == chat_id).first()
        
        if admin and admin.is_active:
            admin.is_active = False
            db.commit()
            await message.answer(
                "👋 Вы отключены от получения уведомлений.\n\n"
                "Используйте /start чтобы снова получать уведомления."
            )
        else:
            await message.answer(
                "Вы не были в списке администраторов или уже отключены."
            )
    except Exception as e:
        print(f"Ошибка при обработке команды /stop: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")
    finally:
        db.close()


@dp.message(Command("test_month_reminder"))
async def cmd_test_month_reminder(message: Message):
    """Проверка уведомления «месяц после продажи»: отправить в этот чат пример по последней продаже."""
    db = get_db_session()
    try:
        admin = db.query(BotAdmin).filter(BotAdmin.chat_id == message.chat.id, BotAdmin.is_active == True).first()
        if not admin:
            await message.answer("Сначала авторизуйтесь через /start.")
            return
        sale = db.query(Sale).order_by(Sale.date.desc()).first()
        if not sale:
            await message.answer("В базе пока нет продаж — нечего показать.")
            return
        text = _build_month_reminder_text(sale, db)
        prefix = (
            "🧪 <b>[ТЕСТ]</b> Так выглядит уведомление «месяц после продажи».\n"
            "Менеджеру оно приходит автоматически через месяц после даты продажи.\n\n"
        )
        await message.answer(prefix + text, parse_mode="HTML")
    except Exception as e:
        print(f"Ошибка /test_month_reminder: {e}")
        import traceback
        traceback.print_exc()
        await message.answer("❌ Ошибка при формировании теста.")
    finally:
        db.close()


@dp.message(Command("test_birthday_reminder"))
async def cmd_test_birthday_reminder(message: Message):
    """Проверка уведомления о дне рождения: отправить в этот чат пример по первому клиенту с датой рождения."""
    db = get_db_session()
    try:
        admin = db.query(BotAdmin).filter(BotAdmin.chat_id == message.chat.id, BotAdmin.is_active == True).first()
        if not admin:
            await message.answer("Сначала авторизуйтесь через /start.")
            return
        client = db.query(Client).filter(Client.birth_date != None).first()
        if not client:
            await message.answer(
                "В базе нет клиентов с указанной датой рождения. "
                "Добавьте дату рождения в карточке клиента на сайте."
            )
            return
        text = _build_birthday_reminder_text(client)
        prefix = "🧪 <b>[ТЕСТ]</b> Так выглядит уведомление о дне рождения клиента. Менеджерам оно приходит каждый день, если у кого-то из клиентов ДР.\n\n"
        await message.answer(prefix + text, parse_mode="HTML")
    except Exception as e:
        print(f"Ошибка /test_birthday_reminder: {e}")
        import traceback
        traceback.print_exc()
        await message.answer("❌ Ошибка при формировании теста.")
    finally:
        db.close()


@dp.callback_query(F.data.startswith("approve_sale_"))
async def approve_sale(callback: CallbackQuery):
    """Обработчик кнопки 'Принять' для продажи - создает Sale из PendingSale"""
    db = get_db_session()
    
    try:
        pending_sale_id = int(callback.data.split("_")[2])
        
        # Загружаем временную заявку
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale:
            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return
        if pending_sale.buy_price is None or pending_sale.buy_price == 0:
            await callback.answer("❌ Сначала укажите цену покупки на сайте (Заявки → Дополнить заявку)", show_alert=True)
            return
        
        # Создаем реальную продажу из временной заявки
        total_bonus = pending_sale.bonus if pending_sale.bonus else 0.0
        initial_bonus = getattr(pending_sale, 'initial_bonus', None) or 0.0
        new_sale = Sale(
            photo=pending_sale.photo,
            product_name=pending_sale.product_name,
            reference=pending_sale.reference,
            item_year=pending_sale.item_year,
            buy_price=pending_sale.buy_price,
            sell_price=pending_sale.sell_price,
            bonus=total_bonus,
            initial_bonus=initial_bonus,
            komplektatsiya=getattr(pending_sale, 'komplektatsiya', None),
            komissionnyy=getattr(pending_sale, 'komissionnyy', False),
            city_id=pending_sale.city_id,
            employee_id=pending_sale.employee_id,
            investor_id=pending_sale.investor_id,
            date=pending_sale.date
        )
        db.add(new_sale)
        db.flush()  # Получаем ID новой продажи
        
        # Переносим расходы
        pending_expenses = db.query(PendingSaleExpense).filter(PendingSaleExpense.pending_sale_id == pending_sale.id).all()
        for pending_exp in pending_expenses:
            expense = Expense(
                sale_id=new_sale.id,
                expense_type_id=pending_exp.expense_type_id,
                amount=pending_exp.amount,
                comment=pending_exp.comment
            )
            db.add(expense)
        
        # Копируем доп. бонусы другим менеджерам и запоминаем для уведомлений
        extra_bonus_recipients = []
        pending_extra = db.query(PendingSaleAdditionalBonus).filter(PendingSaleAdditionalBonus.pending_sale_id == pending_sale.id).all()
        for ab in pending_extra:
            db.add(SaleAdditionalBonus(sale_id=new_sale.id, employee_id=ab.employee_id, amount=ab.amount))
            extra_bonus_recipients.append((ab.employee_id, ab.amount))
        
        # Если продажа из стока, помечаем сток как проданный
        if pending_sale.stock_id:
            stock_item = db.query(StockItem).filter(StockItem.id == pending_sale.stock_id).first()
            if stock_item:
                stock_item.sold = True
        
        # Сохраняем bonus до commit (чтобы избежать DetachedInstanceError)
        sale_bonus = pending_sale.bonus if pending_sale.bonus else 0.0
        sale_id = new_sale.id
        
        # Обновляем статус заявки перед удалением
        pending_sale.status = 'approved'
        db.commit()
        
        # Обновляем сообщения для всех администраторов
        await update_telegram_message_for_sale_async(pending_sale_id, 'approved')
        
        # Отправляем уведомление менеджеру о бонусе (если есть)
        if sale_bonus > 0:
            await send_bonus_notification_to_manager(sale_id)
        for emp_id, amount in extra_bonus_recipients:
            if amount and amount > 0:
                await send_extra_bonus_notification_to_manager(sale_id, emp_id, amount)
        
        # Удаляем временную заявку и связанные доп. бонусы
        db.query(PendingSaleAdditionalBonus).filter(PendingSaleAdditionalBonus.pending_sale_id == pending_sale_id).delete()
        db.delete(pending_sale)
        db.commit()
        
        await callback.answer("✅ Заявка принята и добавлена в базу!")
        
    except Exception as e:
        print(f"Ошибка при принятии заявки: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        await callback.answer("❌ Ошибка при обработке заявки", show_alert=True)
    finally:
        db.close()


@dp.callback_query(F.data.startswith("reject_sale_"))
async def reject_sale(callback: CallbackQuery):
    """Обработчик кнопки 'Отклонить' для продажи - удаляет PendingSale"""
    db = get_db_session()
    
    try:
        pending_sale_id = int(callback.data.split("_")[2])
        
        # Загружаем временную заявку
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale:
            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return
        
        # Удаляем фото, если оно было загружено специально для этой заявки
        # (не удаляем если фото из стока)
        if pending_sale.photo and not pending_sale.stock_id:
            uploads_dir = os.path.join(BASE_DIR, "uploads")
            photo_path = os.path.join(uploads_dir, pending_sale.photo)
            if os.path.exists(photo_path):
                try:
                    os.remove(photo_path)
                except:
                    pass
        
        # Обновляем статус заявки перед удалением
        pending_sale.status = 'rejected'
        db.commit()
        
        # Обновляем сообщения для всех администраторов
        await update_telegram_message_for_sale_async(pending_sale_id, 'rejected')
        
        # Удаляем временную заявку (расходы удалятся каскадно)
        db.delete(pending_sale)
        db.commit()
        
        await callback.answer("❌ Заявка отклонена и удалена!")
        
    except Exception as e:
        print(f"Ошибка при отклонении заявки: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        await callback.answer("❌ Ошибка при обработке заявки", show_alert=True)
    finally:
        db.close()


@dp.callback_query(F.data.startswith("bonus_sale_"))
async def bonus_sale(callback: CallbackQuery):
    """Обработчик кнопки 'Бонус' для продажи - показывает клавиатуру для выбора/ввода бонуса"""
    db = get_db_session()
    
    try:
        pending_sale_id = int(callback.data.split("_")[2])
        
        # Загружаем временную заявку
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale:
            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return
        
        if pending_sale.status != 'pending':
            await callback.answer("❌ Заявка уже обработана", show_alert=True)
            return
        
        # Создаем клавиатуру с быстрыми суммами и кнопкой ввода
        current_bonus = pending_sale.bonus if pending_sale.bonus else 0.0
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="50$", callback_data=f"set_bonus_{pending_sale_id}_50"),
                InlineKeyboardButton(text="100$", callback_data=f"set_bonus_{pending_sale_id}_100"),
                InlineKeyboardButton(text="200$", callback_data=f"set_bonus_{pending_sale_id}_200")
            ],
            [
                InlineKeyboardButton(text="500$", callback_data=f"set_bonus_{pending_sale_id}_500"),
                InlineKeyboardButton(text="1000$", callback_data=f"set_bonus_{pending_sale_id}_1000")
            ],
            [
                InlineKeyboardButton(text="Ввести сумму", callback_data=f"input_bonus_{pending_sale_id}")
            ],
            [
                InlineKeyboardButton(text="Сбросить (0$)", callback_data=f"set_bonus_{pending_sale_id}_0")
            ],
            [
                InlineKeyboardButton(text="◀️ Назад", callback_data=f"back_sale_{pending_sale_id}")
            ]
        ])
        
        await callback.message.edit_reply_markup(reply_markup=keyboard)
        await callback.answer(f"Текущий бонус: {current_bonus:.2f}$")
        
    except Exception as e:
        print(f"Ошибка при обработке бонуса: {e}")
        import traceback
        traceback.print_exc()
        await callback.answer("❌ Ошибка", show_alert=True)
    finally:
        db.close()


@dp.callback_query(F.data.startswith("set_bonus_"))
async def set_bonus_amount(callback: CallbackQuery):
    """Добавить сумму бонуса (прибавляется к текущему)"""
    db = get_db_session()
    
    try:
        parts = callback.data.split("_")
        pending_sale_id = int(parts[2])
        bonus_to_add = float(parts[3])
        
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale or pending_sale.status != 'pending':
            await callback.answer("❌ Заявка не найдена или уже обработана", show_alert=True)
            return
        
        # Если бонус = 0, сбрасываем, иначе прибавляем к текущему
        current_bonus = pending_sale.bonus if pending_sale.bonus else 0.0
        if bonus_to_add == 0:
            new_total_bonus = 0.0
            message_text = f"💰 <b>Итоговый бонус:</b> 0.00$\n\n"
            message_text += f"Текущий бонус: {current_bonus:.2f}$\n"
            message_text += f"Сброс бонуса"
        else:
            new_total_bonus = current_bonus + bonus_to_add
            # Показываем промежуточное сообщение с итоговым бонусом
            message_text = f"💰 <b>Итоговый бонус:</b> {new_total_bonus:.2f}$\n\n"
            message_text += f"Текущий бонус: {current_bonus:.2f}$\n"
            message_text += f"Добавляется: {bonus_to_add:.2f}$\n"
            message_text += f"Итого: {new_total_bonus:.2f}$"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_bonus_{pending_sale_id}_{new_total_bonus}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"cancel_bonus_{pending_sale_id}")
            ]
        ])
        
        await callback.message.answer(message_text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        print(f"Ошибка при установке бонуса: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        await callback.answer("❌ Ошибка", show_alert=True)
    finally:
        db.close()


@dp.callback_query(F.data.startswith("input_bonus_"))
async def input_bonus(callback: CallbackQuery):
    """Запросить ввод суммы бонуса"""
    db = get_db_session()
    
    try:
        pending_sale_id = int(callback.data.split("_")[2])
        
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale or pending_sale.status != 'pending':
            await callback.answer("❌ Заявка не найдена или уже обработана", show_alert=True)
            return
        
        # Сохраняем состояние для ввода бонуса
        global bonus_input_state
        if 'bonus_input_state' not in globals():
            bonus_input_state = {}
        bonus_input_state[callback.message.chat.id] = {
            "pending_sale_id": pending_sale_id,
            "message_id": callback.message.message_id
        }
        
        await callback.message.answer("💰 Введите сумму бонуса (например: 150 или 250.50):")
        await callback.answer()
        
    except Exception as e:
        print(f"Ошибка при запросе ввода бонуса: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)
    finally:
        db.close()


@dp.callback_query(F.data.startswith("confirm_bonus_"))
async def confirm_bonus(callback: CallbackQuery):
    """Подтвердить установку бонуса"""
    db = get_db_session()
    
    try:
        parts = callback.data.split("_")
        pending_sale_id = int(parts[2])
        new_total_bonus = float(parts[3])
        
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale or pending_sale.status != 'pending':
            await callback.answer("❌ Заявка не найдена или уже обработана", show_alert=True)
            return
        
        # Устанавливаем итоговый бонус
        pending_sale.bonus = new_total_bonus
        db.commit()
        
        # Обновляем все сообщения о заявке для всех админов
        await refresh_all_sale_messages(pending_sale_id)
        
        # Удаляем промежуточное сообщение
        try:
            await callback.message.delete()
        except:
            pass
        
        await callback.answer(f"✅ Бонус установлен: {new_total_bonus:.2f}$")
        
    except Exception as e:
        print(f"Ошибка при подтверждении бонуса: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        await callback.answer("❌ Ошибка", show_alert=True)
    finally:
        db.close()


@dp.callback_query(F.data.startswith("cancel_bonus_"))
async def cancel_bonus(callback: CallbackQuery):
    """Отменить установку бонуса"""
    db = get_db_session()
    
    try:
        pending_sale_id = int(callback.data.split("_")[2])
        
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale:
            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return
        
        # Удаляем промежуточное сообщение
        try:
            await callback.message.delete()
        except:
            pass
        
        await callback.answer("❌ Установка бонуса отменена")
        
    except Exception as e:
        print(f"Ошибка при отмене бонуса: {e}")
        import traceback
        traceback.print_exc()
        await callback.answer("❌ Ошибка", show_alert=True)
    finally:
        db.close()


@dp.callback_query(F.data.startswith("back_sale_"))
async def back_to_sale_buttons(callback: CallbackQuery):
    """Вернуться к основным кнопкам заявки"""
    db = get_db_session()
    
    try:
        pending_sale_id = int(callback.data.split("_")[2])
        
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale:
            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return
        
        keyboard = _sale_keyboard(pending_sale)
        await callback.message.edit_reply_markup(reply_markup=keyboard)
        await callback.answer()
        
    except Exception as e:
        print(f"Ошибка при возврате к кнопкам: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)
    finally:
        db.close()


@dp.callback_query(F.data.startswith("sale_buy_"))
async def sale_buy_price_start(callback: CallbackQuery):
    """Запросить ввод цены покупки по заявке на продажу"""
    db = get_db_session()
    try:
        pending_sale_id = int(callback.data.split("_")[2])
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale or pending_sale.status != 'pending':
            await callback.answer("❌ Заявка не найдена или уже обработана", show_alert=True)
            return
        global sale_buy_price_state
        sale_buy_price_state[callback.message.chat.id] = {
            "pending_sale_id": pending_sale_id,
            "message_id": callback.message.message_id
        }
        await callback.message.answer("📝 Введите цену покупки (число, например: 10000 или 5000.50):")
        await callback.answer()
    except Exception as e:
        print(f"Ошибка sale_buy_price_start: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)
    finally:
        db.close()


async def refresh_all_sale_messages(pending_sale_id: int):
    """Обновить все сообщения о заявке для всех админов"""
    db = get_db_session()
    
    try:
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale:
            return
        
        # Получаем всех активных администраторов
        admins = db.query(BotAdmin).filter(BotAdmin.is_active == True, BotAdmin.is_manager == False).all()
        
        city = db.query(City).filter(City.id == pending_sale.city_id).first()
        employee = db.query(Employee).filter(Employee.id == pending_sale.employee_id).first()
        investor = db.query(Investor).filter(Investor.id == pending_sale.investor_id).first() if pending_sale.investor_id else None
        
        expenses = db.query(PendingSaleExpense).filter(PendingSaleExpense.pending_sale_id == pending_sale.id).all()
        expense_types = {et.id: et.name for et in db.query(ExpenseType).all()}
        
        message_text = f"🆕 <b>Новая заявка</b> 📦 <b>К ПРОДАЖЕ</b>\n<a href='{PENDING_REQUESTS_URL}'>Открыть приложение</a>\n\n"
        message_text += f"<b>Товар:</b> {pending_sale.product_name}\n"
        message_text += f"<b>Цена покупки:</b> {f'{pending_sale.buy_price:.2f}' if (pending_sale.buy_price is not None and pending_sale.buy_price != 0) else '— допишет админ'}\n"
        message_text += f"<b>Цена продажи:</b> {pending_sale.sell_price:.2f}\n"
        message_text += f"<b>Город:</b> {city.name if city else '—'}\n"
        message_text += f"<b>Сотрудник:</b> {employee.name if employee else '—'}\n"
        
        if investor:
            message_text += f"<b>Инвестор:</b> {investor.name}\n"
        
        # Дата (перемещена под инвестора)
        sale_date = pending_sale.date.strftime('%d.%m.%Y %H:%M') if isinstance(pending_sale.date, datetime) else str(pending_sale.date)
        message_text += f"<b>Дата:</b> {sale_date}\n"
        
        if expenses:
            total_expenses = sum(e.amount for e in expenses)
            message_text += f"\n<b>Расходы:</b>\n"
            for exp in expenses:
                exp_type_name = expense_types.get(exp.expense_type_id, 'Неизвестно')
                message_text += f"• {exp_type_name}: {exp.amount:.2f}"
                if exp.comment:
                    message_text += f" ({exp.comment})"
                message_text += "\n"
            message_text += f"<b>Итого расходов:</b> {total_expenses:.2f}\n"
        
        # Бонус менеджера
        bonus = pending_sale.bonus if pending_sale.bonus else 0.0
        if bonus > 0:
            message_text += f"<b>Бонус менеджера:</b> {bonus:.2f}\n"
        
        # Чистая прибыль (с учетом бонуса)
        total_expenses = sum(e.amount for e in expenses) if expenses else 0
        if pending_sale.buy_price is not None and pending_sale.buy_price != 0:
            profit = pending_sale.sell_price - pending_sale.buy_price - total_expenses - bonus
            message_text += f"\n<b>Чистая прибыль:</b> {profit:.2f}\n"
        else:
            message_text += f"\n<b>Чистая прибыль:</b> — (допишите цену покупки на сайте или в боте)\n"
        
        keyboard = _sale_keyboard(pending_sale)
        
        # Обновляем сообщения для всех админов
        uploads_dir = os.path.join(BASE_DIR, "uploads")
        for admin in admins:
            try:
                # Пытаемся обновить существующее сообщение, если оно есть
                if pending_sale.telegram_chat_id == admin.chat_id and pending_sale.telegram_message_id:
                    try:
                        if pending_sale.photo and os.path.exists(os.path.join(uploads_dir, pending_sale.photo)):
                            photo_path = os.path.join(uploads_dir, pending_sale.photo)
                            photo = FSInputFile(photo_path)
                            await bot.edit_message_caption(
                                chat_id=admin.chat_id,
                                message_id=pending_sale.telegram_message_id,
                                caption=message_text,
                                reply_markup=keyboard,
                                parse_mode="HTML"
                            )
                        else:
                            await bot.edit_message_text(
                                chat_id=admin.chat_id,
                                message_id=pending_sale.telegram_message_id,
                                text=message_text,
                                reply_markup=keyboard,
                                parse_mode="HTML"
                            )
                    except Exception as e:
                        # Если не удалось обновить, отправляем новое сообщение
                        print(f"Не удалось обновить сообщение для {admin.chat_id}, отправляем новое: {e}")
                        if pending_sale.photo and os.path.exists(os.path.join(uploads_dir, pending_sale.photo)):
                            photo_path = os.path.join(uploads_dir, pending_sale.photo)
                            photo = FSInputFile(photo_path)
                            sent_message = await bot.send_photo(
                                chat_id=admin.chat_id,
                                photo=photo,
                                caption=message_text,
                                reply_markup=keyboard,
                                parse_mode="HTML"
                            )
                        else:
                            sent_message = await bot.send_message(
                                chat_id=admin.chat_id,
                                text=message_text,
                                reply_markup=keyboard,
                                parse_mode="HTML"
                            )
                        # Обновляем message_id для первого админа
                        if admin.chat_id == admins[0].chat_id:
                            pending_sale.telegram_message_id = sent_message.message_id
                            pending_sale.telegram_chat_id = sent_message.chat.id
                            db.commit()
                else:
                    # Если сообщения нет, отправляем новое
                    if pending_sale.photo and os.path.exists(os.path.join(uploads_dir, pending_sale.photo)):
                        photo_path = os.path.join(uploads_dir, pending_sale.photo)
                        photo = FSInputFile(photo_path)
                        sent_message = await bot.send_photo(
                            chat_id=admin.chat_id,
                            photo=photo,
                            caption=message_text,
                            reply_markup=keyboard,
                            parse_mode="HTML"
                        )
                    else:
                        sent_message = await bot.send_message(
                            chat_id=admin.chat_id,
                            text=message_text,
                            reply_markup=keyboard,
                            parse_mode="HTML"
                        )
                    # Обновляем message_id для первого админа
                    if admin.chat_id == admins[0].chat_id:
                        pending_sale.telegram_message_id = sent_message.message_id
                        pending_sale.telegram_chat_id = sent_message.chat.id
                        db.commit()
            except Exception as e:
                print(f"Ошибка при обновлении сообщения для админа {admin.chat_id}: {e}")
                import traceback
                traceback.print_exc()
            
    except Exception as e:
        print(f"Ошибка при обновлении сообщений о заявке: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


async def refresh_sale_message(pending_sale_id: int, chat_id: int, message_id: int):
    """Обновить сообщение о заявке с актуальными данными (для обратной совместимости)"""
    await refresh_all_sale_messages(pending_sale_id)


async def refresh_sale_message_old(pending_sale_id: int, chat_id: int, message_id: int):
    """Обновить сообщение о заявке с актуальными данными"""
    db = get_db_session()
    
    try:
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale:
            return
        
        city = db.query(City).filter(City.id == pending_sale.city_id).first()
        employee = db.query(Employee).filter(Employee.id == pending_sale.employee_id).first()
        investor = db.query(Investor).filter(Investor.id == pending_sale.investor_id).first() if pending_sale.investor_id else None
        
        expenses = db.query(PendingSaleExpense).filter(PendingSaleExpense.pending_sale_id == pending_sale.id).all()
        expense_types = {et.id: et.name for et in db.query(ExpenseType).all()}
        
        message_text = f"🆕 <b>Новая заявка</b> 📦 <b>К ПРОДАЖЕ</b>\n<a href='{PENDING_REQUESTS_URL}'>Открыть приложение</a>\n\n"
        message_text += f"<b>Товар:</b> {pending_sale.product_name}\n"
        message_text += f"<b>Цена покупки:</b> {f'{pending_sale.buy_price:.2f}' if (pending_sale.buy_price is not None and pending_sale.buy_price != 0) else '— допишет админ'}\n"
        message_text += f"<b>Цена продажи:</b> {pending_sale.sell_price:.2f}\n"
        message_text += f"<b>Город:</b> {city.name if city else '—'}\n"
        message_text += f"<b>Сотрудник:</b> {employee.name if employee else '—'}\n"
        
        if investor:
            message_text += f"<b>Инвестор:</b> {investor.name}\n"
        
        # Дата (перемещена под инвестора)
        sale_date = pending_sale.date.strftime('%d.%m.%Y %H:%M') if isinstance(pending_sale.date, datetime) else str(pending_sale.date)
        message_text += f"<b>Дата:</b> {sale_date}\n"
        
        if expenses:
            total_expenses = sum(e.amount for e in expenses)
            message_text += f"\n<b>Расходы:</b>\n"
            for exp in expenses:
                exp_type_name = expense_types.get(exp.expense_type_id, 'Неизвестно')
                message_text += f"• {exp_type_name}: {exp.amount:.2f}"
                if exp.comment:
                    message_text += f" ({exp.comment})"
                message_text += "\n"
            message_text += f"<b>Итого расходов:</b> {total_expenses:.2f}\n"
        
        # Бонус менеджера
        bonus = pending_sale.bonus if pending_sale.bonus else 0.0
        if bonus > 0:
            message_text += f"<b>Бонус менеджера:</b> {bonus:.2f}\n"
        
        # Чистая прибыль (с учетом бонуса)
        total_expenses = sum(e.amount for e in expenses) if expenses else 0
        if pending_sale.buy_price is not None and pending_sale.buy_price != 0:
            profit = pending_sale.sell_price - pending_sale.buy_price - total_expenses - bonus
            message_text += f"\n<b>Чистая прибыль:</b> {profit:.2f}\n"
        else:
            message_text += f"\n<b>Чистая прибыль:</b> — (допишите цену покупки на сайте или в боте)\n"
        
        keyboard = _sale_keyboard(pending_sale)
        
        # Обновляем сообщение
        try:
            if pending_sale.photo:
                uploads_dir = os.path.join(BASE_DIR, "uploads")
                if os.path.exists(os.path.join(uploads_dir, pending_sale.photo)):
                    photo_path = os.path.join(uploads_dir, pending_sale.photo)
                    photo = FSInputFile(photo_path)
                    await bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=message_id,
                        caption=message_text,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=message_text,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
            else:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=message_text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
        except Exception as e:
            print(f"Ошибка при обновлении сообщения: {e}")
            
    except Exception as e:
        print(f"Ошибка при обновлении сообщения о заявке: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


async def send_pending_stock_notification(pending_stock_id: int):
    """Отправить уведомление о новой временной заявке на сток всем администраторам"""
    db = get_db_session()
    
    try:
        # Получаем всех активных администраторов (не менеджеров)
        admins = db.query(BotAdmin).filter(BotAdmin.is_active == True, BotAdmin.is_manager == False).all()
        if not admins:
            print("Нет активных администраторов. Используйте команду /start в боте.")
            return None
        
        # Загружаем временную заявку из базы данных
        pending_stock = db.query(PendingStock).filter(PendingStock.id == pending_stock_id).first()
        if not pending_stock:
            print(f"Временная заявка на сток с ID {pending_stock_id} не найдена в базе данных")
            return None
        
        # Получаем связанные данные
        city = db.query(City).filter(City.id == pending_stock.city_id).first()
        investor = db.query(Investor).filter(Investor.id == pending_stock.investor_id).first() if pending_stock.investor_id else None
        
        # Получаем расходы
        expenses = db.query(PendingStockExpense).filter(PendingStockExpense.pending_stock_id == pending_stock.id).all()
        expense_types = {et.id: et.name for et in db.query(ExpenseType).all()}
        
        # Проверяем, заполнена ли заявка
        if pending_stock.komissionnyy:
            # Для комиссионного стока: проверяем выплату
            is_complete = pending_stock.buy_price is not None
        else:
            # Для общего стока: проверяем только цену покупки (инвестор необязателен)
            is_complete = pending_stock.buy_price is not None
        
        # Формируем текст сообщения
        if pending_stock.komissionnyy:
            message_text = f"🆕 <b>Новая заявка</b> 📦 <b>К КОМИССИОННОМУ СТОКУ</b>\n<a href='{PENDING_REQUESTS_URL}'>Открыть приложение</a>\n\n"
        else:
            message_text = f"🆕 <b>Новая заявка</b> 📦 <b>К СТОКУ</b>\n<a href='{PENDING_REQUESTS_URL}'>Открыть приложение</a>\n\n"
        message_text += f"<b>ID заявки:</b> {pending_stock.id}\n"
        message_text += f"<b>Товар:</b> {pending_stock.product_name}\n"
        
        if pending_stock.reference:
            message_text += f"<b>Референс:</b> {pending_stock.reference}\n"
        if pending_stock.item_year:
            message_text += f"<b>Год изделия:</b> {pending_stock.item_year}\n"
        
        if pending_stock.komissionnyy:
            # Для комиссионного стока: выплата и данные владельца
            message_text += f"<b>Выплата владельцу:</b> {f'{pending_stock.buy_price:.2f}' if pending_stock.buy_price is not None else '— допишите'}\n"
            message_text += f"<b>Ожидаемая цена продажи:</b> {pending_stock.expected_sell_price:.2f}\n"
            message_text += f"<b>Количество:</b> {pending_stock.quantity}\n"
            message_text += f"<b>Город:</b> {city.name if city else 'Не указан'}\n"
            message_text += f"<b>Владелец:</b> {pending_stock.client_full_name if pending_stock.client_full_name else '— допишите'}\n"
            message_text += f"<b>Телефон:</b> {pending_stock.client_phone if pending_stock.client_phone else '— допишите'}\n"
        else:
            # Для общего стока: цена покупки и инвестор
            message_text += f"<b>Цена покупки:</b> {f'{pending_stock.buy_price:.2f}' if pending_stock.buy_price is not None else '— допишите'}\n"
            message_text += f"<b>Ожидаемая цена продажи:</b> {pending_stock.expected_sell_price:.2f}\n"
            message_text += f"<b>Количество:</b> {pending_stock.quantity}\n"
            message_text += f"<b>Город:</b> {city.name if city else 'Не указан'}\n"
            message_text += f"<b>Инвестор:</b> {investor.name if investor else '—'}\n"
        
        # Расходы
        if expenses:
            message_text += f"\n<b>Расходы:</b>\n"
            total_expenses = 0
            for exp in expenses:
                exp_type_name = expense_types.get(exp.expense_type_id, 'Неизвестно')
                message_text += f"  • {exp_type_name}: {exp.amount:.2f}"
                if exp.comment:
                    message_text += f" ({exp.comment})"
                message_text += "\n"
                total_expenses += exp.amount
            message_text += f"<b>Итого расходов:</b> {total_expenses:.2f}\n"
        
        # Ожидаемая прибыль (только если есть цена покупки/выплата)
        if pending_stock.buy_price is not None:
            total_buy = pending_stock.buy_price * pending_stock.quantity
            total_expected_sell = pending_stock.expected_sell_price * pending_stock.quantity
            total_exp = sum(e.amount for e in expenses)
            expected_profit = total_expected_sell - total_buy - total_exp
            message_text += f"\n<b>Ожидаемая прибыль:</b> {expected_profit:.2f}\n"
        else:
            if pending_stock.komissionnyy:
                message_text += f"\n<b>Ожидаемая прибыль:</b> — укажите выплату\n"
            else:
                message_text += f"\n<b>Ожидаемая прибыль:</b> — укажите цену покупки\n"
        
        # Дата
        stock_date = pending_stock.created_at.strftime('%d.%m.%Y %H:%M') if isinstance(pending_stock.created_at, datetime) else str(pending_stock.created_at)
        message_text += f"<b>Дата:</b> {stock_date}\n"
        
        if not is_complete:
            if pending_stock.komissionnyy:
                message_text += "\n⚠️ Дополните заявку на сайте (имя владельца, телефон, выплата).\n"
            else:
                message_text += "\n⚠️ Дополните заявку на сайте (цена покупки).\n"
        
        # Без клавиатуры — всё дополнение и действия только на сайте
        keyboard = None
        
        # Отправляем сообщение всем администраторам
        uploads_dir = os.path.join(BASE_DIR, "uploads")
        sent_messages = []
        
        for admin in admins:
            try:
                if pending_stock.photo and os.path.exists(os.path.join(uploads_dir, pending_stock.photo)):
                    photo_path = os.path.join(uploads_dir, pending_stock.photo)
                    photo = FSInputFile(photo_path)
                    sent_message = await bot.send_photo(
                        chat_id=admin.chat_id,
                        photo=photo,
                        caption=message_text,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
                else:
                    sent_message = await bot.send_message(
                        chat_id=admin.chat_id,
                        text=message_text,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
                sent_messages.append(sent_message)
            except Exception as e:
                print(f"[BOT] ❌ Ошибка при отправке уведомления администратору chat_id={admin.chat_id}: {e}")
                import traceback
                traceback.print_exc()
        
        # Сохраняем ID сообщения в заявке (берем первое успешное сообщение)
        if sent_messages:
            pending_stock.telegram_message_id = sent_messages[0].message_id
            pending_stock.telegram_chat_id = sent_messages[0].chat.id
            db.commit()
            return sent_messages[0].message_id
        
        return None
        
    except Exception as e:
        print(f"Ошибка при отправке уведомления о стоке: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        return None
    finally:
        db.close()


@dp.callback_query(F.data.startswith("stock_inv_"))
async def stock_select_investor(callback: CallbackQuery):
    """Выбор инвестора (владельца) для заявки на сток от менеджера"""
    global stock_completion_state
    db = get_db_session()
    
    try:
        parts = callback.data.split("_")
        pending_stock_id = int(parts[2])
        investor_id = int(parts[3])
        
        pending_stock = db.query(PendingStock).filter(PendingStock.id == pending_stock_id).first()
        if not pending_stock or pending_stock.status != 'pending':
            await callback.answer("❌ Заявка не найдена или уже обработана", show_alert=True)
            return
        
        investor = db.query(Investor).filter(Investor.id == investor_id).first()
        if not investor:
            await callback.answer("❌ Инвестор не найден", show_alert=True)
            return
        
        pending_stock.investor_id = investor_id
        db.commit()
        
        stock_completion_state[callback.message.chat.id] = {
            "pending_stock_id": pending_stock_id,
            "message_id": callback.message.message_id,
            "has_photo": callback.message.photo is not None
        }
        
        new_text = (
            f"✅ <b>Инвестор выбран:</b> {investor.name}\n\n"
            f"Заявка #{pending_stock_id}\n"
            f"Товар: {pending_stock.product_name}\n\n"
            f"📝 <b>Введите цену покупки</b> (число, например 1000 или 1500.50):"
        )
        
        try:
            if callback.message.photo:
                await callback.message.edit_caption(
                    caption=new_text,
                    reply_markup=None,
                    parse_mode="HTML"
                )
            else:
                await callback.message.edit_text(
                    text=new_text,
                    reply_markup=None,
                    parse_mode="HTML"
                )
        except Exception:
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await callback.message.answer(new_text, parse_mode="HTML")
        
        await callback.answer(f"Инвестор: {investor.name}. Теперь введите цену покупки.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        db.rollback()
        await callback.answer("❌ Ошибка", show_alert=True)
    finally:
        db.close()


@dp.message(F.text)
async def handle_text_input(message: Message):
    """Обработка ввода текста - для авторизации, цены покупки стока или бонуса продажи"""
    global stock_completion_state
    global bonus_input_state
    global auth_state
    
    chat_id = message.chat.id
    
    # Проверяем, не авторизация ли это
    if chat_id in auth_state:
        db = get_db_session()
        try:
            state = auth_state[chat_id]
            if state["step"] == "login":
                username = message.text.strip()
                user = db.query(User).filter(User.username == username).first()
                if not user:
                    await message.answer("❌ Пользователь с таким логином не найден. Попробуйте еще раз или введите /start для начала.")
                    del auth_state[chat_id]
                    return
                
                auth_state[chat_id] = {"step": "password", "username": username}
                await message.answer("Введите пароль:")
            elif state["step"] == "password":
                username = state["username"]
                password = message.text.strip()
                user = db.query(User).filter(User.username == username).first()
                
                if not user or not user.check_password(password):
                    await message.answer("❌ Неверный пароль. Введите /start для повторной авторизации.")
                    del auth_state[chat_id]
                    return
                
                # Авторизация успешна
                username_tg = message.from_user.username
                first_name = message.from_user.first_name
                last_name = message.from_user.last_name
                
                # Проверяем, есть ли уже запись
                existing = db.query(BotAdmin).filter(BotAdmin.chat_id == chat_id).first()
                
                # Определяем, админ или менеджер
                is_manager = not user.is_admin
                employee = None
                if is_manager:
                    # Ищем Employee по имени пользователя
                    employee = db.query(Employee).filter(Employee.name == user.name).first()
                
                if existing:
                    existing.user_id = user.id
                    existing.employee_id = employee.id if employee else None
                    existing.username = username_tg
                    existing.first_name = first_name
                    existing.last_name = last_name
                    existing.is_active = True
                    existing.is_manager = is_manager
                    db.commit()
                else:
                    new_bot_user = BotAdmin(
                        chat_id=chat_id,
                        user_id=user.id,
                        employee_id=employee.id if employee else None,
                        username=username_tg,
                        first_name=first_name,
                        last_name=last_name,
                        is_active=True,
                        is_manager=is_manager
                    )
                    db.add(new_bot_user)
                    db.commit()
                
                del auth_state[chat_id]
                
                if is_manager:
                    await message.answer(
                        f"✅ Авторизация успешна!\n\n"
                        f"👋 Добро пожаловать, {user.name}!\n\n"
                        f"Вы будете получать уведомления о начисленных бонусах.\n\n"
                        f"Используйте /stop чтобы отключить уведомления."
                    )
                else:
                    await message.answer(
                        f"✅ Авторизация успешна!\n\n"
                        f"🤖 Добро пожаловать, администратор!\n\n"
                        f"Все новые заявки (на продажу и на сток) будут приходить сюда.\n"
                        f"Используйте кнопки для принятия или отклонения заявок.\n\n"
                        f"Используйте /stop чтобы отключить уведомления."
                    )
        except Exception as e:
            print(f"Ошибка при авторизации: {e}")
            import traceback
            traceback.print_exc()
            await message.answer("❌ Произошла ошибка при авторизации. Попробуйте /start")
            if chat_id in auth_state:
                del auth_state[chat_id]
        finally:
            db.close()
        return
    
    # Проверяем, не ввод ли это бонуса
    if 'bonus_input_state' in globals() and chat_id in bonus_input_state:
        db = get_db_session()
        try:
            state = bonus_input_state[chat_id]
            pending_sale_id = state["pending_sale_id"]
            msg_id = state["message_id"]
            
            try:
                bonus_amount = float(message.text.replace(",", ".").strip())
                if bonus_amount < 0:
                    await message.answer("❌ Введите положительное число или 0.")
                    return
            except ValueError:
                await message.answer("❌ Введите число (например: 150 или 250.50)")
                return
            
            pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
            if not pending_sale or pending_sale.status != 'pending':
                del bonus_input_state[chat_id]
                await message.answer("❌ Заявка не найдена или уже обработана.")
                return
            
            # Прибавляем к текущему бонусу
            current_bonus = pending_sale.bonus if pending_sale.bonus else 0.0
            new_total_bonus = current_bonus + bonus_amount
            
            # Показываем промежуточное сообщение с итоговым бонусом
            message_text = f"💰 <b>Итоговый бонус:</b> {new_total_bonus:.2f}$\n\n"
            message_text += f"Текущий бонус: {current_bonus:.2f}$\n"
            message_text += f"Добавляется: {bonus_amount:.2f}$\n"
            message_text += f"Итого: {new_total_bonus:.2f}$"
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_bonus_{pending_sale_id}_{new_total_bonus}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"cancel_bonus_{pending_sale_id}")
                ]
            ])
            
            await message.answer(message_text, reply_markup=keyboard, parse_mode="HTML")
            del bonus_input_state[chat_id]
            
        except Exception as e:
            print(f"Ошибка при обработке ввода бонуса: {e}")
            import traceback
            traceback.print_exc()
            db.rollback()
            await message.answer("❌ Ошибка при сохранении.")
        finally:
            db.close()
        return
    
    # Ввод цены покупки по заявке на продажу
    if 'sale_buy_price_state' in globals() and chat_id in sale_buy_price_state:
        db = get_db_session()
        try:
            state = sale_buy_price_state[chat_id]
            pending_sale_id = state["pending_sale_id"]
            try:
                buy_price = float(message.text.replace(",", ".").strip())
                if buy_price <= 0:
                    await message.answer("❌ Введите положительное число.")
                    return
            except ValueError:
                await message.answer("❌ Введите число (например: 10000 или 5000.50)")
                return
            pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
            if not pending_sale or pending_sale.status != 'pending':
                if chat_id in sale_buy_price_state:
                    del sale_buy_price_state[chat_id]
                await message.answer("❌ Заявка не найдена или уже обработана.")
                return
            pending_sale.buy_price = buy_price
            db.commit()
            del sale_buy_price_state[chat_id]
            await refresh_all_sale_messages(pending_sale_id)
            await message.answer("✅ Цена покупки сохранена. Заявка готова к одобрению.")
        except Exception as e:
            import traceback
            traceback.print_exc()
            db.rollback()
            await message.answer("❌ Ошибка при сохранении.")
        finally:
            db.close()
        return
    
    # Обработка ввода цены покупки для стока
    if chat_id not in stock_completion_state:
        return
    
    db = get_db_session()
    try:
        state = stock_completion_state[chat_id]
        pending_stock_id = state["pending_stock_id"]
        msg_id = state["message_id"]
        has_photo = state.get("has_photo", False)
        
        try:
            buy_price = float(message.text.replace(",", ".").strip())
            if buy_price <= 0:
                await message.answer("❌ Введите положительное число.")
                return
        except ValueError:
            await message.answer("❌ Введите число (например: 1000 или 1500.50)")
            return
        
        pending_stock = db.query(PendingStock).filter(PendingStock.id == pending_stock_id).first()
        if not pending_stock or pending_stock.status != 'pending':
            del stock_completion_state[chat_id]
            await message.answer("❌ Заявка не найдена или уже обработана.")
            return
        
        pending_stock.buy_price = buy_price
        db.commit()
        
        del stock_completion_state[chat_id]
        
        city = db.query(City).filter(City.id == pending_stock.city_id).first()
        investor = db.query(Investor).filter(Investor.id == pending_stock.investor_id).first()
        expenses = db.query(PendingStockExpense).filter(PendingStockExpense.pending_stock_id == pending_stock.id).all()
        
        if pending_stock.komissionnyy:
            full_text = f"✅ <b>Заявка заполнена!</b> 📦 К КОМИССИОННОМУ СТОКУ\n\n"
        else:
            full_text = f"✅ <b>Заявка заполнена!</b> 📦 К СТОКУ\n\n"
        full_text += f"<b>ID:</b> {pending_stock.id}\n"
        full_text += f"<b>Товар:</b> {pending_stock.product_name}\n"
        if pending_stock.reference:
            full_text += f"<b>Референс:</b> {pending_stock.reference}\n"
        if pending_stock.item_year:
            full_text += f"<b>Год изделия:</b> {pending_stock.item_year}\n"
        if pending_stock.komissionnyy:
            full_text += f"<b>Выплата владельцу:</b> {buy_price:.2f}\n"
            full_text += f"<b>Ожидаемая цена:</b> {pending_stock.expected_sell_price:.2f}\n"
            full_text += f"<b>Количество:</b> {pending_stock.quantity}\n"
            full_text += f"<b>Город:</b> {city.name if city else '—'}\n"
            full_text += f"<b>Владелец:</b> {pending_stock.client_full_name if pending_stock.client_full_name else '—'}\n"
            full_text += f"<b>Телефон:</b> {pending_stock.client_phone if pending_stock.client_phone else '—'}\n"
        else:
            full_text += f"<b>Цена покупки:</b> {buy_price:.2f}\n"
            full_text += f"<b>Ожидаемая цена:</b> {pending_stock.expected_sell_price:.2f}\n"
            full_text += f"<b>Количество:</b> {pending_stock.quantity}\n"
            full_text += f"<b>Город:</b> {city.name if city else '—'}\n"
            full_text += f"<b>Инвестор:</b> {investor.name if investor else '—'}\n"
        total_exp = sum(e.amount for e in expenses)
        total_buy = buy_price * pending_stock.quantity
        total_sell = pending_stock.expected_sell_price * pending_stock.quantity
        profit = total_sell - total_buy - total_exp
        full_text += f"\n<b>Ожидаемая прибыль:</b> {profit:.2f}\n"
        
        try:
            if has_photo:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=msg_id,
                    caption=full_text,
                    reply_markup=None,
                    parse_mode="HTML"
                )
            else:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=full_text,
                    reply_markup=None,
                    parse_mode="HTML"
                )
        except Exception:
            await bot.send_message(
                chat_id=chat_id,
                text=full_text,
                parse_mode="HTML"
            )
        
        await message.answer("✅ Цена добавлена. Заявка готова к одобрению на сайте.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        db.rollback()
        await message.answer("❌ Ошибка при сохранении.")
    finally:
        db.close()


@dp.callback_query(F.data.startswith("approve_stock_"))
async def approve_stock(callback: CallbackQuery):
    """Обработчик кнопки 'Принять' для стока - создает StockItem из PendingStock"""
    db = get_db_session()
    
    try:
        pending_stock_id = int(callback.data.split("_")[2])
        
        # Загружаем временную заявку
        pending_stock = db.query(PendingStock).filter(PendingStock.id == pending_stock_id).first()
        if not pending_stock:
            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return
        
        if pending_stock.buy_price is None:
            await callback.answer("❌ Сначала укажите цену покупки", show_alert=True)
            return
        
        # Создаем реальный сток из временной заявки
        new_stock = StockItem(
            photo=pending_stock.photo,
            product_name=pending_stock.product_name,
            reference=pending_stock.reference,
            item_year=pending_stock.item_year,
            buy_price=pending_stock.buy_price,
            expected_sell_price=pending_stock.expected_sell_price,
            quantity=pending_stock.quantity,
            komplektatsiya=getattr(pending_stock, 'komplektatsiya', None),
            komissionnyy=getattr(pending_stock, 'komissionnyy', False),
            city_id=pending_stock.city_id,
            investor_id=pending_stock.investor_id
        )
        db.add(new_stock)
        db.flush()  # Получаем ID нового стока
        
        # Переносим расходы
        pending_expenses = db.query(PendingStockExpense).filter(PendingStockExpense.pending_stock_id == pending_stock.id).all()
        for pending_exp in pending_expenses:
            stock_expense = StockExpense(
                stock_item_id=new_stock.id,
                expense_type_id=pending_exp.expense_type_id,
                amount=pending_exp.amount,
                comment=pending_exp.comment
            )
            db.add(stock_expense)
        
        # Обновляем статус заявки перед удалением
        pending_stock.status = 'approved'
        db.commit()
        
        # Обновляем сообщения для всех администраторов
        await update_telegram_message_for_stock_async(pending_stock_id, 'approved')
        
        # Удаляем временную заявку
        db.delete(pending_stock)
        db.commit()
        
        await callback.answer("✅ Заявка принята и добавлена в базу!")
        
    except Exception as e:
        print(f"Ошибка при принятии заявки на сток: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        await callback.answer("❌ Ошибка при обработке заявки", show_alert=True)
    finally:
        db.close()


@dp.callback_query(F.data.startswith("reject_stock_"))
async def reject_stock(callback: CallbackQuery):
    """Обработчик кнопки 'Отклонить' для стока - удаляет PendingStock"""
    db = get_db_session()
    
    try:
        pending_stock_id = int(callback.data.split("_")[2])
        
        # Загружаем временную заявку
        pending_stock = db.query(PendingStock).filter(PendingStock.id == pending_stock_id).first()
        if not pending_stock:
            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return
        
        # Удаляем фото, если оно было загружено
        if pending_stock.photo:
            uploads_dir = os.path.join(BASE_DIR, "uploads")
            photo_path = os.path.join(uploads_dir, pending_stock.photo)
            if os.path.exists(photo_path):
                try:
                    os.remove(photo_path)
                except:
                    pass
        
        # Обновляем статус заявки перед удалением
        pending_stock.status = 'rejected'
        db.commit()
        
        # Обновляем сообщения для всех администраторов
        await update_telegram_message_for_stock_async(pending_stock_id, 'rejected')
        
        # Удаляем временную заявку (расходы удалятся каскадно)
        db.delete(pending_stock)
        db.commit()
        
        await callback.answer("❌ Заявка отклонена и удалена!")
        
    except Exception as e:
        print(f"Ошибка при отклонении заявки на сток: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        await callback.answer("❌ Ошибка при обработке заявки", show_alert=True)
    finally:
        db.close()


# Глобальная переменная для event loop бота
_bot_loop = None
_bot_loop_thread = None


def send_pending_sale_notification_async(pending_sale_id: int):
    """Синхронная обертка для отправки уведомления через event loop бота"""
    global _bot_loop
    
    print(f"[FLASK] Запрос на отправку уведомления о заявке #{pending_sale_id}")
    print(f"[FLASK] Event loop: {_bot_loop}")
    
    if _bot_loop is None:
        print("[FLASK] ❌ Event loop бота не инициализирован. Бот еще не запущен.")
        print("[FLASK] Добавляю уведомление в очередь...")
        # Добавляем в очередь для обработки ботом
        try:
            from bot_notification_queue import add_notification
            add_notification('sale', pending_sale_id)
            print("[FLASK] ✅ Уведомление добавлено в очередь")
        except Exception as e:
            print(f"[FLASK] ❌ Ошибка при добавлении в очередь: {e}")
        return
    
    if _bot_loop.is_closed():
        print(f"[FLASK] ❌ Event loop бота закрыт. Бот был остановлен.")
        print(f"[FLASK] Добавляю уведомление в очередь...")
        # Добавляем в очередь для обработки ботом
        try:
            from bot_notification_queue import add_notification
            add_notification('sale', pending_sale_id)
            print("[FLASK] ✅ Уведомление добавлено в очередь")
        except Exception as e:
            print(f"[FLASK] ❌ Ошибка при добавлении в очередь: {e}")
        return
    
    print(f"[FLASK] ✅ Event loop доступен: закрыт={_bot_loop.is_closed()}, работает={_bot_loop.is_running()}")
    
    # Создаем задачу в event loop бота
    try:
        import time
        time.sleep(0.5)  # Небольшая задержка для гарантии, что транзакция зафиксирована
        print(f"[FLASK] Планирование отправки уведомления через event loop бота...")
        future = asyncio.run_coroutine_threadsafe(send_pending_sale_notification(pending_sale_id), _bot_loop)
        print(f"[FLASK] ✅ Задача отправки уведомления запланирована")
        # Не ждем результата, чтобы не блокировать Flask
    except Exception as e:
        import traceback
        print(f"[FLASK] ❌ Ошибка при планировании отправки уведомления: {e}")
        traceback.print_exc()


def send_pending_stock_notification_async(pending_stock_id: int):
    """Синхронная обертка для отправки уведомления о стоке через event loop бота"""
    global _bot_loop
    
    if _bot_loop is None or _bot_loop.is_closed():
        print("Event loop бота не доступен. Бот еще не запущен.")
        return
    
    # Создаем задачу в event loop бота
    try:
        import time
        time.sleep(0.5)  # Небольшая задержка для гарантии, что транзакция зафиксирована
        future = asyncio.run_coroutine_threadsafe(send_pending_stock_notification(pending_stock_id), _bot_loop)
        # Не ждем результата, чтобы не блокировать Flask
    except Exception as e:
        import traceback
        print(f"Ошибка при планировании отправки уведомления о стоке: {e}")
        traceback.print_exc()

def send_bonus_notification_to_manager_async(sale_id: int):
    """Синхронная обертка для отправки уведомления менеджеру о бонусе через event loop бота"""
    global _bot_loop
    
    if _bot_loop is None or _bot_loop.is_closed():
        print("Event loop бота не доступен. Бот еще не запущен.")
        return
    
    # Создаем задачу в event loop бота
    try:
        import time
        time.sleep(0.5)  # Небольшая задержка для гарантии, что транзакция зафиксирована
        future = asyncio.run_coroutine_threadsafe(send_bonus_notification_to_manager(sale_id), _bot_loop)
        # Не ждем результата, чтобы не блокировать Flask
    except Exception as e:
        import traceback
        print(f"Ошибка при планировании отправки уведомления о бонусе: {e}")
        traceback.print_exc()


async def send_bonus_notification_to_manager(sale_id: int):
    """Отправить уведомление менеджеру о начисленном бонусе"""
    db = get_db_session()
    
    try:
        sale = db.query(Sale).filter(Sale.id == sale_id).first()
        if not sale or not sale.bonus or sale.bonus <= 0:
            return
        
        # Находим менеджера (Employee) по employee_id
        employee = db.query(Employee).filter(Employee.id == sale.employee_id).first()
        if not employee:
            return
        
        # Находим активного менеджера в боте
        manager_bot = db.query(BotAdmin).filter(
            BotAdmin.is_active == True,
            BotAdmin.is_manager == True,
            BotAdmin.employee_id == employee.id
        ).first()
        
        if not manager_bot:
            return
        
        # Вычисляем текущий баланс бонусов за месяц
        sale_date = sale.date.date() if isinstance(sale.date, datetime) else sale.date
        month_start = date(sale_date.year, sale_date.month, 1)
        month_end = month_start + relativedelta(months=1)
        
        # Получаем все бонусы менеджера за этот месяц
        monthly_bonuses = db.query(Sale).filter(
            Sale.employee_id == employee.id,
            Sale.date >= month_start,
            Sale.date < month_end,
            Sale.bonus > 0
        ).all()
        
        total_monthly_bonus = sum(s.bonus for s in monthly_bonuses)
        
        # Формируем сообщение
        months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                     'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
        month_name = months_ru[sale_date.month - 1]
        
        initial_bonus = (sale.initial_bonus or 0.0)
        added_bonus = sale.bonus - initial_bonus
        message_text = f"💰 <b>Бонус начислен!</b>\n\n"
        message_text += f"<b>Товар:</b> {sale.product_name}\n"
        if sale.reference:
            message_text += f"<b>Референс:</b> {sale.reference}\n"
        message_text += f"<b>Изначальный бонус:</b> {initial_bonus:.2f}$\n"
        if added_bonus > 0:
            message_text += f"Вы получили дополнительный бонус в размере {added_bonus:.2f}$\n"
        message_text += f"<b>Общий бонус:</b> {sale.bonus:.2f}$\n\n"
        message_text += f"📊 <b>Баланс за {month_name} {sale_date.year}: {total_monthly_bonus:.2f}$</b>"
        
        await bot.send_message(
            chat_id=manager_bot.chat_id,
            text=message_text,
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления о бонусе менеджеру: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


def send_extra_bonus_notification_async(sale_id: int, employee_id: int, amount: float):
    """Синхронная обертка: отправить уведомление менеджеру о начисленном доп. бонусе"""
    global _bot_loop
    if _bot_loop is None or _bot_loop.is_closed():
        return
    try:
        import time
        time.sleep(0.3)
        future = asyncio.run_coroutine_threadsafe(
            send_extra_bonus_notification_to_manager(sale_id, employee_id, amount), _bot_loop
        )
    except Exception as e:
        print(f"Ошибка при планировании отправки уведомления о доп. бонусе: {e}")


async def send_extra_bonus_notification_to_manager(sale_id: int, employee_id: int, amount: float):
    """Отправить уведомление менеджеру о начисленном доп. бонусе по продаже"""
    db = get_db_session()
    try:
        sale = db.query(Sale).filter(Sale.id == sale_id).first()
        if not sale:
            return
        employee = db.query(Employee).filter(Employee.id == employee_id).first()
        if not employee:
            return
        manager_bot = db.query(BotAdmin).filter(
            BotAdmin.is_active == True,
            BotAdmin.is_manager == True,
            BotAdmin.employee_id == employee.id
        ).first()
        if not manager_bot:
            return
        sale_date = sale.date.date() if isinstance(sale.date, datetime) else sale.date
        month_start = date(sale_date.year, sale_date.month, 1)
        month_end = month_start + relativedelta(months=1)
        monthly_main = db.query(Sale).filter(
            Sale.employee_id == employee.id,
            Sale.date >= month_start,
            Sale.date < month_end,
            Sale.bonus > 0
        ).all()
        monthly_extra = db.query(SaleAdditionalBonus).join(Sale).filter(
            SaleAdditionalBonus.employee_id == employee.id,
            Sale.date >= month_start,
            Sale.date < month_end
        ).all()
        total_monthly = sum(s.bonus for s in monthly_main) + sum(e.amount for e in monthly_extra)
        months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                     'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
        month_name = months_ru[sale_date.month - 1]
        message_text = (
            f"💰 <b>Доп. бонус начислен!</b>\n\n"
            f"<b>Товар:</b> {sale.product_name}\n"
        )
        if sale.reference:
            message_text += f"<b>Референс:</b> {sale.reference}\n"
        message_text += f"<b>Сумма доп. бонуса:</b> {amount:.2f}$\n\n"
        message_text += f"📊 <b>Баланс за {month_name} {sale_date.year}: {total_monthly:.2f}$</b>"
        await bot.send_message(
            chat_id=manager_bot.chat_id,
            text=message_text,
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления о доп. бонусе: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


def send_manual_bonus_notification_async(manual_bonus_id: int):
    """Синхронная обёртка: уведомить менеджера о ручном бонусе от администратора"""
    global _bot_loop
    if _bot_loop is None or _bot_loop.is_closed():
        return
    try:
        import time
        time.sleep(0.3)
        asyncio.run_coroutine_threadsafe(
            _send_manual_bonus_notification(manual_bonus_id), _bot_loop
        )
    except Exception as e:
        print(f"Ошибка при планировании уведомления о ручном бонусе: {e}")


async def _send_manual_bonus_notification(manual_bonus_id: int):
    """Отправить менеджеру уведомление о ручном бонусе"""
    db = get_db_session()
    try:
        mb = db.query(ManualBonus).filter(ManualBonus.id == manual_bonus_id).first()
        if not mb:
            return
        employee = db.query(Employee).filter(Employee.id == mb.employee_id).first()
        if not employee:
            return
        manager_bot = db.query(BotAdmin).filter(
            BotAdmin.is_active == True,
            BotAdmin.is_manager == True,
            BotAdmin.employee_id == employee.id
        ).first()
        if not manager_bot:
            return
        bonus_date = mb.date.date() if hasattr(mb.date, 'date') and callable(getattr(mb.date, 'date')) else mb.date
        month_start = date(bonus_date.year, bonus_date.month, 1)
        month_end = month_start + relativedelta(months=1)
        # Общий баланс за месяц (основные + ручные)
        monthly_main = db.query(Sale).filter(
            Sale.employee_id == employee.id,
            Sale.date >= month_start,
            Sale.date < month_end,
            Sale.bonus > 0
        ).all()
        monthly_extra = db.query(SaleAdditionalBonus).join(Sale).filter(
            SaleAdditionalBonus.employee_id == employee.id,
            Sale.date >= month_start,
            Sale.date < month_end
        ).all()
        monthly_manual = db.query(ManualBonus).filter(
            ManualBonus.employee_id == employee.id,
            ManualBonus.date >= month_start,
            ManualBonus.date < month_end
        ).all()
        total_monthly = (
            sum(s.bonus for s in monthly_main)
            + sum(e.amount for e in monthly_extra)
            + sum(m.amount for m in monthly_manual)
        )
        months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                     'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
        month_name = months_ru[bonus_date.month - 1]
        message_text = (
            f"💰 <b>Вам начислен бонус!</b>\n\n"
            f"<b>Сумма:</b> {mb.amount:.2f}$\n"
            f"<b>Дата:</b> {bonus_date.strftime('%d.%m.%Y')}\n"
        )
        if mb.comment:
            message_text += f"<b>Комментарий:</b> {mb.comment}\n"
        message_text += f"\n📊 <b>Баланс за {month_name} {bonus_date.year}: {total_monthly:.2f}$</b>"
        await bot.send_message(
            chat_id=manager_bot.chat_id,
            text=message_text,
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления о ручном бонусе: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


def _client_contacts_text(client):
    """Форматировать контакты клиента для сообщения (как display_contacts в app)."""
    if not client:
        return ""
    lines = []
    if client.phone:
        lines.append(f"Телефон: {client.phone}")
    if client.instagram:
        lines.append(f"Инст: {client.instagram}")
    if client.telegram:
        lines.append(f"Тг: {client.telegram}")
    if client.email:
        lines.append(f"Почта: {client.email}")
    return "\n".join(lines) if lines else ""


def _build_month_reminder_text(sale, db_session):
    """Собрать текст напоминания «месяц после продажи» для одной продажи."""
    sale_date = sale.date.date() if hasattr(sale.date, 'date') and callable(getattr(sale.date, 'date')) else (sale.date if isinstance(sale.date, date) else date.today())
    months_ru = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
    date_str = f"{sale_date.day} {months_ru[sale_date.month - 1]} {sale_date.year}"
    text = (
        "📅 <b>Прошёл месяц после продажи — свяжитесь с клиентом</b>\n\n"
        f"<b>Продажа:</b> {sale.product_name}\n"
    )
    if sale.reference:
        text += f"Референс: {sale.reference}\n"
    text += f"Дата продажи: {date_str}\n"
    if sale.sell_price:
        text += f"Сумма продажи: {sale.sell_price:.0f}$\n"
    client = db_session.query(Client).filter(Client.id == sale.client_id).first() if sale.client_id else None
    if client:
        text += f"\n<b>Клиент:</b> {client.full_name}\n"
        contacts = _client_contacts_text(client)
        if contacts:
            text += f"<b>Контакты для связи:</b>\n<code>{contacts}</code>"
    else:
        text += "\nКлиент не указан в заявке."
    return text


async def send_month_reminder_for_sale(sale_id: int):
    """Отправить менеджеру напоминание: прошёл месяц после продажи, контакты клиента для связи."""
    db = get_db_session()
    try:
        sale = db.query(Sale).filter(Sale.id == sale_id).first()
        if not sale:
            return
        employee = db.query(Employee).filter(Employee.id == sale.employee_id).first()
        if not employee:
            return
        manager_bot = db.query(BotAdmin).filter(
            BotAdmin.is_active == True,
            BotAdmin.is_manager == True,
            BotAdmin.employee_id == employee.id
        ).first()
        if not manager_bot:
            return
        text = _build_month_reminder_text(sale, db)
        await bot.send_message(chat_id=manager_bot.chat_id, text=text, parse_mode="HTML")
        sale.month_reminder_sent_at = datetime.utcnow()
        db.commit()
    except Exception as e:
        print(f"Ошибка при отправке напоминания «месяц после продажи»: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


async def check_and_send_month_reminders():
    """Найти продажи ровно месячной давности (по дате) и отправить менеджерам напоминания."""
    db = get_db_session()
    try:
        today = date.today()
        one_month_ago = today - relativedelta(months=1)
        sales = db.query(Sale).filter(Sale.month_reminder_sent_at.is_(None)).all()
        to_send_ids = []
        for s in sales:
            sd = s.date.date() if hasattr(s.date, 'date') and callable(getattr(s.date, 'date')) else (s.date if isinstance(s.date, date) else None)
            if sd == one_month_ago:
                to_send_ids.append(s.id)
        db.close()
        for sid in to_send_ids:
            await send_month_reminder_for_sale(sid)
        return
    except Exception as e:
        print(f"Ошибка при проверке напоминаний «месяц после продажи»: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


async def run_month_reminder_scheduler():
    """Раз в сутки проверять и отправлять напоминания «месяц после продажи»."""
    await asyncio.sleep(60)  # даём боту запуститься
    while True:
        try:
            await check_and_send_month_reminders()
        except Exception as e:
            print(f"[BOT] Ошибка в планировщике напоминаний: {e}")
        await asyncio.sleep(24 * 3600)  # раз в сутки


def _build_birthday_reminder_text(client):
    """Текст уведомления о дне рождения клиента: контакты и напоминание написать."""
    contacts = _client_contacts_text(client)
    text = (
        f"🎂 <b>День рождения клиента</b>\n\n"
        f"<b>{client.full_name}</b>\n\n"
    )
    if contacts:
        text += f"<b>Контакты для связи:</b>\n<code>{contacts}</code>\n\n"
    text += "💬 <b>Напишите клиенту, поздравьте с днём рождения!</b>"
    return text


async def check_and_send_birthday_reminders():
    """Найти клиентов с днём рождения сегодня (по МСК) и отправить уведомление всем менеджерам."""
    db = get_db_session()
    try:
        today = _today_msk()
        clients = db.query(Client).filter(Client.birth_date != None).all()
        def to_date(bd):
            if bd is None:
                return None
            if hasattr(bd, 'date') and callable(getattr(bd, 'date')):
                return bd.date()
            if isinstance(bd, date):
                return bd
            if isinstance(bd, str):
                try:
                    return datetime.strptime(bd[:10], '%Y-%m-%d').date()
                except Exception:
                    return None
            return None

        birthday_today = []
        for c in clients:
            bd_date = to_date(c.birth_date)
            if bd_date and bd_date.month == today.month and bd_date.day == today.day:
                birthday_today.append(c)
        managers = db.query(BotAdmin).filter(BotAdmin.is_active == True, BotAdmin.is_manager == True).all()
        db.close()
        for client in birthday_today:
            text = _build_birthday_reminder_text(client)
            for manager in managers:
                try:
                    await bot.send_message(chat_id=manager.chat_id, text=text, parse_mode="HTML")
                except Exception as e:
                    print(f"[BOT] Не удалось отправить уведомление о ДР менеджеру {manager.chat_id}: {e}")
    except Exception as e:
        print(f"Ошибка при проверке дней рождения: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


async def run_birthday_scheduler():
    """Раз в сутки проверять дни рождения клиентов и отправлять уведомления менеджерам."""
    await asyncio.sleep(60)
    while True:
        try:
            await check_and_send_birthday_reminders()
        except Exception as e:
            print(f"[BOT] Ошибка в планировщике дней рождения: {e}")
        await asyncio.sleep(24 * 3600)


async def update_telegram_message_for_sale_async(pending_sale_id: int, status: str):
    """Обновить сообщения в Telegram для всех администраторов при изменении статуса заявки на продажу"""
    db = get_db_session()
    
    try:
        # Получаем заявку
        pending_sale = db.query(PendingSale).filter(PendingSale.id == pending_sale_id).first()
        if not pending_sale:
            return
        
        # Получаем всех активных администраторов (не менеджеров)
        admins = db.query(BotAdmin).filter(BotAdmin.is_active == True, BotAdmin.is_manager == False).all()
        
        status_text = "✅ ЗАЯВКА ПРИНЯТА И ДОБАВЛЕНА В БАЗУ" if status == 'approved' else "❌ ЗАЯВКА ОТКЛОНЕНА И УДАЛЕНА"
        
        # Обновляем сообщение для всех администраторов
        for admin in admins:
            try:
                # Пытаемся обновить сообщение, если оно было отправлено этому администратору
                message_updated = False
                if pending_sale.telegram_chat_id == admin.chat_id and pending_sale.telegram_message_id:
                    try:
                        # Пытаемся обновить существующее сообщение
                        if pending_sale.photo:
                            # Для фото пытаемся обновить caption
                            await bot.edit_message_caption(
                                chat_id=admin.chat_id,
                                message_id=pending_sale.telegram_message_id,
                                caption=f"<b>{status_text}</b>",
                                parse_mode="HTML",
                                reply_markup=None
                            )
                            message_updated = True
                        else:
                            await bot.edit_message_text(
                                chat_id=admin.chat_id,
                                message_id=pending_sale.telegram_message_id,
                                text=f"<b>{status_text}</b>",
                                parse_mode="HTML",
                                reply_markup=None
                            )
                            message_updated = True
                    except Exception as e:
                        # Если не удалось обновить, отправляем новое сообщение
                        print(f"Не удалось обновить сообщение для {admin.chat_id}: {e}")
                        message_updated = False
                
                # Если сообщение не было обновлено, отправляем уведомление
                if not message_updated:
                    try:
                        await bot.send_message(
                            chat_id=admin.chat_id,
                            text=f"🔄 <b>Обновление статуса заявки #{pending_sale_id}</b>\n\nТовар: {pending_sale.product_name}\n\n<b>{status_text}</b>",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        print(f"Не удалось отправить уведомление администратору {admin.chat_id}: {e}")
            except Exception as e:
                print(f"Ошибка при обновлении сообщения для администратора {admin.chat_id}: {e}")
    except Exception as e:
        print(f"Ошибка при обновлении сообщений в Telegram: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


async def update_telegram_message_for_stock_async(pending_stock_id: int, status: str):
    """Обновить сообщения в Telegram для всех администраторов при изменении статуса заявки на сток"""
    db = get_db_session()
    
    try:
        # Получаем заявку
        pending_stock = db.query(PendingStock).filter(PendingStock.id == pending_stock_id).first()
        if not pending_stock:
            return
        
        # Получаем всех активных администраторов (не менеджеров)
        admins = db.query(BotAdmin).filter(BotAdmin.is_active == True, BotAdmin.is_manager == False).all()
        
        if status == 'completed':
            # Заявка дополнена на сайте — обновляем сообщение: полная информация + кнопки Принять/Отклонить
            city = db.query(City).filter(City.id == pending_stock.city_id).first()
            investor = db.query(Investor).filter(Investor.id == pending_stock.investor_id).first()
            expenses = db.query(PendingStockExpense).filter(PendingStockExpense.pending_stock_id == pending_stock.id).all()
            expense_types = {et.id: et.name for et in db.query(ExpenseType).all()}
            
            if pending_stock.komissionnyy:
                message_text = f"✅ <b>Заявка дополнена на сайте</b> 📦 К КОМИССИОННОМУ СТОКУ\n\n"
            else:
                message_text = f"✅ <b>Заявка дополнена на сайте</b> 📦 К СТОКУ\n\n"
            message_text += f"<b>ID:</b> {pending_stock.id}\n"
            message_text += f"<b>Товар:</b> {pending_stock.product_name}\n"
            if pending_stock.reference:
                message_text += f"<b>Референс:</b> {pending_stock.reference}\n"
            if pending_stock.komissionnyy:
                message_text += f"<b>Выплата владельцу:</b> {pending_stock.buy_price:.2f}\n"
                message_text += f"<b>Ожидаемая цена:</b> {pending_stock.expected_sell_price:.2f}\n"
                message_text += f"<b>Количество:</b> {pending_stock.quantity}\n"
                message_text += f"<b>Город:</b> {city.name if city else '—'}\n"
                message_text += f"<b>Владелец:</b> {pending_stock.client_full_name if pending_stock.client_full_name else '—'}\n"
                message_text += f"<b>Телефон:</b> {pending_stock.client_phone if pending_stock.client_phone else '—'}\n"
            else:
                message_text += f"<b>Цена покупки:</b> {pending_stock.buy_price:.2f}\n"
                message_text += f"<b>Ожидаемая цена:</b> {pending_stock.expected_sell_price:.2f}\n"
                message_text += f"<b>Количество:</b> {pending_stock.quantity}\n"
                message_text += f"<b>Город:</b> {city.name if city else '—'}\n"
                message_text += f"<b>Инвестор:</b> {investor.name if investor else '—'}\n"
            total_exp = sum(e.amount for e in expenses)
            total_buy = pending_stock.buy_price * pending_stock.quantity
            total_sell = pending_stock.expected_sell_price * pending_stock.quantity
            message_text += f"\n<b>Ожидаемая прибыль:</b> {total_sell - total_buy - total_exp:.2f}\n"
            
            keyboard = None
            
            for admin in admins:
                try:
                    if pending_stock.telegram_chat_id == admin.chat_id and pending_stock.telegram_message_id:
                        try:
                            uploads_dir = os.path.join(BASE_DIR, "uploads")
                            if pending_stock.photo and os.path.exists(os.path.join(uploads_dir, pending_stock.photo)):
                                await bot.edit_message_caption(
                                    chat_id=admin.chat_id, message_id=pending_stock.telegram_message_id,
                                    caption=message_text, parse_mode="HTML", reply_markup=keyboard
                                )
                            else:
                                await bot.edit_message_text(
                                    chat_id=admin.chat_id, message_id=pending_stock.telegram_message_id,
                                    text=message_text, parse_mode="HTML", reply_markup=keyboard
                                )
                        except Exception as e:
                            print(f"Не удалось обновить сообщение: {e}")
                except Exception as e:
                    print(f"Ошибка при обновлении для {admin.chat_id}: {e}")
            db.close()
            return
        
        status_text = "✅ ЗАЯВКА ПРИНЯТА И ДОБАВЛЕНА В БАЗУ" if status == 'approved' else "❌ ЗАЯВКА ОТКЛОНЕНА И УДАЛЕНА"
        
        # Обновляем сообщение для всех администраторов
        for admin in admins:
            try:
                # Пытаемся обновить сообщение, если оно было отправлено этому администратору
                if pending_stock.telegram_chat_id == admin.chat_id and pending_stock.telegram_message_id:
                    try:
                        # Пытаемся обновить существующее сообщение
                        if pending_stock.photo:
                            await bot.edit_message_caption(
                                chat_id=admin.chat_id,
                                message_id=pending_stock.telegram_message_id,
                                caption=f"<b>{status_text}</b>",
                                parse_mode="HTML",
                                reply_markup=None
                            )
                        else:
                            await bot.edit_message_text(
                                chat_id=admin.chat_id,
                                message_id=pending_stock.telegram_message_id,
                                text=f"<b>{status_text}</b>",
                                parse_mode="HTML",
                                reply_markup=None
                            )
                    except Exception as e:
                        # Если сообщение не найдено, отправляем новое уведомление
                        print(f"Не удалось обновить сообщение для {admin.chat_id}: {e}")
                        try:
                            await bot.send_message(
                                chat_id=admin.chat_id,
                                text=f"🔄 <b>Обновление статуса заявки #{pending_stock_id}</b>\n\nТовар: {pending_stock.product_name}\n\n<b>{status_text}</b>",
                                parse_mode="HTML"
                            )
                        except:
                            pass
                else:
                    # Если сообщение было отправлено другому администратору, отправляем уведомление
                    try:
                        await bot.send_message(
                            chat_id=admin.chat_id,
                            text=f"🔄 <b>Обновление статуса заявки #{pending_stock_id}</b>\n\nТовар: {pending_stock.product_name}\n\n<b>{status_text}</b>",
                            parse_mode="HTML"
                        )
                    except:
                        pass
            except Exception as e:
                print(f"Ошибка при обновлении сообщения для администратора {admin.chat_id}: {e}")
    except Exception as e:
        print(f"Ошибка при обновлении сообщений в Telegram: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


def update_telegram_message_for_sale(pending_sale_id: int, status: str):
    """Синхронная обертка для обновления сообщения о продаже"""
    global _bot_loop
    
    if _bot_loop is None or _bot_loop.is_closed():
        print("Event loop бота не доступен.")
        return
    
    try:
        future = asyncio.run_coroutine_threadsafe(update_telegram_message_for_sale_async(pending_sale_id, status), _bot_loop)
    except Exception as e:
        import traceback
        print(f"Ошибка при планировании обновления сообщения: {e}")
        traceback.print_exc()


def update_telegram_message_for_stock(pending_stock_id: int, status: str):
    """Синхронная обертка для обновления сообщения о стоке"""
    global _bot_loop
    
    if _bot_loop is None or _bot_loop.is_closed():
        print("Event loop бота не доступен.")
        return
    
    try:
        future = asyncio.run_coroutine_threadsafe(update_telegram_message_for_stock_async(pending_stock_id, status), _bot_loop)
    except Exception as e:
        import traceback
        print(f"Ошибка при планировании обновления сообщения: {e}")
        traceback.print_exc()


async def check_notification_queue():
    """Периодически проверяет очередь уведомлений и обрабатывает их"""
    print("[BOT] Запущена задача проверки очереди уведомлений")
    while True:
        try:
            await asyncio.sleep(2)  # Проверяем каждые 2 секунды
            
            from bot_notification_queue import get_pending_notifications, remove_notification
            
            notifications = get_pending_notifications()
            if notifications:
                print(f"[BOT] Найдено уведомлений в очереди: {len(notifications)}")
            
            for notif in notifications:
                try:
                    notif_type = notif.get('type')
                    notif_id = notif.get('id')
                    notif_file = notif.get('file')
                    
                    if notif_type == 'sale':
                        print(f"[BOT] Обработка уведомления о продаже #{notif_id} из очереди")
                        await send_pending_sale_notification(notif_id)
                        remove_notification(notif_file)
                        print(f"[BOT] ✅ Уведомление о продаже #{notif_id} обработано")
                    elif notif_type == 'stock':
                        print(f"[BOT] Обработка уведомления о стоке #{notif_id} из очереди")
                        await send_pending_stock_notification(notif_id)
                        remove_notification(notif_file)
                        print(f"[BOT] ✅ Уведомление о стоке #{notif_id} обработано")
                except Exception as e:
                    print(f"[BOT] ❌ Ошибка при обработке уведомления из очереди: {e}")
                    import traceback
                    traceback.print_exc()
                    # Удаляем файл даже при ошибке, чтобы не зациклиться
                    try:
                        remove_notification(notif.get('file'))
                    except:
                        pass
        except Exception as e:
            print(f"[BOT] ❌ Ошибка в задаче проверки очереди: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(5)  # При ошибке ждем дольше


async def main():
    """Запуск бота"""
    global _bot_loop
    print("=" * 50)
    print("Запуск телеграм бота...")
    print("=" * 50)
    # Используем текущий event loop (уже установлен в main.py)
    current_loop = asyncio.get_event_loop()
    if _bot_loop is None:
        _bot_loop = current_loop
        print(f"[BOT] ✅ Event loop установлен: {_bot_loop}")
    else:
        print(f"[BOT] ✅ Event loop уже установлен: {_bot_loop}")
    print(f"[BOT] Event loop закрыт: {_bot_loop.is_closed() if _bot_loop else 'N/A'}")
    print(f"[BOT] Event loop работает: {_bot_loop.is_running() if _bot_loop else 'N/A'}")
    
    # Патчим add_signal_handler на уровне текущего event loop
    # Это нужно для работы в отдельном потоке
    loop = asyncio.get_event_loop()
    original_add_signal_handler = loop.add_signal_handler
    
    def patched_add_signal_handler(sig, callback, *args):
        """Патч для отключения signal handlers в отдельном потоке"""
        try:
            return original_add_signal_handler(sig, callback, *args)
        except (ValueError, RuntimeError) as e:
            if "set_wakeup_fd" in str(e) or "signal" in str(e).lower() or "main thread" in str(e).lower():
                # Игнорируем ошибку - signal handlers не нужны в отдельном потоке
                print(f"Предупреждение: signal handlers отключены (работаем в отдельном потоке)")
                return None
            raise
    
    # Применяем патч к текущему loop
    loop.add_signal_handler = patched_add_signal_handler
    
    # Запускаем задачу для проверки очереди уведомлений
    asyncio.create_task(check_notification_queue())
    # Напоминания менеджерам «месяц после продажи» — раз в сутки
    asyncio.create_task(run_month_reminder_scheduler())
    # Уведомления менеджерам о днях рождения клиентов — раз в сутки
    asyncio.create_task(run_birthday_scheduler())
    
    try:
        # Запускаем polling - теперь signal handlers будут игнорироваться
        await dp.start_polling(bot, close_loop=False, skip_updates=True)
    except (RuntimeError, ValueError) as e:
        error_str = str(e).lower()
        if "set_wakeup_fd" in error_str or "signal" in error_str or "main thread" in error_str:
            # Если все еще ошибка, используем альтернативный метод - ручной polling
            print("Используем альтернативный метод запуска polling (без signal handlers)...")
            await bot.delete_webhook(drop_pending_updates=True)
            # Запускаем polling вручную через get_updates
            offset = None
            while True:
                try:
                    updates = await bot.get_updates(offset=offset, timeout=30, allowed_updates=[])
                    if updates:
                        for update in updates:
                            offset = update.update_id + 1
                            await dp.feed_update(bot, update)
                except Exception as err:
                    print(f"Ошибка при получении обновлений: {err}")
                    import traceback
                    traceback.print_exc()
                    await asyncio.sleep(5)
        else:
            print(f"Ошибка при запуске бота: {e}")
            import traceback
            traceback.print_exc()
            raise
    except Exception as e:
        print(f"Ошибка при запуске бота: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    asyncio.run(main())
