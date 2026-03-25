from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_, extract
from sqlalchemy.orm import joinedload
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from functools import wraps
import os
import asyncio
import threading
import uuid
import hmac
import hashlib
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.config['SECRET_KEY'] = 'your_secret_key_here'
# Используем sales.db в папке приложения (та же база, где инвесторы и остальные данные)
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{os.path.join(BASE_DIR, "sales.db")}'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['ALLOWED_EXTENSIONS'] = {'png', 'jpg', 'jpeg', 'gif', 'webp', 'bmp'}

db = SQLAlchemy(app)


class City(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)

    def __repr__(self):
        return f'<City {self.name}>'


class ExpenseType(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)

    def __repr__(self):
        return f'<ExpenseType {self.name}>'


class Employee(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)

    def __repr__(self):
        return f'<Employee {self.name}>'


PRODUCT_CATEGORIES = [
    ('watches', 'Часы'),
    ('jewelry', 'Ювелирные изделия'),
    ('bags', 'Сумки'),
    ('accessories', 'Аксессуары'),
]


class Sale(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    photo = db.Column(db.String(200))
    product_name = db.Column(db.String(200), nullable=False)
    reference = db.Column(db.String(200))
    item_year = db.Column(db.Integer, nullable=True)  # Год изделия (необязательное)
    buy_price = db.Column(db.Float, nullable=False)
    sell_price = db.Column(db.Float, nullable=False)
    bonus = db.Column(db.Float, default=0.0)  # Бонус менеджера за продажу (итого)
    initial_bonus = db.Column(db.Float, default=0.0)  # Бонус, указанный менеджером при создании заявки
    murad_bonus = db.Column(db.Float, default=0.0, nullable=False)  # Бонус Мурада по Москве, 100$
    komplektatsiya = db.Column('komplektatsiya', db.String(50), nullable=True)  # Комплектация: Полный комплект, только часы, только коробка, только документы
    komissionnyy = db.Column(db.Boolean, default=False, nullable=False)  # Комиссионный товар
    category = db.Column(db.String(50), nullable=True)  # watches, jewelry, bags, accessories
    city_id = db.Column(db.Integer, db.ForeignKey('city.id'), nullable=False)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    investor_id = db.Column(db.Integer, db.ForeignKey('investor.id'), nullable=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=True)
    date = db.Column(db.DateTime, default=datetime.utcnow)
    month_reminder_sent_at = db.Column(db.DateTime, nullable=True)  # Напоминание менеджеру "месяц после продажи" отправлено
    comment = db.Column(db.Text, nullable=True)  # Комментарий

    city = db.relationship('City', backref='sales')
    employee = db.relationship('Employee', backref='sales')
    investor = db.relationship('Investor', backref='sales')
    client = db.relationship('Client', backref='sales')
    expenses = db.relationship('Expense', backref='sale', lazy=True, cascade="all, delete-orphan")
    additional_bonuses = db.relationship('SaleAdditionalBonus', backref='sale', lazy=True, cascade="all, delete-orphan", foreign_keys='SaleAdditionalBonus.sale_id')

    @property
    def profit(self):
        total_expenses = sum(e.amount for e in self.expenses)
        bonus = self.bonus if self.bonus else 0.0
        murad = getattr(self, 'murad_bonus', 0) or 0.0
        extra = sum(ab.amount for ab in self.additional_bonuses) if getattr(self, 'additional_bonuses', None) else 0.0
        return self.sell_price - self.buy_price - total_expenses - bonus - murad - extra


class Expense(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sale_id = db.Column(db.Integer, db.ForeignKey('sale.id'), nullable=False)
    expense_type_id = db.Column(db.Integer, db.ForeignKey('expense_type.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    comment = db.Column(db.String(200))

    expense_type = db.relationship('ExpenseType', backref='expenses')


class GeneralExpense(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    expense_type_id = db.Column(db.Integer, db.ForeignKey('expense_type.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    date = db.Column(db.DateTime, default=datetime.utcnow)
    city_id = db.Column(db.Integer, db.ForeignKey('city.id'))
    description = db.Column(db.String(200))

    expense_type = db.relationship('ExpenseType', backref='general_expenses')
    city = db.relationship('City', backref='general_expenses')


class StockExpense(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    stock_item_id = db.Column(db.Integer, db.ForeignKey('stock_item.id'), nullable=False)
    expense_type_id = db.Column(db.Integer, db.ForeignKey('expense_type.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    comment = db.Column(db.String(200))

    expense_type = db.relationship('ExpenseType', backref='stock_expenses')
    stock_item = db.relationship('StockItem', backref='expenses')


class Investor(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    full_name = db.Column(db.String(200), nullable=True)   # ФИО
    phone = db.Column(db.String(50), nullable=True)         # Телефон
    email = db.Column(db.String(120), nullable=True)        # Почта
    is_commission_client = db.Column(db.Boolean, default=False, nullable=False)  # Комиссионный клиент

    def __repr__(self):
        return f'<Investor {self.name}>'


class Client(db.Model):
    """Клиент: ФИО + контакты (телефон, инст, тг, почта). ФИО и хотя бы один контакт обязательны."""
    id = db.Column(db.Integer, primary_key=True)
    full_name = db.Column(db.String(200), nullable=False)  # ФИО
    phone = db.Column(db.String(50), nullable=True)
    instagram = db.Column(db.String(100), nullable=True)  # Инст
    telegram = db.Column(db.String(100), nullable=True)  # Тг
    email = db.Column(db.String(120), nullable=True)
    birth_date = db.Column(db.Date, nullable=True)  # Дата рождения
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<Client {self.full_name}>'

    def display_contacts(self):
        """Список заполненных контактов для отображения"""
        contacts = []
        if self.phone:
            contacts.append(('Телефон', self.phone))
        if self.instagram:
            contacts.append(('Инст', self.instagram))
        if self.telegram:
            contacts.append(('Тг', self.telegram))
        if self.email:
            contacts.append(('Почта', self.email))
        return contacts


class StockItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    city_id = db.Column(db.Integer, db.ForeignKey('city.id'), nullable=False)
    investor_id = db.Column(db.Integer, db.ForeignKey('investor.id'), nullable=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=True)  # Для комиссионного стока (старое поле, оставляем для совместимости)
    # Поля для комиссионного стока (ФИО и контакты клиента)
    client_full_name = db.Column(db.String(200), nullable=True)  # ФИО клиента для комиссионного стока
    client_phone = db.Column(db.String(50), nullable=True)
    client_instagram = db.Column(db.String(100), nullable=True)
    client_telegram = db.Column(db.String(100), nullable=True)
    client_email = db.Column(db.String(120), nullable=True)
    product_name = db.Column(db.String(200), nullable=False)
    reference = db.Column(db.String(200))
    item_year = db.Column(db.Integer, nullable=True)  # Год изделия (необязательное)
    buy_price = db.Column(db.Float, nullable=True)  # Покупка для общего стока, выплата для комиссионного
    expected_sell_price = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Integer, default=1)
    photo = db.Column(db.String(200))
    komplektatsiya = db.Column('komplektatsiya', db.String(50), nullable=True)  # Комплектация: Полный комплект, только часы, только коробка, только документы
    komissionnyy = db.Column(db.Boolean, default=False, nullable=False)  # Комиссионный товар
    category = db.Column(db.String(50), nullable=True)
    date_added = db.Column(db.DateTime, default=datetime.utcnow)
    sold = db.Column(db.Boolean, default=False, nullable=False)
    comment = db.Column(db.Text, nullable=True)  # Комментарий

    city = db.relationship('City', backref='stock_items')
    investor = db.relationship('Investor', backref='stock_items')
    client = db.relationship('Client', backref='stock_items')

    @property
    def total_invested(self):
        """Сумма вложений; если покупка не указана — не участвует в расчётах (0)."""
        if self.buy_price is None:
            return 0
        return self.buy_price * self.quantity

    @property
    def expected_profit(self):
        """Ожидаемая прибыль; если покупка не указана — 0."""
        if self.buy_price is None:
            return 0
        return (self.expected_sell_price - self.buy_price) * self.quantity

    def needs_completion(self):
        """Нужно дозаполнить: нет цены покупки/выплаты или (общий сток без инвестора)."""
        if self.buy_price is None:
            return True
        if not self.komissionnyy and self.investor_id is None:
            return True
        return False


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)
    is_investor = db.Column(db.Boolean, default=False, nullable=False)  # Роль инвестора
    is_top_manager = db.Column(db.Boolean, default=False, nullable=False)  # Роль топ-менеджера
    is_service = db.Column(db.Boolean, default=False, nullable=False)  # Роль сервиса/ремонта
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    def __repr__(self):
        return f'<User {self.username}>'


class PendingSale(db.Model):
    """Временная заявка на продажу, ожидающая одобрения"""
    id = db.Column(db.Integer, primary_key=True)
    photo = db.Column(db.String(200))
    product_name = db.Column(db.String(200), nullable=False)
    reference = db.Column(db.String(200))
    item_year = db.Column(db.Integer, nullable=True)  # Год изделия (необязательное)
    buy_price = db.Column(db.Float, nullable=False)  # 0 = не указано (менеджер не ввёл), админ допишет на сайте/в боте
    sell_price = db.Column(db.Float, nullable=False)
    bonus = db.Column(db.Float, default=0.0)  # Бонус менеджера за продажу (итого)
    initial_bonus = db.Column(db.Float, default=0.0)  # Бонус, указанный менеджером при создании заявки
    murad_bonus = db.Column(db.Float, default=0.0, nullable=False)  # Бонус Мурада по Москве, 100$
    komplektatsiya = db.Column('komplektatsiya', db.String(50), nullable=True)  # Комплектация: Полный комплект, только часы, только коробка, только документы
    komissionnyy = db.Column(db.Boolean, default=False, nullable=False)  # Комиссионный товар
    category = db.Column(db.String(50), nullable=True)
    city_id = db.Column(db.Integer, db.ForeignKey('city.id'), nullable=False)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    investor_id = db.Column(db.Integer, db.ForeignKey('investor.id'), nullable=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=True)
    date = db.Column(db.DateTime, default=datetime.utcnow)
    stock_id = db.Column(db.Integer, db.ForeignKey('stock_item.id'), nullable=True)  # Если продажа из стока
    telegram_message_id = db.Column(db.Integer, nullable=True)
    telegram_chat_id = db.Column(db.BigInteger, nullable=True)
    status = db.Column(db.String(20), default='pending', nullable=False)  # pending, approved, rejected
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    city = db.relationship('City')
    employee = db.relationship('Employee')
    investor = db.relationship('Investor')
    client = db.relationship('Client')
    stock_item = db.relationship('StockItem')
    expenses_data = db.relationship('PendingSaleExpense', backref='pending_sale', lazy=True, cascade="all, delete-orphan")


class PendingSaleExpense(db.Model):
    """Расходы для временной заявки"""
    id = db.Column(db.Integer, primary_key=True)
    pending_sale_id = db.Column(db.Integer, db.ForeignKey('pending_sale.id'), nullable=False)
    expense_type_id = db.Column(db.Integer, db.ForeignKey('expense_type.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    comment = db.Column(db.String(200))
    
    expense_type = db.relationship('ExpenseType')


class SaleAdditionalBonus(db.Model):
    """Дополнительный бонус другому менеджеру по продаже (админ накидывает в заявке)"""
    id = db.Column(db.Integer, primary_key=True)
    sale_id = db.Column(db.Integer, db.ForeignKey('sale.id'), nullable=False)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    employee = db.relationship('Employee', backref='sale_additional_bonuses')


class PendingSaleAdditionalBonus(db.Model):
    """Дополнительный бонус в заявке на продажу (до одобрения)"""
    id = db.Column(db.Integer, primary_key=True)
    pending_sale_id = db.Column(db.Integer, db.ForeignKey('pending_sale.id'), nullable=False)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    pending_sale = db.relationship('PendingSale', backref=db.backref('additional_bonuses', lazy=True, cascade='all, delete-orphan'))
    employee = db.relationship('Employee', backref='pending_sale_additional_bonuses')


class ManualBonus(db.Model):
    """Ручной бонус — выдаётся менеджеру администратором вне привязки к конкретной продаже"""
    __tablename__ = 'manual_bonus'
    id = db.Column(db.Integer, primary_key=True)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    date = db.Column(db.Date, nullable=False)
    comment = db.Column(db.String(300), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    employee = db.relationship('Employee', backref='manual_bonuses')


class PendingStock(db.Model):
    """Временная заявка на сток, ожидающая одобрения"""
    id = db.Column(db.Integer, primary_key=True)
    photo = db.Column(db.String(200))
    product_name = db.Column(db.String(200), nullable=False)
    reference = db.Column(db.String(200))
    item_year = db.Column(db.Integer, nullable=True)  # Год изделия (необязательное)
    buy_price = db.Column(db.Float, nullable=True)  # Менеджер не указывает — админ дописывает в Telegram (для общего стока) или выплата (для комиссионного)
    expected_sell_price = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Integer, default=1)
    komplektatsiya = db.Column('komplektatsiya', db.String(50), nullable=True)  # Комплектация: Полный комплект, только часы, только коробка, только документы
    komissionnyy = db.Column(db.Boolean, default=False, nullable=False)  # Комиссионный товар
    category = db.Column(db.String(50), nullable=True)
    city_id = db.Column(db.Integer, db.ForeignKey('city.id'), nullable=False)
    investor_id = db.Column(db.Integer, db.ForeignKey('investor.id'), nullable=True)  # Для общего стока
    # Поля для комиссионного стока (имя и телефон владельца)
    client_full_name = db.Column(db.String(200), nullable=True)  # Имя владельца для комиссионного стока
    client_phone = db.Column(db.String(50), nullable=True)  # Телефон владельца для комиссионного стока
    telegram_message_id = db.Column(db.Integer, nullable=True)
    telegram_chat_id = db.Column(db.BigInteger, nullable=True)
    status = db.Column(db.String(20), default='pending', nullable=False)  # pending, approved, rejected
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    city = db.relationship('City')
    investor = db.relationship('Investor')
    expenses_data = db.relationship('PendingStockExpense', backref='pending_stock', lazy=True, cascade="all, delete-orphan")


class PendingStockExpense(db.Model):
    """Расходы для временной заявки на сток"""
    id = db.Column(db.Integer, primary_key=True)
    pending_stock_id = db.Column(db.Integer, db.ForeignKey('pending_stock.id'), nullable=False)
    expense_type_id = db.Column(db.Integer, db.ForeignKey('expense_type.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    comment = db.Column(db.String(200), nullable=True)

    expense_type = db.relationship('ExpenseType')


class PendingPrepayment(db.Model):
    """Временная заявка на предоплату, ожидающая одобрения"""
    id = db.Column(db.Integer, primary_key=True)
    photo = db.Column(db.String(200))
    product_name = db.Column(db.String(200), nullable=False)
    reference = db.Column(db.String(200))
    item_year = db.Column(db.Integer, nullable=True)  # Год изделия (необязательное)
    buy_price = db.Column(db.Float, nullable=True)  # Цена покупки
    prepayment_amount = db.Column(db.Float, nullable=False)  # Сумма предоплаты (заход)
    sell_price = db.Column(db.Float, nullable=False)  # Цена продажи
    bonus = db.Column(db.Float, default=0.0)  # Бонус менеджера за продажу (итого)
    initial_bonus = db.Column(db.Float, default=0.0)  # Бонус, указанный менеджером при создании заявки
    komplektatsiya = db.Column('komplektatsiya', db.String(50), nullable=True)  # Комплектация: Полный комплект, только часы, только коробка, только документы
    komissionnyy = db.Column(db.Boolean, default=False, nullable=False)  # Комиссионный товар
    city_id = db.Column(db.Integer, db.ForeignKey('city.id'), nullable=False)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    investor_id = db.Column(db.Integer, db.ForeignKey('investor.id'), nullable=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=True)
    date = db.Column(db.DateTime, default=datetime.utcnow)
    telegram_message_id = db.Column(db.Integer, nullable=True)
    telegram_chat_id = db.Column(db.BigInteger, nullable=True)
    status = db.Column(db.String(20), default='pending', nullable=False)  # pending, approved, rejected, converted_to_sale
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    comment = db.Column(db.Text, nullable=True)  # Комментарий
    
    city = db.relationship('City')
    employee = db.relationship('Employee')
    investor = db.relationship('Investor')
    client = db.relationship('Client')
    expenses_data = db.relationship('PendingPrepaymentExpense', backref='pending_prepayment', lazy=True, cascade="all, delete-orphan")


class PendingPrepaymentExpense(db.Model):
    """Расходы для временной заявки на предоплату"""
    id = db.Column(db.Integer, primary_key=True)
    pending_prepayment_id = db.Column(db.Integer, db.ForeignKey('pending_prepayment.id'), nullable=False)
    expense_type_id = db.Column(db.Integer, db.ForeignKey('expense_type.id'), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    comment = db.Column(db.String(500), nullable=True)
    
    expense_type = db.relationship('ExpenseType')
    comment = db.Column(db.String(200))
    
    expense_type = db.relationship('ExpenseType')


class SaleApproval(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sale_id = db.Column(db.Integer, db.ForeignKey('sale.id'), nullable=False)
    telegram_message_id = db.Column(db.Integer, nullable=True)
    telegram_chat_id = db.Column(db.BigInteger, nullable=True)
    status = db.Column(db.String(20), default='pending', nullable=False)  # pending, approved, rejected
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ChannelStats(db.Model):
    """Статистика по каналам привлечения клиентов"""
    __tablename__ = 'channel_stats'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False)  # Дата
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)  # Менеджер
    telegram_count = db.Column(db.Integer, default=0, nullable=False)  # Телеграм
    instagram_count = db.Column(db.Integer, default=0, nullable=False)  # Инстаграм
    website_count = db.Column(db.Integer, default=0, nullable=False)  # Сайт
    phone_count = db.Column(db.Integer, default=0, nullable=False)  # Личный телефон
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    employee = db.relationship('Employee', backref='channel_stats')


class BotAdmin(db.Model):
    """Администраторы Telegram бота"""
    id = db.Column(db.Integer, primary_key=True)
    chat_id = db.Column(db.BigInteger, unique=True, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)  # Связь с User
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=True)  # Связь с Employee (для менеджеров)
    username = db.Column(db.String(200), nullable=True)
    first_name = db.Column(db.String(200), nullable=True)
    last_name = db.Column(db.String(200), nullable=True)
    added_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    is_manager = db.Column(db.Boolean, default=False, nullable=False)  # True если менеджер, False если админ


class Repair(db.Model):
    """Ремонт: Часы или Ювелирные изделия"""
    id = db.Column(db.Integer, primary_key=True)
    photo = db.Column(db.String(200))
    product_name = db.Column(db.String(200), nullable=False)  # Наименование товара
    buy_price = db.Column(db.Float, nullable=False)  # Заход
    sell_price = db.Column(db.Float, nullable=False)  # Продажа
    city_id = db.Column(db.Integer, db.ForeignKey('city.id'), nullable=False)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=True)
    phone = db.Column(db.String(50), nullable=True)  # Телефон
    telegram = db.Column(db.String(100), nullable=True)  # Тг
    date = db.Column(db.DateTime, default=datetime.utcnow)
    repair_type = db.Column(db.String(50), nullable=False)  # Ремонт, ТО, Полировка, Замена стекла, Запчасти, Другое
    repair_type_other = db.Column(db.String(200), nullable=True)  # Если выбрано "Другое"
    repair_category = db.Column(db.String(50), nullable=False)  # 'watches' или 'jewelry'
    comment = db.Column(db.Text, nullable=True)  # Комментарий
    
    city = db.relationship('City', backref='repairs')
    client = db.relationship('Client', backref='repairs')
    
    def __repr__(self):
        return f'<Repair {self.product_name}>'


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def generate_unique_filename(original_filename):
    """
    Генерирует уникальное имя файла на основе UUID4 и оригинального расширения.
    Это предотвращает конфликты при загрузке файлов с одинаковыми именами.
    """
    if not original_filename or '.' not in original_filename:
        # Если нет расширения, используем .jpg по умолчанию
        ext = 'jpg'
        unique_name = str(uuid.uuid4()) + '.' + ext
    else:
        # Получаем расширение из оригинального имени
        ext = original_filename.rsplit('.', 1)[1].lower()
        # Генерируем уникальное имя с сохранением расширения
        unique_name = str(uuid.uuid4()) + '.' + ext
    
    return unique_name


def optimize_image(image_path, max_size=(1920, 1920), quality=85):
    """
    Оптимизирует изображение: уменьшает размер и сжимает.
    Учитывает EXIF-ориентацию для правильного отображения.
    
    Args:
        image_path: путь к исходному изображению
        max_size: максимальный размер (ширина, высота) в пикселях
        quality: качество JPEG (1-100, рекомендуется 80-90)
    
    Returns:
        True если успешно, False если ошибка или PIL недоступен
    """
    if not PIL_AVAILABLE:
        return False
    
    try:
        # Открываем изображение
        img = Image.open(image_path)
        
        # Исправляем ориентацию на основе EXIF данных
        try:
            # Получаем EXIF данные
            exif = img.getexif()
            if exif is not None:
                # Получаем значение ориентации (тег 274)
                orientation = exif.get(274)  # EXIF Orientation tag
                
                # Применяем поворот в зависимости от ориентации
                if orientation == 2:
                    img = img.transpose(Image.Transpose.FLIP_LEFT_RIGHT)
                elif orientation == 3:
                    img = img.rotate(180, expand=True)
                elif orientation == 4:
                    img = img.transpose(Image.Transpose.FLIP_TOP_BOTTOM)
                elif orientation == 5:
                    img = img.transpose(Image.Transpose.FLIP_LEFT_RIGHT).rotate(90, expand=True)
                elif orientation == 6:
                    img = img.rotate(-90, expand=True)
                elif orientation == 7:
                    img = img.transpose(Image.Transpose.FLIP_LEFT_RIGHT).rotate(-90, expand=True)
                elif orientation == 8:
                    img = img.rotate(90, expand=True)
        except (AttributeError, KeyError, TypeError):
            # Если нет EXIF данных или ошибка при чтении - пропускаем
            pass
        
        # Конвертируем RGBA в RGB для JPEG (если нужно)
        if img.mode in ('RGBA', 'LA', 'P'):
            # Создаем белый фон
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Получаем оригинальный размер
        original_size = img.size
        
        # Уменьшаем размер если он больше максимального
        if original_size[0] > max_size[0] or original_size[1] > max_size[1]:
            img.thumbnail(max_size, Image.Resampling.LANCZOS)
        
        # Сохраняем оптимизированное изображение
        # Используем optimize=True для дополнительного сжатия
        # exif=b'' удаляет EXIF данные (они уже применены к изображению)
        img.save(image_path, 'JPEG', quality=quality, optimize=True, exif=b'')
        
        return True
    except Exception as e:
        print(f"Ошибка при оптимизации изображения {image_path}: {e}")
        return False


# Декоратор для проверки авторизации
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Пожалуйста, войдите в систему', 'error')
            return redirect(url_for('login'))
        # Проверяем, является ли пользователь инвестором или сервисом
        user = get_current_user()
        if user and user.is_investor:
            # Инвестор может заходить только на страницу стока и связанные страницы
            allowed_routes = ['stock', 'investor_stock_detail', 'investor_stock_item_detail', 'my_sales_history', 'logout', 'uploaded_file']
            if f.__name__ not in allowed_routes:
                flash('Доступ запрещен', 'error')
                return redirect(url_for('stock'))
        if user and getattr(user, "is_service", False):
            # Роль сервиса/ремонта может работать только с разделом ремонта
            allowed_routes = [
                'repair_main',
                'repair_list',
                'add_repair',
                'edit_repair',
                'delete_repair',
                'logout',
                'uploaded_file',
            ]
            if f.__name__ not in allowed_routes:
                flash('Доступ только к разделу ремонта', 'error')
                return redirect(url_for('repair_main'))
        return f(*args, **kwargs)
    return decorated_function


# Декоратор для проверки прав администратора (или топ-менеджера)
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Пожалуйста, войдите в систему', 'error')
            return redirect(url_for('login'))
        user = User.query.get(session['user_id'])
        if not user or (not user.is_admin and not user.is_top_manager):
            flash('У вас нет прав доступа к этой странице', 'error')
            return redirect(url_for('main'))
        return f(*args, **kwargs)
    return decorated_function


def get_current_user():
    """Возвращает текущего пользователя или None"""
    if 'user_id' not in session:
        return None
    return User.query.get(session['user_id'])


def get_current_user_employee():
    """Возвращает Employee для текущего менеджера (продавца), для админа, топ-менеджера и инвестора — None"""
    user = get_current_user()
    if not user or user.is_admin or user.is_investor or user.is_top_manager:
        return None
    return Employee.query.filter_by(name=user.name).first()

def get_current_user_investor():
    """Получить Investor для текущего пользователя по имени"""
    user = get_current_user()
    if not user or not user.is_investor:
        return None
    return Investor.query.filter_by(name=user.name).first()


# Бонус Мурада (топ-менеджер) по продажам Москвы:
# - часы: 100$
# - ювелирные изделия: 50$
MURAD_BONUS_BY_CATEGORY = {
    'watches': 100.0,
    'jewelry': 50.0,
    'bags': 100.0,
    'accessories': 50.0,
}

def get_murad_bonus_for_city(city_or_id, category=None):
    """Возвращает бонус Мурада для Москвы в зависимости от категории товара."""
    if city_or_id is None:
        return 0.0
    city = city_or_id if hasattr(city_or_id, 'name') else City.query.get(city_or_id)
    if not (city and city.name == 'Москва'):
        return 0.0
    normalized_category = (category or '').strip().lower()
    return MURAD_BONUS_BY_CATEGORY.get(normalized_category, 0.0)


def _manager_stats_period(year_filter, month_filter):
    """Возвращает (start_date, end_date, period_label) для manager_stats."""
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    if year_filter != 'all' and month_filter != 'all':
        try:
            start = date(int(year_filter), int(month_filter), 1)
            end = start + relativedelta(months=1)
            period_label = f"{months_ru[int(month_filter) - 1]} {year_filter}"
        except:
            start = date.today().replace(day=1)
            end = start + relativedelta(months=1)
            period_label = "Все время"
    elif year_filter != 'all':
        try:
            start = date(int(year_filter), 1, 1)
            end = date(int(year_filter) + 1, 1, 1)
            period_label = f"{year_filter} год"
        except:
            start = date.today().replace(day=1)
            end = start + relativedelta(months=1)
            period_label = "Все время"
    else:
        start = date(2000, 1, 1)
        end = date.today() + relativedelta(days=1)
        period_label = "Все время"
    return start, end, period_label


def get_active_employees():
    """Возвращает список активных Employee (у которых есть активный User)"""
    all_employees = Employee.query.all()
    active_employees = []
    for employee in all_employees:
        # Проверяем, есть ли активный User с таким именем (не админ, не инвестор, не топ-менеджер)
        user = User.query.filter_by(name=employee.name, is_admin=False, is_investor=False, is_top_manager=False).first()
        if user:
            active_employees.append(employee)
    return active_employees


def get_active_investors():
    """Возвращает список всех инвесторов"""
    return Investor.query.order_by(Investor.name).all()


@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


# РОУТЫ ДЛЯ АВТОРИЗАЦИИ
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        user = User.query.filter_by(username=username).first()
        
        if user and user.check_password(password):
            session['user_id'] = user.id
            session['username'] = user.username
            # Топ-менеджер получает admin-уровень доступа, кроме заявок на продажу
            session['is_admin'] = user.is_admin or user.is_top_manager
            session['is_investor'] = user.is_investor
            session['is_top_manager'] = user.is_top_manager
            session['is_service'] = getattr(user, "is_service", False)
            flash(f'Добро пожаловать, {user.name}!', 'success')
            # Инвестор перенаправляется на страницу стока
            if user.is_investor:
                return redirect(url_for('stock'))
            # Пользователь сервиса сразу идёт в раздел ремонта
            if getattr(user, "is_service", False):
                return redirect(url_for('repair_main'))
            return redirect(url_for('main'))
        else:
            flash('Неверный логин или пароль', 'error')
    
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    flash('Вы вышли из системы', 'success')
    return redirect(url_for('login'))


# ГЛАВНАЯ СТРАНИЦА (MAIN) - корневой маршрут
@app.route('/')
@app.route('/main')
@login_required
def main():
    # Инвестор перенаправляется на страницу выбора стока
    user = get_current_user()
    if user and user.is_investor:
        return redirect(url_for('stock_select'))
    return render_template('main.html')


# СТРАНИЦА ВЫБОРА ТИПА СТОКА
@app.route('/stock_select')
@login_required
def stock_select():
    return render_template('stock_select.html')


# СТРАНИЦА ОБЩЕГО СТОКА
@app.route('/stock')
@login_required
def stock():
    user = get_current_user()
    current_investor = get_current_user_investor()
    is_admin = session.get('is_admin', False)
    is_investor = user.is_investor if user else False
    
    # Если инвестор - показываем только его позиции (только общий сток)
    if is_investor and current_investor:
        all_stock_items = StockItem.query.filter_by(investor_id=current_investor.id, sold=False, komissionnyy=False).all()
        
        total_invested = sum(item.total_invested for item in all_stock_items)
        stock_positions = sum(item.quantity for item in all_stock_items)
        # Для инвестора показываем только 40% от потенциальной прибыли
        expected_profit_full = sum(item.expected_profit for item in all_stock_items)
        expected_profit = expected_profit_full * 0.4  # 40% от прибыли
        
        # Также считаем общую прибыль компании (для отображения внизу)
        # Получаем все продажи этого инвестора (они связаны через investor_id)
        investor_sales = Sale.query.filter_by(investor_id=current_investor.id).all()
        total_company_profit = 0
        for sale in investor_sales:
            # Чистая прибыль = цена продажи - цена покупки - расходы - бонусы
            sale_expenses = sum(e.amount for e in sale.expenses)
            net_profit = sale.sell_price - sale.buy_price - sale_expenses - (sale.bonus if sale.bonus else 0)
            total_company_profit += net_profit
        
        return render_template('stock.html',
                               total_invested=total_invested,
                               stock_positions=stock_positions,
                               expected_profit=expected_profit,
                               is_investor=True,
                               total_company_profit=(total_company_profit * 0.4) if total_company_profit > 0 else 0,  # 40% от общей прибыли
                               current_investor=current_investor,
                               stock_type='general')
    else:
        # Для админа/менеджера - показываем все позиции общего стока
        general_items = StockItem.query.filter_by(sold=False, komissionnyy=False).all()
        commission_items = StockItem.query.filter_by(sold=False, komissionnyy=True).all()
        
        # Покупка (общий) + выплаты (комиссионный); стоки без buy_price дают 0
        total_invested = sum(item.total_invested for item in general_items) + sum(item.total_invested for item in commission_items)
        stock_positions = sum(item.quantity for item in general_items) + sum(item.quantity for item in commission_items)
        expected_profit = sum(item.expected_profit for item in general_items) + sum(item.expected_profit for item in commission_items)
        
        # Стоки к дозаполнению (только для админа)
        incomplete_stock_count = 0
        if is_admin:
            all_unsold = StockItem.query.filter_by(sold=False).all()
            incomplete_stock_count = sum(1 for item in all_unsold if item.needs_completion())
        
        return render_template('stock.html',
                               total_invested=total_invested,
                               stock_positions=stock_positions,
                               expected_profit=expected_profit,
                               is_investor=False,
                               is_admin=is_admin,
                               incomplete_stock_count=incomplete_stock_count,
                               stock_type='general')


# СТРАНИЦА КОМИССИОННОГО СТОКА
@app.route('/commission_stock')
@login_required
def commission_stock():
    user = get_current_user()
    current_investor = get_current_user_investor()
    is_admin = session.get('is_admin', False)
    is_investor = user.is_investor if user else False
    
    # Если инвестор - показываем только его позиции (только комиссионный сток)
    if is_investor and current_investor:
        all_stock_items = StockItem.query.filter_by(investor_id=current_investor.id, sold=False, komissionnyy=True).all()
        
        total_invested = sum(item.total_invested for item in all_stock_items)
        stock_positions = sum(item.quantity for item in all_stock_items)
        # Для инвестора показываем только 40% от потенциальной прибыли
        expected_profit_full = sum(item.expected_profit for item in all_stock_items)
        expected_profit = expected_profit_full * 0.4  # 40% от прибыли
        
        # Также считаем общую прибыль компании (для отображения внизу)
        investor_sales = Sale.query.filter_by(investor_id=current_investor.id).all()
        total_company_profit = 0
        for sale in investor_sales:
            sale_expenses = sum(e.amount for e in sale.expenses)
            net_profit = sale.sell_price - sale.buy_price - sale_expenses - (sale.bonus if sale.bonus else 0)
            total_company_profit += net_profit
        
        return render_template('stock.html',
                               total_invested=total_invested,
                               stock_positions=stock_positions,
                               expected_profit=expected_profit,
                               is_investor=True,
                               total_company_profit=(total_company_profit * 0.4) if total_company_profit > 0 else 0,
                               current_investor=current_investor,
                               stock_type='commission')
    else:
        # Для админа/менеджера - показываем все позиции (только комиссионный сток)
        all_stock_items = StockItem.query.filter_by(sold=False, komissionnyy=True).all()
        
        total_invested = sum(item.total_invested for item in all_stock_items)
        stock_positions = sum(item.quantity for item in all_stock_items)
        expected_profit = sum(item.expected_profit for item in all_stock_items)
        
        incomplete_stock_count = 0
        if is_admin:
            all_unsold = StockItem.query.filter_by(sold=False).all()
            incomplete_stock_count = sum(1 for item in all_unsold if item.needs_completion())
        
        return render_template('stock.html',
                               total_invested=total_invested,
                               stock_positions=stock_positions,
                               expected_profit=expected_profit,
                               is_investor=False,
                               is_admin=is_admin,
                               incomplete_stock_count=incomplete_stock_count,
                               stock_type='commission')


# СТРАНИЦА "СТОКИ К ДОЗАПОЛНЕНИЮ" (только для админа)
@app.route('/stock_incomplete')
@login_required
def stock_incomplete():
    if not session.get('is_admin'):
        flash('Доступ только для администратора', 'error')
        return redirect(url_for('stock'))
    all_unsold = StockItem.query.filter_by(sold=False).all()
    incomplete_items = [item for item in all_unsold if item.needs_completion()]
    return render_template('stock_incomplete.html', stock_items=incomplete_items)


# СТРАНИЦА "ВСЕ ГОРОДА" ДЛЯ ОБЩЕГО СТОКА
@app.route('/stock_cities')
@login_required
def stock_cities():
    # Получаем все города, у которых есть общий или комиссионный сток
    cities = City.query.all()
    cities_data = {}
    
    for city in cities:
        # Общий сток (не комиссионный)
        general_items = StockItem.query.filter(
            StockItem.city_id == city.id,
            or_(StockItem.sold == False, StockItem.sold.is_(None)),
            StockItem.komissionnyy == False
        ).all()
        # Комиссионный сток
        commission_items = StockItem.query.filter(
            StockItem.city_id == city.id,
            or_(StockItem.sold == False, StockItem.sold.is_(None)),
            StockItem.komissionnyy == True
        ).all()
        
        all_items = general_items + commission_items
        if all_items:  # Показываем только города со стоком
            # Покупка (общий) + выплаты (комиссионный)
            total_invested = sum(item.total_invested for item in all_items)
            # Позиции и прибыль - общий + комиссионный
            stock_positions = sum(item.quantity for item in all_items)
            expected_profit = sum(item.expected_profit for item in all_items)
            
            cities_data[city.name] = {
                'positions': stock_positions,
                'invested': total_invested,
                'expected_profit': expected_profit
            }
    
    # Сортируем города по названию
    sorted_cities = sorted(cities_data.items())
    
    is_admin = session.get('is_admin', False)
    
    return render_template('stock_cities.html', 
                         cities_data=dict(sorted_cities),
                         is_admin=is_admin,
                         stock_type='general')


# СТРАНИЦА "ВСЕ ГОРОДА" ДЛЯ КОМИССИОННОГО СТОКА
@app.route('/commission_stock_cities')
@login_required
def commission_stock_cities():
    # Получаем все города, у которых есть комиссионный сток
    cities = City.query.all()
    cities_data = {}
    
    for city in cities:
        # Фильтруем только непроданные позиции комиссионного стока
        stock_items = StockItem.query.filter(
            StockItem.city_id == city.id,
            or_(StockItem.sold == False, StockItem.sold.is_(None)),
            StockItem.komissionnyy == True
        ).all()
        
        if stock_items:  # Показываем только города с стоком
            total_invested = sum(item.total_invested for item in stock_items)
            stock_positions = sum(item.quantity for item in stock_items)
            expected_profit = sum(item.expected_profit for item in stock_items)
            
            cities_data[city.name] = {
                'positions': stock_positions,
                'invested': total_invested,
                'expected_profit': expected_profit
            }
    
    # Сортируем города по названию
    sorted_cities = sorted(cities_data.items())
    
    is_admin = session.get('is_admin', False)
    
    return render_template('stock_cities.html', 
                         cities_data=dict(sorted_cities),
                         is_admin=is_admin,
                         stock_type='commission')


# СТРАНИЦА ДЕТАЛЬНОГО ОБЩЕГО СТОКА ПО ГОРОДУ
@app.route('/stock_city/<city_name>')
@login_required
def stock_city_detail(city_name):
    # Находим город
    city = City.query.filter_by(name=city_name).first_or_404()
    
    # Получаем общий сток для этого города (только непроданные)
    general_items = StockItem.query.filter_by(city_id=city.id, sold=False, komissionnyy=False).all()
    # Получаем комиссионный сток для этого города (только непроданные)
    commission_items = StockItem.query.filter_by(city_id=city.id, sold=False, komissionnyy=True).all()
    # Объединяем: сначала общий, потом комиссионный
    stock_items = general_items + commission_items
    stock_ids = [i.id for i in stock_items]
    pending_sale_stock_ids = {r[0] for r in db.session.query(PendingSale.stock_id).filter(
        PendingSale.stock_id.isnot(None),
        PendingSale.status == 'pending',
        PendingSale.stock_id.in_(stock_ids)
    ).all()} if stock_ids else set()
    
    # Подсчитываем общую статистику
    # Покупка (общий) + выплаты (комиссионный)
    total_invested = sum(item.total_invested for item in stock_items)
    # Позиции и прибыль - общий + комиссионный
    stock_positions = sum(item.quantity for item in stock_items)
    expected_profit = sum(item.expected_profit for item in stock_items)
    
    is_admin = session.get('is_admin', False)
    
    return render_template('stock_city_detail.html',
                           city=city,
                           stock_items=stock_items,
                           total_invested=total_invested,
                           stock_positions=stock_positions,
                           expected_profit=expected_profit,
                           is_admin=is_admin,
                           stock_type='general',
                           pending_sale_stock_ids=pending_sale_stock_ids)


# СТРАНИЦА ДЕТАЛЬНОГО КОМИССИОННОГО СТОКА ПО ГОРОДУ
@app.route('/commission_stock_city/<city_name>')
@login_required
def commission_stock_city_detail(city_name):
    # Находим город
    city = City.query.filter_by(name=city_name).first_or_404()
    
    # Получаем все товары в комиссионном стоке для этого города (только непроданные)
    stock_items = StockItem.query.filter_by(city_id=city.id, sold=False, komissionnyy=True).all()
    stock_ids = [i.id for i in stock_items]
    pending_sale_stock_ids = {r[0] for r in db.session.query(PendingSale.stock_id).filter(
        PendingSale.stock_id.isnot(None),
        PendingSale.status == 'pending',
        PendingSale.stock_id.in_(stock_ids)
    ).all()} if stock_ids else set()
    
    # Подсчитываем общую статистику
    total_invested = sum(item.total_invested for item in stock_items)
    stock_positions = sum(item.quantity for item in stock_items)
    expected_profit = sum(item.expected_profit for item in stock_items)
    
    is_admin = session.get('is_admin', False)
    
    return render_template('stock_city_detail.html',
                           city=city,
                           stock_items=stock_items,
                           total_invested=total_invested,
                           stock_positions=stock_positions,
                           expected_profit=expected_profit,
                           is_admin=is_admin,
                           stock_type='commission',
                           pending_sale_stock_ids=pending_sale_stock_ids)


# СТРАНИЦА "ВСЕ ИНВЕСТОРЫ" ДЛЯ СТОКА
@app.route('/stock_investors')
@login_required
def stock_investors():
    # Получаем всех активных инвесторов, у которых есть сток
    investors = get_active_investors()
    investors_data = {}
    
    for investor in investors:
        stock_items = StockItem.query.filter_by(investor_id=investor.id, sold=False).all()
        if stock_items:  # Показываем только инвесторов с стоком
            total_invested = sum(item.total_invested for item in stock_items)
            stock_positions = sum(item.quantity for item in stock_items)
            expected_profit = sum(item.expected_profit for item in stock_items)
            
            # Получаем список уникальных городов для этого инвестора
            cities = sorted(set([item.city.name for item in stock_items]))
            cities_str = ', '.join(cities)
            
            investors_data[investor.name] = {
                'id': investor.id,
                'positions': stock_positions,
                'invested': total_invested,
                'expected_profit': expected_profit,
                'cities': cities_str,
                'is_commission_client': investor.is_commission_client
            }
    
    # Сортируем инвесторов по названию
    sorted_investors = sorted(investors_data.items())
    
    is_admin = session.get('is_admin', False)
    
    # Получаем инвесторов для управления (раздельно)
    regular_investors   = Investor.query.filter_by(is_commission_client=False).order_by(Investor.name).all()
    commission_investors = Investor.query.filter_by(is_commission_client=True).order_by(Investor.name).all()

    return render_template('stock_investors.html',
                         investors_data=dict(sorted_investors),
                         regular_investors=regular_investors,
                         commission_investors=commission_investors,
                         is_admin=is_admin)


# СТРАНИЦА ДЕТАЛЬНОГО СТОКА ДЛЯ ИНВЕСТОРА (его собственные позиции)
@app.route('/investor_stock_detail')
@login_required
def investor_stock_detail():
    """Детальная страница позиций инвестора"""
    user = get_current_user()
    if not user or not user.is_investor:
        flash('Доступ запрещен', 'error')
        return redirect(url_for('main'))
    
    current_investor = get_current_user_investor()
    if not current_investor:
        flash('Инвестор не найден', 'error')
        return redirect(url_for('main'))
    
    # Получаем все товары в стоке для этого инвестора (только непроданные)
    stock_items = StockItem.query.filter_by(investor_id=current_investor.id, sold=False).all()
    
    # Подсчитываем общую статистику
    total_invested = sum(item.total_invested for item in stock_items)
    stock_positions = sum(item.quantity for item in stock_items)
    expected_profit_full = sum(item.expected_profit for item in stock_items)
    expected_profit = expected_profit_full * 0.4  # 40% от прибыли
    
    return render_template('investor_stock_detail.html',
                           investor=current_investor,
                           stock_items=stock_items,
                           total_invested=total_invested,
                           stock_positions=stock_positions,
                           expected_profit=expected_profit)


# СТРАНИЦА ПОДРОБНЕЕ ДЛЯ КАЖДОЙ ПОЗИЦИИ ИНВЕСТОРА
@app.route('/investor_stock_item/<int:stock_item_id>')
@login_required
def investor_stock_item_detail(stock_item_id):
    """Страница подробной информации о позиции инвестора"""
    user = get_current_user()
    if not user or not user.is_investor:
        flash('Доступ запрещен', 'error')
        return redirect(url_for('main'))
    
    current_investor = get_current_user_investor()
    if not current_investor:
        flash('Инвестор не найден', 'error')
        return redirect(url_for('main'))
    
    # Получаем позицию стока с загрузкой расходов
    from sqlalchemy.orm import joinedload
    stock_item = StockItem.query.options(
        joinedload(StockItem.expenses).joinedload(StockExpense.expense_type)
    ).get_or_404(stock_item_id)
    
    # Проверяем, что позиция принадлежит текущему инвестору
    if stock_item.investor_id != current_investor.id:
        flash('Доступ запрещен', 'error')
        return redirect(url_for('investor_stock_detail'))
    
    # Если позиция продана, получаем информацию о продаже
    # Ищем продажу по инвестору и совпадению данных (product_name, buy_price)
    sale = None
    investor_profit = 0.0
    total_profit = 0.0
    sale_expenses_total = 0.0
    
    if stock_item.sold:
        # Ищем продажу инвестора с совпадающими данными
        # Загружаем расходы вместе с продажей (включая expense_type)
        sale = Sale.query.options(
            joinedload(Sale.expenses).joinedload(Expense.expense_type)
        ).filter_by(
            investor_id=current_investor.id,
            product_name=stock_item.product_name,
            buy_price=stock_item.buy_price
        ).first()
        if sale:
            # Чистая прибыль = цена продажи - цена покупки - расходы из стока - расходы из продажи - бонусы
            stock_expenses_total = sum(e.amount for e in stock_item.expenses)
            sale_expenses_total = sum(e.amount for e in sale.expenses)
            total_expenses = stock_expenses_total + sale_expenses_total
            murad_b = getattr(sale, 'murad_bonus', 0) or 0.0
            total_profit = sale.sell_price - sale.buy_price - total_expenses - (sale.bonus if sale.bonus else 0) - murad_b
            investor_profit = total_profit * 0.4  # 40% от чистой прибыли
            sale_expenses_total = total_expenses  # Общая сумма всех расходов для отображения
        else:
            # Если позиция помечена как проданная, но продажа не найдена
            total_profit = 0.0
            investor_profit = 0.0
    else:
        # Если еще не продано, считаем потенциальную прибыль
        # Учитываем расходы из стока
        stock_expenses_total = sum(e.amount for e in stock_item.expenses)
        try:
            # Потенциальная прибыль = ожидаемая цена продажи - цена покупки - расходы из стока
            total_profit = float(stock_item.expected_sell_price - stock_item.buy_price - stock_expenses_total) * stock_item.quantity
        except:
            total_profit = 0.0
        investor_profit = total_profit * 0.4  # 40% от потенциальной прибыли
        sale_expenses_total = stock_expenses_total  # Для отображения расходов из стока
    
    return render_template('investor_stock_item_detail.html',
                           stock_item=stock_item,
                           sale=sale,
                           investor_profit=investor_profit,
                           total_profit=total_profit,
                           sale_expenses_total=sale_expenses_total)


def generate_investor_token(investor_id):
    """Генерирует публичный токен для страницы инвестора (без хранения в БД)."""
    secret = app.config['SECRET_KEY'].encode()
    msg = str(investor_id).encode()
    return hmac.new(secret, msg, hashlib.sha256).hexdigest()[:20]


# ПУБЛИЧНАЯ СТРАНИЦА СТОКА ИНВЕСТОРА (без авторизации)
@app.route('/public/investor/<int:investor_id>/<token>')
def public_investor_stock(investor_id, token):
    """Публичная read-only страница стока инвестора — без входа в систему."""
    expected = generate_investor_token(investor_id)
    if not hmac.compare_digest(token, expected):
        return 'Ссылка недействительна', 403

    investor = Investor.query.get_or_404(investor_id)
    stock_items = StockItem.query.filter_by(investor_id=investor.id, sold=False).order_by(StockItem.date_added.desc()).all()
    stock_positions = sum(item.quantity for item in stock_items)
    total_invested = sum(item.total_invested for item in stock_items)

    history_url = url_for('public_investor_history', investor_id=investor_id, token=token)
    logo_url = url_for('uploaded_file', filename='logo.png')
    return render_template('public_investor_stock.html',
                           investor=investor,
                           stock_items=stock_items,
                           stock_positions=stock_positions,
                           total_invested=total_invested,
                           history_url=history_url,
                           logo_url=logo_url)


# ПУБЛИЧНАЯ ИСТОРИЯ ПРОДАЖ ИНВЕСТОРА (без авторизации, без клиента)
@app.route('/public/investor/<int:investor_id>/<token>/history')
def public_investor_history(investor_id, token):
    """Публичная история продаж стока инвестора — без входа, без данных клиента."""
    expected = generate_investor_token(investor_id)
    if not hmac.compare_digest(token, expected):
        return 'Ссылка недействительна', 403

    investor = Investor.query.get_or_404(investor_id)
    # Все продажи привязанные к этому инвестору
    sold_sales = Sale.query.filter_by(investor_id=investor.id).order_by(Sale.date.desc()).all()

    stock_url = url_for('public_investor_stock', investor_id=investor_id, token=token)
    logo_url = url_for('uploaded_file', filename='logo.png')
    return render_template('public_investor_history.html',
                           investor=investor,
                           sold_sales=sold_sales,
                           stock_url=stock_url,
                           logo_url=logo_url)


# СТРАНИЦА ДЕТАЛЬНОГО СТОКА ПО ИНВЕСТОРУ
@app.route('/stock_investor/<investor_name>')
@login_required
def stock_investor_detail(investor_name):
    # Находим инвестора
    investor = Investor.query.filter_by(name=investor_name).first_or_404()
    
    # Получаем все товары в стоке для этого инвестора (только непроданные)
    stock_items = StockItem.query.filter_by(investor_id=investor.id, sold=False).all()
    stock_ids = [i.id for i in stock_items]
    pending_sale_stock_ids = {r[0] for r in db.session.query(PendingSale.stock_id).filter(
        PendingSale.stock_id.isnot(None),
        PendingSale.status == 'pending',
        PendingSale.stock_id.in_(stock_ids)
    ).all()} if stock_ids else set()
    
    # Подсчитываем общую статистику
    total_invested = sum(item.total_invested for item in stock_items)
    stock_positions = sum(item.quantity for item in stock_items)
    expected_profit = sum(item.expected_profit for item in stock_items)
    
    is_admin = session.get('is_admin', False)
    
    token = generate_investor_token(investor.id)
    public_link = url_for('public_investor_stock', investor_id=investor.id, token=token)

    return render_template('stock_investor_detail.html',
                           investor=investor,
                           stock_items=stock_items,
                           total_invested=total_invested,
                           stock_positions=stock_positions,
                           expected_profit=expected_profit,
                           is_admin=is_admin,
                           public_link=public_link,
                           pending_sale_stock_ids=pending_sale_stock_ids)


# СТРАНИЦА КЛИЕНТОВ
@app.route('/clients')
@login_required
def clients():
    clients_list = Client.query.order_by(Client.full_name).all()
    commission_clients = Investor.query.filter_by(is_commission_client=True).order_by(Investor.name).all()
    return render_template('clients.html', clients=clients_list, commission_clients=commission_clients)


# СТРАНИЦА ИСТОРИИ ПРОДАЖ ПО ИНВЕСТОРУ (для админов - с параметром)
@app.route('/investor_sales_history/<investor_name>')
@login_required
def investor_sales_history(investor_name):
    # Находим инвестора
    investor = Investor.query.filter_by(name=investor_name).first_or_404()
    
    # Проверка доступа для инвестора - если инвестор, то только свою историю
    user = get_current_user()
    if user and user.is_investor:
        current_investor = get_current_user_investor()
        if not current_investor or current_investor.id != investor.id:
            flash('Доступ запрещен', 'error')
            return redirect(url_for('stock'))
    
    # Параметры из запроса
    sort = request.args.get('sort', 'date_desc')
    year_filter = request.args.get('year', 'all')
    month_filter = request.args.get('month', 'all')
    period_filter = request.args.get('period', 'all')
    
    # Базовый запрос - фильтруем по инвестору (менеджер видит только свои)
    sales_query = Sale.query.filter(Sale.investor_id == investor.id)
    current_employee = get_current_user_employee()
    if current_employee:
        sales_query = sales_query.filter(Sale.employee_id == current_employee.id)
    
    # Определяем период для фильтрации
    if period_filter == 'current_month':
        today = date.today()
        start_date = today.replace(day=1)
        end_date = start_date + relativedelta(months=1)
        sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
    elif year_filter != 'all':
        try:
            year_int = int(year_filter)
            if month_filter != 'all':
                try:
                    month_int = int(month_filter)
                    start_date = date(year_int, month_int, 1)
                    end_date = start_date + relativedelta(months=1)
                    sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
                except:
                    start_date = date(year_int, 1, 1)
                    end_date = date(year_int + 1, 1, 1)
                    sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
            else:
                start_date = date(year_int, 1, 1)
                end_date = date(year_int + 1, 1, 1)
                sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
        except:
            pass
    
    # Сортировка
    if sort == 'date_asc':
        sales_query = sales_query.order_by(Sale.date.asc())
    elif sort == 'date_desc':
        sales_query = sales_query.order_by(Sale.date.desc())
    elif sort == 'sell_desc':
        sales_query = sales_query.order_by(Sale.sell_price.desc())
    elif sort == 'sell_asc':
        sales_query = sales_query.order_by(Sale.sell_price.asc())
    else:
        sales_query = sales_query.order_by(Sale.date.desc())
    
    sales = sales_query.all()
    
    # Сортировка по прибыли в Python
    if sort == 'profit_desc':
        sales.sort(key=lambda s: s.profit, reverse=True)
    elif sort == 'profit_asc':
        sales.sort(key=lambda s: s.profit)
    
    # Статистика
    total_realized = sum(s.sell_price for s in sales)  # Сумма реализованных позиций
    count_sales = len(sales)  # Количество реализованных позиций
    gross_income = sum(s.sell_price - s.buy_price for s in sales)  # Грязная выручка (до расходов и бонусов)
    total_expenses = sum(sum(e.amount for e in s.expenses) for s in sales)  # Расходы
    total_buy = sum(s.buy_price for s in sales)  # Сумма покупок
    total_bonuses = sum((s.bonus if s.bonus else 0.0) + (getattr(s, 'murad_bonus', 0) or 0.0) for s in sales)  # Бонусы менеджеров + бонус Мурада
    net_profit = gross_income - total_expenses - total_bonuses  # Прибыль с вычетом расходов и бонусов
    
    # Для инвестора показываем только 40% от прибыли
    is_investor_user = user and user.is_investor if user else False
    if is_investor_user:
        net_profit = net_profit * 0.4  # 40% от чистой прибыли
    
    # Список всех годов для фильтра
    all_years = sorted(set(s.date.year for s in Sale.query.filter(Sale.investor_id == investor.id).with_entities(Sale.date).all()), reverse=True)
    if not all_years:
        all_years = [date.today().year]
    
    # Месяцы для фильтра
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    
    is_admin = session.get('is_admin', False)
    
    return render_template('investor_sales_history.html',
                           investor=investor,
                           sales=sales,
                           total_realized=round(total_realized, 2),
                           count_sales=count_sales,
                           gross_income=round(gross_income, 2),
                           total_expenses=round(total_expenses, 2),
                           net_profit=round(net_profit, 2),
                           sort=sort,
                           year_filter=year_filter if year_filter != 'all' else 'all',
                           month_filter=month_filter if month_filter != 'all' else 'all',
                           period_filter=period_filter,
                           all_years=all_years,
                           months_ru=months_ru,
                           is_investor=is_investor_user,
                           is_admin=is_admin)


# СТРАНИЦА ИСТОРИИ ПРОДАЖ ДЛЯ ТЕКУЩЕГО ИНВЕСТОРА (отдельная страница только для инвесторов)
@app.route('/my_sales_history')
@login_required
def my_sales_history():
    """Страница истории продаж для текущего инвестора - только для инвесторов"""
    user = get_current_user()
    if not user or not user.is_investor:
        flash('Доступ запрещен', 'error')
        return redirect(url_for('stock'))
    
    current_investor = get_current_user_investor()
    if not current_investor:
        flash('Инвестор не найден', 'error')
        return redirect(url_for('stock'))
    
    investor = current_investor
    
    # Параметры из запроса
    sort = request.args.get('sort', 'date_desc')
    year_filter = request.args.get('year', 'all')
    month_filter = request.args.get('month', 'all')
    period_filter = request.args.get('period', 'all')
    
    # Базовый запрос - фильтруем только по текущему инвестору
    sales_query = Sale.query.filter(Sale.investor_id == investor.id)
    
    # Определяем период для фильтрации
    if period_filter == 'current_month':
        today = date.today()
        start_date = today.replace(day=1)
        end_date = start_date + relativedelta(months=1)
        sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
    elif year_filter != 'all':
        try:
            year_int = int(year_filter)
            if month_filter != 'all':
                try:
                    month_int = int(month_filter)
                    start_date = date(year_int, month_int, 1)
                    end_date = start_date + relativedelta(months=1)
                    sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
                except:
                    start_date = date(year_int, 1, 1)
                    end_date = date(year_int + 1, 1, 1)
                    sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
            else:
                start_date = date(year_int, 1, 1)
                end_date = date(year_int + 1, 1, 1)
                sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
        except:
            pass
    
    # Сортировка
    if sort == 'date_asc':
        sales_query = sales_query.order_by(Sale.date.asc())
    elif sort == 'date_desc':
        sales_query = sales_query.order_by(Sale.date.desc())
    elif sort == 'sell_desc':
        sales_query = sales_query.order_by(Sale.sell_price.desc())
    elif sort == 'sell_asc':
        sales_query = sales_query.order_by(Sale.sell_price.asc())
    else:
        sales_query = sales_query.order_by(Sale.date.desc())
    
    sales = sales_query.all()
    
    # Сортировка по прибыли в Python
    if sort == 'profit_desc':
        sales.sort(key=lambda s: s.profit, reverse=True)
    elif sort == 'profit_asc':
        sales.sort(key=lambda s: s.profit)
    
    # Статистика
    total_realized = sum(s.sell_price for s in sales)  # Сумма реализованных позиций
    count_sales = len(sales)  # Количество реализованных позиций
    gross_income = sum(s.sell_price - s.buy_price for s in sales)  # Грязная выручка (до расходов и бонусов)
    total_expenses = sum(sum(e.amount for e in s.expenses) for s in sales)  # Расходы
    total_buy = sum(s.buy_price for s in sales)  # Сумма покупок
    total_bonuses = sum((s.bonus if s.bonus else 0.0) + (getattr(s, 'murad_bonus', 0) or 0.0) for s in sales)  # Бонусы менеджеров + бонус Мурада
    net_profit = gross_income - total_expenses - total_bonuses  # Прибыль с вычетом расходов и бонусов
    
    # Для инвестора показываем только 40% от прибыли
    net_profit = net_profit * 0.4  # 40% от чистой прибыли
    
    # Список всех годов для фильтра
    all_years = sorted(set(s.date.year for s in Sale.query.filter(Sale.investor_id == investor.id).with_entities(Sale.date).all()), reverse=True)
    if not all_years:
        all_years = [date.today().year]
    
    # Месяцы для фильтра
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    
    return render_template('investor_sales_history.html',
                           investor=investor,
                           sales=sales,
                           total_realized=round(total_realized, 2),
                           count_sales=count_sales,
                           gross_income=round(gross_income, 2),
                           total_expenses=round(total_expenses, 2),
                           net_profit=round(net_profit, 2),
                           sort=sort,
                           year_filter=year_filter if year_filter != 'all' else 'all',
                           month_filter=month_filter if month_filter != 'all' else 'all',
                           period_filter=period_filter,
                           all_years=all_years,
                           months_ru=months_ru,
                           is_investor=True)


# СТРАНИЦА DASHBOARD (старая главная)
@app.route('/dashboard', methods=['GET', 'POST'])
@login_required
def dashboard():
    # Получаем параметры периода
    period_type = request.args.get('period_type', 'current_month')  # current_month, custom
    year = request.args.get('year')
    month = request.args.get('month')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # Определяем даты фильтра
    if period_type == 'custom' and start_date and end_date:
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        end = end + relativedelta(days=1)  # чтобы включить конец дня
    else:
        # По умолчанию — текущий месяц
        today = date.today()
        if year and month:
            try:
                selected_date = date(int(year), int(month), 1)
            except:
                selected_date = today
        else:
            selected_date = today

        start = selected_date.replace(day=1)
        end = (start + relativedelta(months=1))

    # Фильтруем продажи за период (менеджер видит только свои)
    sales_query = Sale.query.filter(Sale.date >= start, Sale.date < end)
    current_employee = get_current_user_employee()
    if current_employee:
        sales_query = sales_query.filter(Sale.employee_id == current_employee.id)
    sales = sales_query.all()

    # Расчёты для dashboard
    count_sales = len(sales)
    gross_income = sum(s.sell_price - s.buy_price for s in sales)  # Грязная выручка (до расходов и бонусов)
    total_buy = sum(s.buy_price for s in sales)
    total_sale_expenses = sum(sum(e.amount for e in s.expenses) for s in sales)
    total_bonuses = sum((s.bonus if s.bonus else 0.0) + (getattr(s, 'murad_bonus', 0) or 0.0) for s in sales)  # Бонусы менеджеров + бонус Мурада
    general_expenses = GeneralExpense.query.filter(GeneralExpense.date >= start, GeneralExpense.date < end).all()
    total_general_expenses = sum(g.amount for g in general_expenses)
    net_profit = gross_income - total_sale_expenses - total_general_expenses - total_bonuses

    # Для отображения периода
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    month_name = months_ru[start.month - 1]
    if period_type == 'custom':
        period_label = f"{start.strftime('%d.%m.%Y')} — {(end - relativedelta(days=1)).strftime('%d.%m.%Y')}"
    else:
        period_label = f"{month_name} {start.year}"

    # Список годов для выбора
    all_years = sorted(set(s.date.year for s in Sale.query.with_entities(Sale.date).all()), reverse=True)
    if not all_years:
        all_years = [date.today().year]

    return render_template('dashboard.html',
                           count_sales=count_sales,
                           gross_income=round(gross_income, 2),
                           net_profit=round(net_profit, 2),
                           period_label=period_label,
                           all_years=all_years,
                           selected_year=start.year,
                           selected_month=start.month,
                           period_type=period_type)


# СТАРАЯ СТРАНИЦА СО СПИСКОМ ПРОДАЖ — теперь на /sales
@app.route('/sales')
@login_required
def sales():
    # Параметры из запроса
    sort = request.args.get('sort', 'date_desc')
    city_filter = request.args.get('city', 'all')  # 'all' или название города
    year_filter = request.args.get('year', 'all')
    month_filter = request.args.get('month', 'all')
    employee_filter = request.args.get('employee', 'all')  # 'all' или ID сотрудника
    client_filter = request.args.get('client', 'all')  # 'all' или ID клиента

    # Базовый запрос (менеджер видит только свои продажи)
    sales_query = Sale.query
    current_employee = get_current_user_employee()
    if current_employee:
        sales_query = sales_query.filter(Sale.employee_id == current_employee.id)

    # Фильтр по городу
    if city_filter != 'all':
        sales_query = sales_query.join(City).filter(City.name == city_filter)
    
    # Фильтр по сотруднику
    if employee_filter != 'all':
        try:
            employee_id = int(employee_filter)
            sales_query = sales_query.filter(Sale.employee_id == employee_id)
        except:
            pass

    # Фильтр по клиенту
    if client_filter != 'all':
        try:
            client_id = int(client_filter)
            sales_query = sales_query.filter(Sale.client_id == client_id)
        except:
            pass

    # Фильтр по году и месяцу
    if year_filter != 'all':
        try:
            year_int = int(year_filter)
            if month_filter != 'all':
                try:
                    month_int = int(month_filter)
                    start_date = date(year_int, month_int, 1)
                    end_date = start_date + relativedelta(months=1)
                    sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
                except:
                    start_date = date(year_int, 1, 1)
                    end_date = date(year_int + 1, 1, 1)
                    sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
            else:
                start_date = date(year_int, 1, 1)
                end_date = date(year_int + 1, 1, 1)
                sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
        except:
            pass

    # Сортировка
    if sort == 'date_asc':
        sales_query = sales_query.order_by(Sale.date.asc())
    elif sort == 'date_desc':
        sales_query = sales_query.order_by(Sale.date.desc())
    elif sort == 'sell_desc':
        sales_query = sales_query.order_by(Sale.sell_price.desc())
    elif sort == 'sell_asc':
        sales_query = sales_query.order_by(Sale.sell_price.asc())
    elif sort == 'city':
        sales_query = sales_query.join(City).order_by(City.name.asc())
    else:
        sales_query = sales_query.order_by(Sale.date.desc())

    sales = sales_query.all()

    # Сортировка по прибыли в Python
    if sort == 'profit_desc':
        sales.sort(key=lambda s: s.profit, reverse=True)
    elif sort == 'profit_asc':
        sales.sort(key=lambda s: s.profit)

    # Общая прибыль (учитываем фильтр)
    total_profit = sum(s.profit for s in sales)
    
    # Приводим дату к типу date для корректного сравнения
    def get_sale_date(sale):
        return sale.date.date() if isinstance(sale.date, datetime) else sale.date
    
    # Границы периода и подпись зависят от выбранного фильтра
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    
    if year_filter == 'all':
        # За все время: сводка по всем отфильтрованным продажам
        start_month = None
        end_month = None
        period_label = 'За все время'
        sales_in_period = sales
    elif year_filter != 'all' and month_filter != 'all':
        # Конкретный месяц
        try:
            year_int = int(year_filter)
            month_int = int(month_filter)
            start_month = date(year_int, month_int, 1)
            end_month = start_month + relativedelta(months=1)
            period_label = f"{months_ru[month_int - 1]} {year_int}"
        except (ValueError, TypeError):
            start_month = date.today().replace(day=1)
            end_month = start_month + relativedelta(months=1)
            period_label = f"{months_ru[start_month.month - 1]} {start_month.year}"
        sales_in_period = [s for s in sales if start_month <= get_sale_date(s) < end_month]
    else:
        # Только год
        try:
            year_int = int(year_filter)
            start_month = date(year_int, 1, 1)
            end_month = date(year_int + 1, 1, 1)
            period_label = f"{year_int} год"
        except (ValueError, TypeError):
            start_month = date.today().replace(day=1)
            end_month = start_month + relativedelta(months=1)
            period_label = f"{months_ru[start_month.month - 1]} {start_month.year}"
        sales_in_period = [s for s in sales if start_month <= get_sale_date(s) < end_month]
    
    # Расходы из продаж за выбранный период
    total_sale_expenses = sum(sum(e.amount for e in s.expenses) for s in sales_in_period)
    
    # Общие расходы за период (для "всего времени" — без фильтра по дате)
    if start_month is not None and end_month is not None:
        general_expenses_query = GeneralExpense.query.filter(GeneralExpense.date >= start_month, GeneralExpense.date < end_month)
    else:
        general_expenses_query = GeneralExpense.query
    if city_filter != 'all':
        city_obj = City.query.filter_by(name=city_filter).first()
        if city_obj:
            general_expenses_query = general_expenses_query.filter(GeneralExpense.city_id == city_obj.id)
    general_expenses = general_expenses_query.all()
    total_general_expenses = sum(g.amount for g in general_expenses)
    
    total_expenses = total_sale_expenses + total_general_expenses
    
    # Метрики за выбранный период
    count_sales = len(sales_in_period)
    gross_income = sum(s.sell_price - s.buy_price for s in sales_in_period)
    total_buy = sum(s.buy_price for s in sales_in_period)
    total_bonuses = sum((s.bonus if s.bonus else 0.0) + (getattr(s, 'murad_bonus', 0) or 0.0) for s in sales_in_period)
    net_profit_month = gross_income - total_expenses - total_bonuses

    # Список всех уникальных городов для фильтра
    all_cities = sorted([c.name for c in City.query.all()])
    
    # Список всех сотрудников для фильтра
    all_employees = sorted(get_active_employees(), key=lambda e: e.name)
    
    # Список всех клиентов для фильтра
    all_clients = Client.query.order_by(Client.full_name).all()
    
    # Список всех годов для фильтра
    all_years = sorted(set(s.date.year for s in Sale.query.with_entities(Sale.date).all()), reverse=True)
    if not all_years:
        all_years = [date.today().year]

    # Для менеджера фиксируем фильтр по себе в отображении
    if current_employee:
        employee_filter = str(current_employee.id)

    return render_template('index.html',
                           sales=sales,
                           total_profit=round(total_profit, 2),
                           sort=sort,
                           city_filter=city_filter,
                           employee_filter=employee_filter if employee_filter != 'all' else 'all',
                           client_filter=client_filter if client_filter != 'all' else 'all',
                           year_filter=year_filter if year_filter != 'all' else 'all',
                           month_filter=month_filter if month_filter != 'all' else 'all',
                           all_cities=all_cities,
                           all_employees=all_employees,
                           all_clients=all_clients,
                           all_years=all_years,
                           months_ru=months_ru,
                           total_expenses=round(total_expenses, 2),
                           count_sales=count_sales,
                           gross_income=round(gross_income, 2),
                           net_profit_month=round(net_profit_month, 2),
                           period_label=period_label,
                           is_admin=session.get('is_admin', False))


@app.route('/add_sale', methods=['GET', 'POST'])
@login_required
def add_sale():
    cities = City.query.all()
    expense_types = ExpenseType.query.all()
    employees = get_active_employees()
    investors = get_active_investors()
    clients_list = Client.query.order_by(Client.full_name).all()
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    
    # Менеджер может создать продажу только от своего имени
    if not is_admin and not current_employee:
        flash('Обратитесь к администратору для настройки вашего профиля продавца.', 'error')
        return redirect(url_for('sales'))
    
    # Получаем stock_id, prepayment_id и new_client_id из параметров запроса
    stock_id = request.args.get('stock_id', type=int)
    prepayment_id = request.args.get('prepayment_id', type=int)
    new_client_id = request.args.get('new_client_id', type=int)
    stock_item = None
    prepayment_item = None
    if stock_id:
        stock_item = StockItem.query.get(stock_id)
        if not stock_item:
            flash('Сток не найден!', 'error')
            return redirect(url_for('stock'))
    if prepayment_id:
        prepayment_item = PendingPrepayment.query.get(prepayment_id)
        if not prepayment_item:
            flash('Предоплата не найдена!', 'error')
            return redirect(url_for('prepayment'))

    if request.method == 'POST':
        product_name = request.form['product_name']
        reference = request.form.get('reference', '')
        item_year = int(request.form['item_year']) if request.form.get('item_year') else None
        # Покупка: для админа обязательна; для менеджера можно не указывать (0 = допишет админ)
        buy_price_str = request.form.get('buy_price', '').strip()
        try:
            buy_price = float(buy_price_str) if buy_price_str else 0.0
        except ValueError:
            buy_price = 0.0
        if is_admin and not buy_price_str:
            buy_price = 0.0  # админ не должен оставлять пустым (HTML required), но на всякий случай
        sell_price = float(request.form['sell_price'])
        bonus = float(request.form.get('bonus', 0) or 0)  # Бонус менеджера
        komplektatsiya = request.form.get('komplektatsiya', '').strip() or None  # Комплектация
        komissionnyy = request.form.get('komissionnyy') == '1'  # Комиссионный товар
        sale_category = request.form.get('category') or None
        city_id = int(request.form['city_id'])
        # Только админ может выбрать продавца, менеджер — всегда сам
        employee_id = int(request.form['employee_id']) if is_admin else current_employee.id
        investor_id = int(request.form.get('investor_id')) if request.form.get('investor_id') else None
        client_id = int(request.form.get('client_id')) if request.form.get('client_id') else None
        date_str = request.form['date']
        date = datetime.strptime(date_str, '%Y-%m-%d') if date_str else datetime.utcnow()
        
        # Получаем stock_id и prepayment_id из скрытых полей формы
        stock_id_from_form = request.form.get('stock_id', type=int)
        prepayment_id_from_form = request.form.get('prepayment_id', type=int)

        photo_path = None
        photo = request.files['photo']
        if photo and allowed_file(photo.filename):
            filename = generate_unique_filename(photo.filename)
            photo_path = filename
            full_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            photo.save(full_path)
            # Оптимизируем изображение после сохранения
            optimize_image(full_path)
        elif stock_id_from_form:
            # Если фото не загружено, но есть сток, используем фото из стока
            stock_item_for_sale = StockItem.query.get(stock_id_from_form)
            if stock_item_for_sale and stock_item_for_sale.photo:
                photo_path = stock_item_for_sale.photo
        elif prepayment_id_from_form:
            # Если фото не загружено, но есть предоплата, используем фото из предоплаты
            prepayment_for_sale = PendingPrepayment.query.get(prepayment_id_from_form)
            if prepayment_for_sale and prepayment_for_sale.photo:
                photo_path = prepayment_for_sale.photo

        # Админ создаёт продажу напрямую, менеджер — через заявку
        comment = request.form.get('comment', '').strip() or None
        if is_admin:
            # Создаём продажу напрямую
            murad_bonus = get_murad_bonus_for_city(city_id, sale_category)
            new_sale = Sale(
                photo=photo_path,
                product_name=product_name,
                reference=reference,
                item_year=item_year,
                buy_price=buy_price,
                sell_price=sell_price,
                bonus=bonus,
                initial_bonus=bonus,
                murad_bonus=murad_bonus,
                komplektatsiya=komplektatsiya,
                komissionnyy=komissionnyy,
                category=sale_category,
                city_id=city_id,
                employee_id=employee_id,
                investor_id=investor_id,
                client_id=client_id,
                date=date,
                comment=comment
            )
            db.session.add(new_sale)
            db.session.flush()

            # Добавление расходов из формы (включая расходы из стока или предоплаты, если пользователь их добавил)
            # Если продажа из стока, помечаем сток как проданный
            if stock_id_from_form:
                stock_item_for_sale = StockItem.query.get(stock_id_from_form)
                if stock_item_for_sale:
                    stock_item_for_sale.sold = True
            
            # Если продажа из предоплаты, удаляем заявку предоплаты
            if prepayment_id_from_form:
                prepayment_for_sale = PendingPrepayment.query.get(prepayment_id_from_form)
                if prepayment_for_sale:
                    db.session.delete(prepayment_for_sale)
            
            expense_type_ids = request.form.getlist('expense_type_id')
            amounts = request.form.getlist('expense_amount')
            comments = request.form.getlist('expense_comment')
            for et_id, amt, comment in zip(expense_type_ids, amounts, comments):
                if et_id and amt:
                    expense = Expense(
                        sale_id=new_sale.id,
                        expense_type_id=int(et_id),
                        amount=float(amt),
                        comment=comment if comment else None
                    )
                    db.session.add(expense)

            db.session.commit()
            flash('Продажа успешно добавлена!', 'success')
            return redirect(url_for('sales'))
        else:
            # Создаем временную заявку вместо продажи
            murad_bonus = get_murad_bonus_for_city(city_id, sale_category)
            pending_sale = PendingSale(
                photo=photo_path,
                product_name=product_name,
                reference=reference,
                item_year=item_year,
                buy_price=buy_price,
                sell_price=sell_price,
                bonus=bonus,
                initial_bonus=bonus,
                murad_bonus=murad_bonus,
                komplektatsiya=komplektatsiya,
                komissionnyy=komissionnyy,
                category=sale_category,
                city_id=city_id,
                employee_id=employee_id,
                investor_id=investor_id,
                client_id=client_id,
                date=date,
                stock_id=stock_id_from_form,
                status='pending'
            )
            db.session.add(pending_sale)
            db.session.flush()  # Получаем ID для добавления расходов

            # Добавление расходов из формы (включая расходы из стока, если пользователь их добавил)
            expense_type_ids = request.form.getlist('expense_type_id')
            amounts = request.form.getlist('expense_amount')
            comments = request.form.getlist('expense_comment')
            for et_id, amt, comment in zip(expense_type_ids, amounts, comments):
                if et_id and amt:
                    pending_expense = PendingSaleExpense(
                        pending_sale_id=pending_sale.id,
                        expense_type_id=int(et_id),
                        amount=float(amt),
                        comment=comment if comment else None
                    )
                    db.session.add(pending_expense)
            
            db.session.commit()
            
            # Отправляем уведомление в телеграм бота
            print(f"[FLASK] Создана заявка #{pending_sale.id}, отправка уведомления в бот...")
            try:
                from telegram_bot import send_pending_sale_notification_async
                print(f"[FLASK] Функция send_pending_sale_notification_async импортирована успешно")
                send_pending_sale_notification_async(pending_sale.id)
                print(f"[FLASK] Функция send_pending_sale_notification_async вызвана для заявки #{pending_sale.id}")
            except Exception as e:
                import traceback
                print(f"[FLASK] ❌ Ошибка при отправке уведомления: {e}")
                traceback.print_exc()
                # Если не удалось отправить напрямую, добавляем в очередь
                try:
                    from bot_notification_queue import add_notification
                    add_notification('sale', pending_sale.id)
                    print(f"[FLASK] Уведомление добавлено в очередь для обработки ботом")
                except Exception as queue_error:
                    print(f"[FLASK] ❌ Ошибка при добавлении в очередь: {queue_error}")
            
            flash('Заявка отправлена на одобрение! Ожидайте решения администратора.', 'info')
            return redirect(url_for('sales'))

    # Для GET запроса - автозаполняем форму данными из стока или предоплаты
    stock_data = {}
    prepayment_data = {}
    if stock_item:
        _stock_inv = Investor.query.get(stock_item.investor_id) if stock_item.investor_id else None
        stock_data = {
            'product_name': stock_item.product_name,
            'reference': stock_item.reference or '',
            'item_year': stock_item.item_year,
            'buy_price': stock_item.buy_price,
            'sell_price': stock_item.expected_sell_price,
            'city_id': stock_item.city_id,
            'investor_id': stock_item.investor_id,
            'photo': stock_item.photo,
            'stock_id': stock_item.id,
            'expenses': stock_item.expenses,
            'komissionnyy': stock_item.komissionnyy,
            'investor_is_commission_client': bool(_stock_inv and _stock_inv.is_commission_client)
        }

    prepayment_client = None
    if prepayment_item:
        prepayment_data = {
            'prepayment_id': prepayment_item.id,
            'product_name': prepayment_item.product_name,
            'reference': prepayment_item.reference or '',
            'item_year': prepayment_item.item_year,
            'buy_price': prepayment_item.buy_price if prepayment_item.buy_price else prepayment_item.prepayment_amount,
            'sell_price': prepayment_item.sell_price,
            'city_id': prepayment_item.city_id,
            'investor_id': prepayment_item.investor_id,
            'client_id': prepayment_item.client_id,
            'employee_id': prepayment_item.employee_id,
            'photo': prepayment_item.photo,
            'komplektatsiya': prepayment_item.komplektatsiya,
            'komissionnyy': prepayment_item.komissionnyy,
            'expenses': prepayment_item.expenses_data,
            'bonus': prepayment_item.bonus if prepayment_item.bonus else 0,
            'date': prepayment_item.date.strftime('%Y-%m-%d') if prepayment_item.date else None
        }
        # Используем prepayment_data вместо stock_data
        stock_data = prepayment_data
        # Получаем объект клиента для автозаполнения
        if prepayment_item.client_id:
            prepayment_client = Client.query.get(prepayment_item.client_id)

    new_client = Client.query.get(new_client_id) if new_client_id else None
    return render_template('add_sale.html', 
                         cities=cities, 
                         expense_types=expense_types, 
                         employees=employees, 
                         investors=investors,
                         clients=clients_list,
                         stock_data=stock_data,
                         prepayment_client=prepayment_client,
                         new_client=new_client,
                         is_admin=is_admin,
                         current_user_employee=current_employee,
                         product_categories=PRODUCT_CATEGORIES)


@app.route('/add_stock', methods=['GET', 'POST'])
@login_required
def add_stock():
    cities = City.query.all()
    expense_types = ExpenseType.query.all()
    investors = get_active_investors()
    clients_list = Client.query.order_by(Client.full_name).all()
    is_admin = session.get('is_admin', False)
    
    # Проверяем, это комиссионный сток?
    is_commission = request.args.get('commission') == '1' or request.form.get('komissionnyy') == '1'

    if request.method == 'POST':
        product_name = request.form['product_name']
        reference = request.form.get('reference', '')
        item_year = int(request.form['item_year']) if request.form.get('item_year') else None
        expected_sell_price = float(request.form['expected_sell_price'])
        komplektatsiya = request.form.get('komplektatsiya', '').strip() or None  # Комплектация
        komissionnyy = request.form.get('komissionnyy') == '1'  # Комиссионный товар
        stock_category = request.form.get('category') or None
        city_id = int(request.form['city_id'])
        quantity = int(request.form.get('quantity', 1))
        
        # Для комиссионного стока: выплата и данные клиента (ФИО и контакты)
        # Для общего стока: покупка и инвестор
        if komissionnyy:
            # Комиссионный сток: выплата и данные клиента/инвестора
            buy_price = None
            if request.form.get('buy_price'):  # Здесь buy_price используется как выплата
                try:
                    buy_price = float(request.form['buy_price'])
                except (ValueError, TypeError):
                    buy_price = None
            # Получаем данные клиента из формы
            client_full_name = request.form.get('client_full_name', '').strip() or None
            client_phone = request.form.get('client_phone', '').strip() or None
            client_instagram = request.form.get('client_instagram', '').strip() or None
            client_telegram = request.form.get('client_telegram', '').strip() or None
            client_email = request.form.get('client_email', '').strip() or None
            client_id = None
            # Если выбран инвестор — сохраняем investor_id (менеджер и админ могут выбрать)
            investor_id = int(request.form['commission_investor_id']) if request.form.get('commission_investor_id') else None
        else:
            # Общий сток: покупка и инвестор (необязательно)
            buy_price = None
            if request.form.get('buy_price'):
                try:
                    buy_price = float(request.form['buy_price'])
                except (ValueError, TypeError):
                    buy_price = None
            investor_id = int(request.form['investor_id']) if request.form.get('investor_id') else None
            client_id = None
            client_full_name = None
            client_phone = None
            client_instagram = None
            client_telegram = None
            client_email = None

        photo_path = None
        photo = request.files['photo']
        if photo and allowed_file(photo.filename):
            filename = generate_unique_filename(photo.filename)
            photo_path = filename
            full_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            photo.save(full_path)
            # Оптимизируем изображение после сохранения
            optimize_image(full_path)

        # И админ, и менеджер создают сток сразу (без заявки на одобрение)
        comment = request.form.get('comment', '').strip() or None
        new_stock = StockItem(
            photo=photo_path,
            product_name=product_name,
            reference=reference,
            item_year=item_year,
            buy_price=buy_price,
            expected_sell_price=expected_sell_price,
            quantity=quantity,
            komplektatsiya=komplektatsiya,
            komissionnyy=komissionnyy,
            city_id=city_id,
            investor_id=investor_id,
            client_id=client_id,
            client_full_name=client_full_name,
            client_phone=client_phone,
            client_instagram=client_instagram,
            client_telegram=client_telegram,
            client_email=client_email,
            category=stock_category,
            comment=comment
        )
        db.session.add(new_stock)
        db.session.flush()

        expense_type_ids = request.form.getlist('expense_type_id')
        amounts = request.form.getlist('expense_amount')
        for et_id, amt in zip(expense_type_ids, amounts):
            if et_id and amt:
                stock_expense = StockExpense(
                    stock_item_id=new_stock.id,
                    expense_type_id=int(et_id),
                    amount=float(amt)
                )
                db.session.add(stock_expense)

        db.session.commit()
        flash('Сток успешно добавлен!', 'success')
        return redirect(url_for('stock'))

    return render_template('add_stock.html', 
                         cities=cities, 
                         expense_types=expense_types, 
                         investors=investors, 
                         clients=clients_list,
                         is_admin=is_admin,
                         is_commission=is_commission,
                         product_categories=PRODUCT_CATEGORIES)


@app.route('/edit_stock/<int:stock_id>', methods=['GET', 'POST'])
@login_required
def edit_stock(stock_id):
    stock_item = StockItem.query.get_or_404(stock_id)
    cities = City.query.all()
    investors = get_active_investors()
    clients_list = Client.query.order_by(Client.full_name).all()
    expense_types = ExpenseType.query.all()
    is_commission = stock_item.komissionnyy

    if request.method == 'POST':
        stock_item.product_name = request.form['product_name']
        stock_item.reference = request.form.get('reference', '')
        stock_item.item_year = int(request.form['item_year']) if request.form.get('item_year') else None
        buy_price_val = request.form.get('buy_price')
        stock_item.buy_price = float(buy_price_val) if buy_price_val else None
        stock_item.expected_sell_price = float(request.form['expected_sell_price'])
        stock_item.quantity = int(request.form.get('quantity', 1))
        stock_item.komplektatsiya = request.form.get('komplektatsiya', '').strip() or None  # Комплектация
        stock_item.city_id = int(request.form['city_id'])
        
        # Для комиссионного стока: данные клиента (ФИО и контакты), для общего: инвестор
        if is_commission:
            stock_item.client_full_name = request.form.get('client_full_name', '').strip() or None
            stock_item.client_phone = request.form.get('client_phone', '').strip() or None
            stock_item.client_instagram = request.form.get('client_instagram', '').strip() or None
            stock_item.client_telegram = request.form.get('client_telegram', '').strip() or None
            stock_item.client_email = request.form.get('client_email', '').strip() or None
            stock_item.client_id = None
            # Если выбран инвестор — сохраняем investor_id
            stock_item.investor_id = int(request.form['commission_investor_id']) if request.form.get('commission_investor_id') else None
        else:
            stock_item.investor_id = int(request.form['investor_id']) if request.form.get('investor_id') else None
            stock_item.client_id = None
            stock_item.client_full_name = None
            stock_item.client_phone = None
            stock_item.client_instagram = None
            stock_item.client_telegram = None
            stock_item.client_email = None
        
        stock_item.comment = request.form.get('comment', '').strip() or None
        stock_item.category = request.form.get('category') or None

        photo = request.files['photo']
        if photo and allowed_file(photo.filename):
            # Удаляем старое фото если есть
            if stock_item.photo:
                old_path = os.path.join(app.config['UPLOAD_FOLDER'], stock_item.photo)
                if os.path.exists(old_path):
                    os.remove(old_path)
            
            filename = generate_unique_filename(photo.filename)
            full_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            photo.save(full_path)
            # Оптимизируем изображение после сохранения
            optimize_image(full_path)
            stock_item.photo = filename

        # Обновление расходов
        for exp in stock_item.expenses:
            db.session.delete(exp)
        expense_type_ids = request.form.getlist('expense_type_id')
        amounts = request.form.getlist('expense_amount')
        for et_id, amt in zip(expense_type_ids, amounts):
            if et_id and amt:
                stock_expense = StockExpense(
                    stock_item_id=stock_item.id,
                    expense_type_id=int(et_id),
                    amount=float(amt)
                )
                db.session.add(stock_expense)

        db.session.commit()
        flash('Сток обновлен!', 'success')
        return redirect(url_for('stock'))

    return render_template('edit_stock.html', 
                         stock_item=stock_item, 
                         cities=cities, 
                         investors=investors, 
                         clients=clients_list,
                         expense_types=expense_types,
                         is_commission=is_commission,
                         product_categories=PRODUCT_CATEGORIES)


@app.route('/sell_stock/<int:stock_id>', methods=['POST'])
@login_required
def sell_stock(stock_id):
    stock_item = StockItem.query.get_or_404(stock_id)
    stock_item.sold = True
    db.session.commit()
    flash('Позиция помечена как проданная!', 'success')
    return redirect(request.referrer or url_for('stock'))


@app.route('/delete_stock/<int:stock_id>', methods=['POST'])
@login_required
def delete_stock(stock_id):
    """Удаление позиции из стока"""
    stock_item = StockItem.query.get_or_404(stock_id)
    is_admin = session.get('is_admin', False)
    
    # Только администратор может удалять сток
    if not is_admin:
        flash('У вас нет прав для удаления позиции из стока.', 'error')
        return redirect(request.referrer or url_for('stock'))
    
    try:
        # Удаляем фото, если оно было загружено
        if stock_item.photo:
            photo_path = os.path.join(app.config['UPLOAD_FOLDER'], stock_item.photo)
            if os.path.exists(photo_path):
                try:
                    os.remove(photo_path)
                except:
                    pass
        
        # Удаляем связанные расходы
        StockExpense.query.filter_by(stock_item_id=stock_item.id).delete()
        
        # Удаляем позицию
        db.session.delete(stock_item)
        db.session.commit()
        
        flash('Позиция удалена из стока!', 'success')
    except Exception as e:
        db.session.rollback()
        print(f"Ошибка при удалении позиции: {e}")
        import traceback
        traceback.print_exc()
        flash('Ошибка при удалении позиции', 'error')
    
    return redirect(request.referrer or url_for('stock'))


@app.route('/edit_sale/<int:sale_id>', methods=['GET', 'POST'])
@login_required
def edit_sale(sale_id):
    sale = Sale.query.get_or_404(sale_id)
    new_client_id = request.args.get('new_client_id', type=int)
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    
    # Менеджер может редактировать только свои продажи
    if not is_admin and current_employee and sale.employee_id != current_employee.id:
        flash('У вас нет прав для редактирования этой продажи.', 'error')
        return redirect(url_for('sales'))
    
    cities = City.query.all()
    expense_types = ExpenseType.query.all()
    employees = get_active_employees()
    investors = get_active_investors()
    clients_list = Client.query.order_by(Client.full_name).all()

    if request.method == 'POST':
        sale.product_name = request.form['product_name']
        sale.reference = request.form.get('reference', '')
        sale.item_year = int(request.form['item_year']) if request.form.get('item_year') else None
        sale.buy_price = float(request.form['buy_price'])
        sale.sell_price = float(request.form['sell_price'])
        sale.bonus = float(request.form.get('bonus', 0) or 0)  # Бонус менеджера
        sale.komplektatsiya = request.form.get('komplektatsiya', '').strip() or None  # Комплектация
        sale.city_id = int(request.form['city_id'])
        sale_category = request.form.get('category') or None
        sale.murad_bonus = get_murad_bonus_for_city(sale.city_id, sale_category)
        # Только админ может менять продавца
        if is_admin:
            sale.employee_id = int(request.form['employee_id'])
        # Только админ может менять инвестора
        if is_admin:
            sale.investor_id = int(request.form.get('investor_id')) if request.form.get('investor_id') else None
        sale.client_id = int(request.form.get('client_id')) if request.form.get('client_id') else None
        date_str = request.form['date']
        sale.date = datetime.strptime(date_str, '%Y-%m-%d') if date_str else datetime.utcnow()
        sale.comment = request.form.get('comment', '').strip() or None
        sale.category = sale_category

        photo = request.files['photo']
        if photo and allowed_file(photo.filename):
            # Удаляем старое фото если есть
            if sale.photo:
                old_path = os.path.join(app.config['UPLOAD_FOLDER'], sale.photo)
                if os.path.exists(old_path):
                    os.remove(old_path)
            
            filename = generate_unique_filename(photo.filename)
            full_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            photo.save(full_path)
            # Оптимизируем изображение после сохранения
            optimize_image(full_path)
            sale.photo = filename

        # Обновление расходов
        for exp in sale.expenses:
            db.session.delete(exp)
        expense_type_ids = request.form.getlist('expense_type_id')
        amounts = request.form.getlist('expense_amount')
        for et_id, amt in zip(expense_type_ids, amounts):
            if et_id and amt:
                expense = Expense(sale_id=sale.id, expense_type_id=int(et_id), amount=float(amt))
                db.session.add(expense)

        db.session.commit()
        flash('Продажа обновлена!', 'success')
        return redirect(url_for('sales'))

    new_client = Client.query.get(new_client_id) if new_client_id else None
    return render_template('edit_sale.html', sale=sale, cities=cities, expense_types=expense_types, employees=employees, investors=investors, clients=clients_list, new_client=new_client, is_admin=is_admin, product_categories=PRODUCT_CATEGORIES)


@app.route('/delete_sale/<int:sale_id>', methods=['POST'])
@login_required
def delete_sale(sale_id):
    sale = Sale.query.get_or_404(sale_id)
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    
    # Менеджер может удалять только свои продажи
    if not is_admin and current_employee and sale.employee_id != current_employee.id:
        flash('У вас нет прав для удаления этой продажи.', 'error')
        return redirect(url_for('sales'))

    if sale.photo:
        photo_path = os.path.join(app.config['UPLOAD_FOLDER'], sale.photo)
        if os.path.exists(photo_path):
            os.remove(photo_path)

    db.session.delete(sale)
    db.session.commit()
    flash('Продажа удалена!', 'success')
    return redirect(url_for('sales'))


@app.route('/stats', methods=['GET', 'POST'])
@login_required
def stats():
    # Параметры периода и города
    period_type = request.args.get('period_type', 'current_month')
    year = request.args.get('year')
    month = request.args.get('month')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    city_filter = request.args.get('city', 'all')
    expense_type_filter = request.args.get('expense_type', 'all')
    # Источник расходов: все, только по изделиям (из продаж) или только общие
    expense_source_filter = request.args.get('expense_source', 'all')

    # Определяем даты
    if period_type == 'custom' and start_date and end_date:
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        end = end + relativedelta(days=1)
    else:
        today = date.today()
        if year and month:
            try:
                selected_date = date(int(year), int(month), 1)
            except:
                selected_date = today
        else:
            selected_date = today
        start = selected_date.replace(day=1)
        end = start + relativedelta(months=1)

    # Фильтры для продаж и расходов (менеджер видит только свои)
    sales_query = Sale.query.filter(Sale.date >= start, Sale.date < end)
    current_employee = get_current_user_employee()
    if current_employee:
        sales_query = sales_query.filter(Sale.employee_id == current_employee.id)
    general_exp_query = GeneralExpense.query.filter(GeneralExpense.date >= start, GeneralExpense.date < end)

    if city_filter != 'all':
        sales_query = sales_query.filter(Sale.city_id == City.query.filter_by(name=city_filter).first().id)
        general_exp_query = general_exp_query.filter(GeneralExpense.city_id == City.query.filter_by(name=city_filter).first().id)

    sales = sales_query.all()
    general_expenses = general_exp_query.all()

    # Группируем расходы по датам
    expenses_by_date = {}
    total_expenses = 0
    
    # Расходы из продаж (используем дату продажи)
    for s in sales:
        # Если нужен только блок общих расходов — пропускаем расходы, привязанные к продажам
        if expense_source_filter == 'general':
            continue
        sale_date = s.date.date() if isinstance(s.date, datetime) else s.date
        for e in s.expenses:
            # Применяем фильтр по типу расхода
            if expense_type_filter != 'all':
                try:
                    if e.expense_type_id != int(expense_type_filter):
                        continue
                except:
                    continue
            if sale_date not in expenses_by_date:
                expenses_by_date[sale_date] = []
            expenses_by_date[sale_date].append({
                'id': e.id,
                'type': e.expense_type.name,
                'amount': e.amount,
                'description': e.comment,  # Комментарий из расходов продажи
                'is_sale_expense': True
            })
            total_expenses += e.amount
    
    # Общие расходы (используем дату расхода)
    for g in general_expenses:
        # Если нужны только расходы по изделиям (из продаж) — пропускаем общие расходы
        if expense_source_filter == 'sale':
            continue
        # Применяем фильтр по типу расхода
        if expense_type_filter != 'all':
            try:
                if g.expense_type_id != int(expense_type_filter):
                    continue
            except:
                continue
        exp_date = g.date.date() if isinstance(g.date, datetime) else g.date
        if exp_date not in expenses_by_date:
            expenses_by_date[exp_date] = []
        expenses_by_date[exp_date].append({
            'id': g.id,
            'type': g.expense_type.name,
            'amount': g.amount,
            'description': g.description,  # Комментарий для общих расходов
            'is_sale_expense': False
        })
        total_expenses += g.amount
    
    # Сортируем даты по убыванию (новые сначала)
    sorted_expenses_by_date = sorted(expenses_by_date.items(), key=lambda x: x[0], reverse=True)

    # Общая чистая прибыль (для полноты, но фокус на расходах)
    gross_income = sum(s.sell_price - s.buy_price for s in sales)
    total_buy = sum(s.buy_price for s in sales)
    total_bonuses = sum((s.bonus if s.bonus else 0.0) + (getattr(s, 'murad_bonus', 0) or 0.0) for s in sales)  # Бонусы менеджеров + бонус Мурада
    net_profit = gross_income - total_expenses - total_bonuses

    # Период лейбл
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    month_name = months_ru[start.month - 1]
    if period_type == 'custom':
        period_label = f"{start.strftime('%d.%m.%Y')} — {(end - relativedelta(days=1)).strftime('%d.%m.%Y')}"
    else:
        period_label = f"{month_name} {start.year}"

    all_years = sorted(set(s.date.year for s in Sale.query.with_entities(Sale.date).all()), reverse=True)
    if not all_years:
        all_years = [date.today().year]
    all_cities = sorted([c.name for c in City.query.all()])

    expense_types = ExpenseType.query.all()
    cities = City.query.all()

    return render_template('stats.html',
                           total_expenses=round(total_expenses, 2),
                           expenses_by_date=sorted_expenses_by_date,
                           period_label=period_label,
                           all_years=all_years,
                           selected_year=start.year,
                           selected_month=start.month,
                           period_type=period_type,
                           city_filter=city_filter,
                           expense_type_filter=expense_type_filter,
                           expense_source_filter=expense_source_filter,
                           all_cities=all_cities,
                           expense_types=expense_types,
                           cities=cities,
                           net_profit=round(net_profit, 2))


@app.route('/delete_expense', methods=['POST'])
@login_required
def delete_expense():
    """Удалить один расход со страницы расходов (как из продаж, так и общий)."""
    expense_id = request.form.get('expense_id')
    source = request.form.get('source')
    if not expense_id or source not in ('sale', 'general'):
        flash('Некорректный запрос на удаление расхода.', 'error')
        return redirect(request.referrer or url_for('stats'))
    try:
        if source == 'sale':
            expense = Expense.query.get_or_404(int(expense_id))
        else:
            expense = GeneralExpense.query.get_or_404(int(expense_id))
        db.session.delete(expense)
        db.session.commit()
        flash('Расход удалён.', 'success')
    except Exception as e:
        db.session.rollback()
        print(f'Ошибка при удалении расхода: {e}')
        flash('Не удалось удалить расход.', 'error')
    return redirect(request.referrer or url_for('stats'))


# СТРАНИЦА СТАТИСТИКИ МЕНЕДЖЕРА (БОНУСЫ)
@app.route('/manager_menu', methods=['GET'])
@login_required
def manager_menu():
    """Промежуточная страница для выбора между Бонусами и Каналами"""
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    
    # Менеджер видит только свои данные, админ может выбрать менеджера
    if not is_admin:
        if not current_employee:
            flash('Обратитесь к администратору для настройки вашего профиля продавца.', 'error')
            return redirect(url_for('dashboard'))
    
    return render_template('manager_menu.html', 
                         is_admin=is_admin,
                         current_employee=current_employee)

@app.route('/add_manual_bonus', methods=['POST'])
@login_required
def add_manual_bonus():
    """Выдать ручной бонус менеджеру (только для админа)"""
    if not session.get('is_admin', False):
        return jsonify({'error': 'Нет прав'}), 403
    try:
        employee_id = int(request.form.get('employee_id', 0))
        amount = float(request.form.get('amount', 0))
        date_str = request.form.get('date', '')
        comment = request.form.get('comment', '').strip() or None
        if not employee_id or amount <= 0:
            return jsonify({'error': 'Неверные данные'}), 400
        bonus_date = datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else date.today()
        mb = ManualBonus(employee_id=employee_id, amount=amount, date=bonus_date, comment=comment)
        db.session.add(mb)
        db.session.commit()
        from telegram_bot import send_manual_bonus_notification_async
        send_manual_bonus_notification_async(mb.id)
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/manager_stats', methods=['GET'])
@login_required
def manager_stats():
    # Параметры фильтрации
    year_filter = request.args.get('year', 'all')
    month_filter = request.args.get('month', 'all')
    employee_filter = request.args.get('employee', 'all')
    
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    is_top_manager = session.get('is_top_manager', False)
    
    # Топ-менеджер (Мурад) всегда видит только свою статистику бонусов (бонус по Москве), даже если он админ
    if is_top_manager:
        # Показываем только бонусы Мурада
        start, end, period_label = _manager_stats_period(year_filter, month_filter)
        sales_murad = Sale.query.filter(
            Sale.date >= start, Sale.date < end,
            Sale.murad_bonus > 0
        ).order_by(Sale.date.desc()).all()
        murad_bonuses_by_date = {}
        total_murad_bonus = 0.0
        for sale in sales_murad:
            sale_date = sale.date.date() if isinstance(sale.date, datetime) else sale.date
            if sale_date not in murad_bonuses_by_date:
                murad_bonuses_by_date[sale_date] = []
            murad_bonuses_by_date[sale_date].append({
                'product_name': sale.product_name,
                'reference': sale.reference,
                'sell_price': sale.sell_price,
                'bonus': sale.murad_bonus,
                'is_manual': False,
                'is_extra': False,
                'initial_bonus': 0.0,
                'bonus_main': None,
                'bonus_murad': sale.murad_bonus,
                'bonus_extra_sum': 0,
            })
            total_murad_bonus += sale.murad_bonus
        sorted_murad_by_date = sorted(murad_bonuses_by_date.items(), key=lambda x: x[0], reverse=True)
        all_years = sorted(set(s.date.year for s in Sale.query.with_entities(Sale.date).all()), reverse=True) or [date.today().year]
        return render_template('manager_stats.html',
                               total_bonus=round(total_murad_bonus, 2),
                               bonuses_by_date=sorted_murad_by_date,
                               period_label=period_label,
                               all_years=all_years,
                               selected_year=year_filter,
                               selected_month=month_filter,
                               employee_filter='all',
                               all_employees=[],
                               is_admin=False,
                               is_top_manager_stats=True)
    
    # Менеджер видит только свои бонусы, админ может выбрать менеджера
    if not is_admin:
        if not current_employee:
            flash('Обратитесь к администратору для настройки вашего профиля продавца.', 'error')
            return redirect(url_for('dashboard'))
        employee_filter = str(current_employee.id)
    
    # Определяем период
    if year_filter != 'all' and month_filter != 'all':
        try:
            start = date(int(year_filter), int(month_filter), 1)
            end = start + relativedelta(months=1)
        except:
            start = date.today().replace(day=1)
            end = start + relativedelta(months=1)
    elif year_filter != 'all':
        try:
            start = date(int(year_filter), 1, 1)
            end = date(int(year_filter) + 1, 1, 1)
        except:
            start = date.today().replace(day=1)
            end = start + relativedelta(months=1)
    else:
        # Все время
        start = date(2000, 1, 1)
        end = date.today() + relativedelta(days=1)
    
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    if year_filter != 'all' and month_filter != 'all':
        month_name = months_ru[int(month_filter) - 1]
        period_label = f"{month_name} {year_filter}"
    elif year_filter != 'all':
        period_label = f"{year_filter} год"
    else:
        period_label = "Все время"
    
    # Админ выбрал «Мурад» — показываем только бонусы Мурада (по Москве)
    if is_admin and employee_filter == 'murad':
        sales_murad = Sale.query.filter(
            Sale.date >= start, Sale.date < end,
            Sale.murad_bonus > 0
        ).order_by(Sale.date.desc()).all()
        murad_bonuses_by_date = {}
        total_murad_bonus = 0.0
        for sale in sales_murad:
            sale_date = sale.date.date() if isinstance(sale.date, datetime) else sale.date
            if sale_date not in murad_bonuses_by_date:
                murad_bonuses_by_date[sale_date] = []
            murad_bonuses_by_date[sale_date].append({
                'product_name': sale.product_name,
                'reference': sale.reference,
                'sell_price': sale.sell_price,
                'bonus': sale.murad_bonus,
                'is_manual': False,
                'is_extra': False,
                'initial_bonus': 0.0,
                'bonus_main': None,
                'bonus_murad': sale.murad_bonus,
                'bonus_extra_sum': 0,
            })
            total_murad_bonus += sale.murad_bonus
        sorted_murad_by_date = sorted(murad_bonuses_by_date.items(), key=lambda x: x[0], reverse=True)
        all_employees = sorted(get_active_employees(), key=lambda e: e.name)
        return render_template('manager_stats.html',
                               total_bonus=round(total_murad_bonus, 2),
                               bonuses_by_date=sorted_murad_by_date,
                               period_label=period_label,
                               all_years=sorted(set(s.date.year for s in Sale.query.with_entities(Sale.date).all()), reverse=True) or [date.today().year],
                               selected_year=year_filter,
                               selected_month=month_filter,
                               employee_filter='murad',
                               all_employees=all_employees,
                               is_admin=True,
                               is_top_manager_stats=True)
    
    # Для админа при выборе «Все»: одна строка на продажу с полной суммой (основной + Мурад + доп.)
    if is_admin and employee_filter == 'all':
        all_sales = Sale.query.filter(Sale.date >= start, Sale.date < end).order_by(Sale.date.desc()).all()
        bonuses_by_date = {}
        total_bonus = 0.0
        for sale in all_sales:
            main_b = (sale.bonus or 0.0)
            murad_b = (getattr(sale, 'murad_bonus', None) or 0.0)
            extra_sum = sum(ab.amount for ab in (getattr(sale, 'additional_bonuses', []) or []))
            total_sale_bonus = main_b + murad_b + extra_sum
            if total_sale_bonus <= 0:
                continue
            sale_date = sale.date.date() if isinstance(sale.date, datetime) else sale.date
            if sale_date not in bonuses_by_date:
                bonuses_by_date[sale_date] = []
            initial_b = getattr(sale, 'initial_bonus', None) or 0.0
            bonuses_by_date[sale_date].append({
                'product_name': sale.product_name,
                'reference': sale.reference,
                'sell_price': sale.sell_price,
                'bonus': total_sale_bonus,
                'initial_bonus': initial_b,
                'is_extra': False,
                'is_manual': False,
                'bonus_main': main_b,
                'bonus_murad': murad_b,
                'bonus_extra_sum': extra_sum,
            })
            total_bonus += total_sale_bonus
        # Ручные бонусы всех менеджеров за период
        manual_bonuses = ManualBonus.query.filter(ManualBonus.date >= start, ManualBonus.date < end).all()
        for mb in manual_bonuses:
            if mb.date not in bonuses_by_date:
                bonuses_by_date[mb.date] = []
            bonuses_by_date[mb.date].append({
                'product_name': f"Ручной бонус ({mb.employee.name})",
                'reference': None,
                'sell_price': 0,
                'bonus': mb.amount,
                'initial_bonus': 0.0,
                'is_extra': False,
                'is_manual': True,
                'comment': mb.comment,
                'bonus_main': None,
                'bonus_murad': None,
                'bonus_extra_sum': 0,
            })
            total_bonus += mb.amount
        sorted_bonuses_by_date = sorted(bonuses_by_date.items(), key=lambda x: x[0], reverse=True)
        all_years = sorted(set(s.date.year for s in Sale.query.with_entities(Sale.date).all()), reverse=True)
        if not all_years:
            all_years = [date.today().year]
        all_employees = sorted(get_active_employees(), key=lambda e: e.name) if is_admin else []
        return render_template('manager_stats.html',
                               total_bonus=round(total_bonus, 2),
                               bonuses_by_date=sorted_bonuses_by_date,
                               period_label=period_label,
                               all_years=all_years,
                               selected_year=year_filter,
                               selected_month=month_filter,
                               employee_filter=employee_filter,
                               all_employees=all_employees,
                               is_admin=is_admin,
                               is_top_manager_stats=False,
                               today_date=date.today().isoformat())
    
    # Запрос продаж с бонусами
    sales_query = Sale.query.filter(Sale.date >= start, Sale.date < end, Sale.bonus > 0)
    
    # Фильтр по менеджеру
    if employee_filter != 'all':
        try:
            employee_id = int(employee_filter)
            sales_query = sales_query.filter(Sale.employee_id == employee_id)
        except:
            pass
    
    sales_with_bonuses = sales_query.order_by(Sale.date.desc()).all()
    
    # Группируем бонусы по датам
    bonuses_by_date = {}
    total_bonus = 0
    # Для админа по одному менеджеру: объединяем основной и доп. бонус по одной продаже в одну строку
    effective_employee_id = None
    if employee_filter != 'all':
        try:
            effective_employee_id = int(employee_filter)
        except (ValueError, TypeError):
            pass
    elif not is_admin:
        current_emp = get_current_user_employee()
        if current_emp:
            effective_employee_id = current_emp.id
    
    # Доп. бонусы по продажам для этого менеджера (sale_id -> сумма доп. бонуса)
    extra_by_sale = {}
    if effective_employee_id is not None:
        extra_rows = db.session.query(SaleAdditionalBonus.sale_id, SaleAdditionalBonus.amount).filter(
            SaleAdditionalBonus.employee_id == effective_employee_id
        ).join(Sale, SaleAdditionalBonus.sale_id == Sale.id).filter(
            Sale.date >= start,
            Sale.date < end
        ).all()
        for sid, amt in extra_rows:
            extra_by_sale[sid] = extra_by_sale.get(sid, 0) + amt
    
    for sale in sales_with_bonuses:
        sale_date = sale.date.date() if isinstance(sale.date, datetime) else sale.date
        if sale_date not in bonuses_by_date:
            bonuses_by_date[sale_date] = []
        initial_b = getattr(sale, 'initial_bonus', None) or 0.0
        extra_on_sale = extra_by_sale.pop(sale.id, 0)  # основной + доп. по этой продаже в одну строку
        row_bonus = sale.bonus + extra_on_sale
        bonuses_by_date[sale_date].append({
            'product_name': sale.product_name,
            'reference': sale.reference,
            'sell_price': sale.sell_price,
            'bonus': row_bonus,
            'initial_bonus': initial_b,
            'is_extra': False,
            'is_manual': False,
            'bonus_main': sale.bonus,
            'bonus_murad': None,
            'bonus_extra_sum': extra_on_sale,
        })
        total_bonus += row_bonus
    
    # Доп. бонусы по продажам, где менеджер не основной (только доп.) — оставшиеся в extra_by_sale
    if effective_employee_id is not None and extra_by_sale:
        sales_only_extra = Sale.query.filter(Sale.id.in_(extra_by_sale.keys())).all()
        for sale in sales_only_extra:
            amt = extra_by_sale[sale.id]
            sale_date = sale.date.date() if isinstance(sale.date, datetime) else sale.date
            if sale_date not in bonuses_by_date:
                bonuses_by_date[sale_date] = []
            bonuses_by_date[sale_date].append({
                'is_manual': False,
                'product_name': sale.product_name,
                'reference': sale.reference,
                'sell_price': sale.sell_price,
                'bonus': amt,
                'initial_bonus': 0.0,
                'is_extra': True,
                'bonus_main': None,
                'bonus_murad': None,
                'bonus_extra_sum': amt,
            })
            total_bonus += amt
    
    # Ручные бонусы для конкретного менеджера за период
    if effective_employee_id is not None:
        manual_bonuses = ManualBonus.query.filter(
            ManualBonus.employee_id == effective_employee_id,
            ManualBonus.date >= start,
            ManualBonus.date < end
        ).all()
        for mb in manual_bonuses:
            if mb.date not in bonuses_by_date:
                bonuses_by_date[mb.date] = []
            bonuses_by_date[mb.date].append({
                'product_name': 'Ручной бонус',
                'reference': None,
                'sell_price': 0,
                'bonus': mb.amount,
                'initial_bonus': 0.0,
                'is_extra': False,
                'is_manual': True,
                'comment': mb.comment,
                'bonus_main': None,
                'bonus_murad': None,
                'bonus_extra_sum': 0,
            })
            total_bonus += mb.amount

    # Сортируем даты по убыванию
    sorted_bonuses_by_date = sorted(bonuses_by_date.items(), key=lambda x: x[0], reverse=True)
    
    # Список годов для фильтра
    all_years = sorted(set(s.date.year for s in Sale.query.with_entities(Sale.date).all()), reverse=True)
    if not all_years:
        all_years = [date.today().year]
    
    # Список менеджеров для админа
    all_employees = sorted(get_active_employees(), key=lambda e: e.name) if is_admin else []
    
    return render_template('manager_stats.html',
                           total_bonus=round(total_bonus, 2),
                           bonuses_by_date=sorted_bonuses_by_date,
                           period_label=period_label,
                           all_years=all_years,
                           selected_year=year_filter,
                           selected_month=month_filter,
                           employee_filter=employee_filter,
                           all_employees=all_employees,
                           is_admin=is_admin,
                           is_top_manager_stats=False,
                           today_date=date.today().isoformat())

@app.route('/channels', methods=['GET', 'POST'])
@login_required
def channels():
    """Страница статистики по каналам привлечения клиентов"""
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    
    # Параметры фильтрации
    year_filter = request.args.get('year', 'all')
    month_filter = request.args.get('month', 'all')
    employee_filter = request.args.get('employee', 'all')
    
    # Менеджер видит только свои данные
    if not is_admin:
        if not current_employee:
            flash('Обратитесь к администратору для настройки вашего профиля продавца.', 'error')
            return redirect(url_for('dashboard'))
        employee_filter = str(current_employee.id)
    
    # Обработка добавления данных
    if request.method == 'POST':
        try:
            stat_date = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
            employee_id = int(request.form['employee_id'])
            telegram_count = int(request.form.get('telegram_count', 0) or 0)
            instagram_count = int(request.form.get('instagram_count', 0) or 0)
            website_count = int(request.form.get('website_count', 0) or 0)
            phone_count = int(request.form.get('phone_count', 0) or 0)
            
            # Проверяем, есть ли уже запись за эту дату для этого менеджера
            existing = ChannelStats.query.filter_by(
                date=stat_date,
                employee_id=employee_id
            ).first()
            
            if existing:
                # Обновляем существующую запись
                existing.telegram_count = telegram_count
                existing.instagram_count = instagram_count
                existing.website_count = website_count
                existing.phone_count = phone_count
                existing.updated_at = datetime.utcnow()
            else:
                # Создаем новую запись
                new_stat = ChannelStats(
                    date=stat_date,
                    employee_id=employee_id,
                    telegram_count=telegram_count,
                    instagram_count=instagram_count,
                    website_count=website_count,
                    phone_count=phone_count
                )
                db.session.add(new_stat)
            
            db.session.commit()
            flash('Данные по каналам успешно сохранены!', 'success')
            return redirect(url_for('channels', year=year_filter, month=month_filter, employee=employee_filter))
        except Exception as e:
            db.session.rollback()
            print(f"Ошибка при сохранении данных по каналам: {e}")
            import traceback
            traceback.print_exc()
            flash('Ошибка при сохранении данных', 'error')
    
    # Определяем период
    if year_filter != 'all' and month_filter != 'all':
        try:
            start = date(int(year_filter), int(month_filter), 1)
            end = start + relativedelta(months=1)
        except:
            start = date.today().replace(day=1)
            end = start + relativedelta(months=1)
    elif year_filter != 'all':
        try:
            start = date(int(year_filter), 1, 1)
            end = date(int(year_filter) + 1, 1, 1)
        except:
            start = date.today().replace(day=1)
            end = start + relativedelta(months=1)
    else:
        # Все время
        start = date(2000, 1, 1)
        end = date.today() + relativedelta(days=1)
    
    # Запрос данных по каналам
    stats_query = ChannelStats.query.filter(ChannelStats.date >= start, ChannelStats.date < end)
    
    # Фильтр по менеджеру
    if employee_filter != 'all':
        try:
            employee_id = int(employee_filter)
            stats_query = stats_query.filter(ChannelStats.employee_id == employee_id)
        except:
            pass
    
    channel_stats = stats_query.order_by(ChannelStats.date.desc()).all()
    
    # Группируем по датам
    stats_by_date = {}
    total_telegram = 0
    total_instagram = 0
    total_website = 0
    total_phone = 0
    
    for stat in channel_stats:
        if stat.date not in stats_by_date:
            stats_by_date[stat.date] = []
        stats_by_date[stat.date].append(stat)
        total_telegram += stat.telegram_count
        total_instagram += stat.instagram_count
        total_website += stat.website_count
        total_phone += stat.phone_count
    
    # Сортируем даты по убыванию
    sorted_stats_by_date = sorted(stats_by_date.items(), key=lambda x: x[0], reverse=True)
    
    # Период для отображения
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    
    if year_filter != 'all' and month_filter != 'all':
        month_name = months_ru[int(month_filter) - 1]
        period_label = f"{month_name} {year_filter}"
    elif year_filter != 'all':
        period_label = f"{year_filter} год"
    else:
        period_label = "Все время"
    
    # Список годов для фильтра
    try:
        all_years = sorted(set(s.date.year for s in ChannelStats.query.with_entities(ChannelStats.date).all()), reverse=True)
        if not all_years:
            all_years = [date.today().year]
    except:
        all_years = [date.today().year]
    
    # Список менеджеров для админа
    all_employees = sorted(get_active_employees(), key=lambda e: e.name) if is_admin else []
    
    # Определяем выбранного менеджера для формы
    if not is_admin:
        if current_employee:
            selected_employee_id = current_employee.id
        else:
            flash('Обратитесь к администратору для настройки вашего профиля продавца.', 'error')
            return redirect(url_for('dashboard'))
    else:
        # Для админа
        if employee_filter != 'all':
            try:
                selected_employee_id = int(employee_filter)
            except:
                selected_employee_id = all_employees[0].id if all_employees else None
        else:
            selected_employee_id = all_employees[0].id if all_employees else None
    
    return render_template('channels.html',
                         stats_by_date=sorted_stats_by_date,
                         total_telegram=total_telegram,
                         total_instagram=total_instagram,
                         total_website=total_website,
                         total_phone=total_phone,
                         period_label=period_label,
                         all_years=all_years,
                         selected_year=year_filter,
                         selected_month=month_filter,
                         employee_filter=employee_filter,
                         all_employees=all_employees,
                         is_admin=is_admin,
                         selected_employee_id=selected_employee_id,
                         today=date.today())


# Добавление общего расхода
@app.route('/add_general_expense', methods=['POST'])
@login_required
def add_general_expense():
    expense_type_id = int(request.form['expense_type_id'])
    amount = float(request.form['amount'])
    date_str = request.form['date']
    date = datetime.strptime(date_str, '%Y-%m-%d') if date_str else datetime.utcnow()
    city_id = int(request.form['city_id']) if request.form['city_id'] != 'none' else None
    description = request.form.get('description', '')

    new_gen_exp = GeneralExpense(expense_type_id=expense_type_id, amount=amount, date=date, city_id=city_id, description=description)
    db.session.add(new_gen_exp)
    db.session.commit()
    flash('Общий расход добавлен!', 'success')
    return redirect(url_for('stats'))


# Маршруты для добавления городов, типов расходов и сотрудников (без изменений)
@app.route('/add_city', methods=['POST'])
@login_required
def add_city():
    name = request.form['name'].strip()
    if name and not City.query.filter_by(name=name).first():
        new_city = City(name=name)
        db.session.add(new_city)
        db.session.commit()
        flash('Город добавлен!', 'success')
    else:
        flash('Город уже существует или пустое имя.', 'error')
    return redirect(request.referrer or url_for('add_sale'))


@app.route('/add_expense_type', methods=['POST'])
@login_required
def add_expense_type():
    name = request.form['name'].strip()
    if name and not ExpenseType.query.filter_by(name=name).first():
        new_type = ExpenseType(name=name)
        db.session.add(new_type)
        db.session.commit()
        flash('Тип расхода добавлен!', 'success')
    else:
        flash('Тип уже существует или пустое имя.', 'error')
    return redirect(request.referrer or url_for('add_sale'))


@app.route('/add_employee', methods=['POST'])
@login_required
def add_employee():
    name = request.form['name'].strip()
    if name:
        new_employee = Employee(name=name)
        db.session.add(new_employee)
        db.session.commit()
        flash('Сотрудник добавлен!', 'success')
    else:
        flash('Пустое имя.', 'error')
    return redirect(request.referrer or url_for('add_sale'))


@app.route('/add_investor', methods=['POST'])
@login_required
def add_investor():
    name = request.form.get('name', '').strip()
    full_name = request.form.get('full_name', '').strip()
    phone = request.form.get('phone', '').strip()
    email = request.form.get('email', '').strip()

    if not name:
        flash('Имя обязательно для заполнения', 'error')
        ref = request.referrer or url_for('stock_investors')
        if '/stock_investors' in ref:
            return redirect(url_for('stock_investors'))
        return redirect(ref)

    if Investor.query.filter_by(name=name).first():
        flash('Инвестор с таким именем уже существует', 'error')
        ref = request.referrer or url_for('stock_investors')
        if '/stock_investors' in ref:
            return redirect(url_for('stock_investors'))
        return redirect(ref)

    is_commission_client = request.form.get('is_commission_client') == '1'
    new_investor = Investor(
        name=name,
        full_name=full_name or None,
        phone=phone or None,
        email=email or None,
        is_commission_client=is_commission_client
    )
    db.session.add(new_investor)
    db.session.commit()
    flash(f'Инвестор {name} успешно добавлен!', 'success')

    ref = request.referrer or url_for('add_stock')
    if '/stock_investors' in ref:
        return redirect(url_for('stock_investors'))
    return redirect(ref)


@app.route('/edit_investor/<int:investor_id>', methods=['POST'])
@login_required
def edit_investor(investor_id):
    investor = Investor.query.get_or_404(investor_id)
    new_name = request.form.get('name', '').strip()
    
    if not new_name:
        flash('Имя инвестора не может быть пустым', 'error')
        return redirect(request.referrer or url_for('stock_investors'))
    
    # Проверяем, не существует ли уже инвестор с таким именем
    existing = Investor.query.filter_by(name=new_name).first()
    if existing and existing.id != investor_id:
        flash('Инвестор с таким именем уже существует', 'error')
        return redirect(request.referrer or url_for('stock_investors'))
    
    investor.name = new_name
    db.session.commit()
    flash('Инвестор обновлен!', 'success')
    return redirect(url_for('stock_investors'))


@app.route('/add_client', methods=['POST'])
@login_required
def add_client():
    full_name = request.form.get('full_name', '').strip()
    phone = request.form.get('phone', '').strip() or None
    instagram = request.form.get('instagram', '').strip() or None
    telegram = request.form.get('telegram', '').strip() or None
    email = request.form.get('email', '').strip() or None
    birth_date_str = request.form.get('birth_date', '')
    birth_date = None
    if birth_date_str:
        try:
            birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d').date()
        except ValueError:
            pass
    if not full_name:
        flash('ФИО обязательно для заполнения', 'error')
        ref = request.referrer or url_for('clients')
        if '/clients' in ref:
            return redirect(url_for('clients'))
        return redirect(ref or url_for('add_sale'))
    has_contact = any([phone, instagram, telegram, email])
    if not has_contact:
        flash('Укажите хотя бы один способ связи: телефон, Instagram, Telegram или почту', 'error')
        ref = request.referrer or url_for('clients')
        if '/clients' in ref:
            return redirect(url_for('clients'))
        return redirect(ref or url_for('add_sale'))
    new_client = Client(
        full_name=full_name,
        phone=phone,
        instagram=instagram,
        telegram=telegram,
        email=email,
        birth_date=birth_date
    )
    db.session.add(new_client)
    db.session.commit()
    flash('Клиент добавлен!', 'success')
    ref = request.referrer or url_for('add_sale')
    if '/clients' in ref:
        return redirect(url_for('clients'))
    if 'add_sale' in ref or 'edit_sale' in ref:
        sep = '&' if '?' in ref else '?'
        ref = ref + sep + f'new_client_id={new_client.id}'
    return redirect(ref)


@app.route('/api/add_client', methods=['POST'])
@login_required
def api_add_client():
    """AJAX-добавление клиента без перезагрузки страницы"""
    data = request.get_json(silent=True) or request.form
    full_name = (data.get('full_name') or '').strip()
    phone = (data.get('phone') or '').strip() or None
    instagram = (data.get('instagram') or '').strip() or None
    telegram = (data.get('telegram') or '').strip() or None
    email = (data.get('email') or '').strip() or None
    birth_date_str = (data.get('birth_date') or '').strip()
    birth_date = None
    if birth_date_str:
        try:
            birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d').date()
        except ValueError:
            pass
    if not full_name:
        return jsonify({'success': False, 'error': 'ФИО обязательно для заполнения'})
    if not any([phone, instagram, telegram, email]):
        return jsonify({'success': False, 'error': 'Укажите хотя бы один контакт: телефон, Instagram, Telegram или почту'})
    new_client = Client(
        full_name=full_name,
        phone=phone,
        instagram=instagram,
        telegram=telegram,
        email=email,
        birth_date=birth_date
    )
    db.session.add(new_client)
    db.session.commit()
    return jsonify({'success': True, 'id': new_client.id, 'full_name': new_client.full_name,
                    'phone': new_client.phone or '', 'telegram': new_client.telegram or '',
                    'email': new_client.email or '', 'instagram': new_client.instagram or ''})


@app.route('/api/add_investor', methods=['POST'])
@login_required
def api_add_investor():
    """AJAX-добавление инвестора без перезагрузки страницы"""
    data = request.get_json(silent=True) or request.form
    name = (data.get('name') or '').strip()
    full_name = (data.get('full_name') or '').strip() or None
    phone = (data.get('phone') or '').strip() or None
    email = (data.get('email') or '').strip() or None
    is_commission_client = bool(data.get('is_commission_client', False))
    if not name:
        return jsonify({'success': False, 'error': 'Псевдоним обязателен для заполнения'})
    if Investor.query.filter_by(name=name).first():
        return jsonify({'success': False, 'error': 'Инвестор с таким именем уже существует'})
    new_investor = Investor(name=name, full_name=full_name, phone=phone, email=email,
                            is_commission_client=is_commission_client)
    db.session.add(new_investor)
    db.session.commit()
    return jsonify({'success': True, 'id': new_investor.id, 'name': new_investor.name,
                    'full_name': new_investor.full_name or '', 'phone': new_investor.phone or '',
                    'email': new_investor.email or '',
                    'is_commission_client': new_investor.is_commission_client})


@app.route('/api/clients/search')
@login_required
def api_clients_search():
    """Поиск клиентов для автоподстановки"""
    q = request.args.get('q', '').strip()
    limit = min(int(request.args.get('limit', 20)), 50)
    if len(q) < 1:
        clients = Client.query.order_by(Client.full_name).limit(limit).all()
    else:
        search = f'%{q}%'
        clients = Client.query.filter(
            or_(
                Client.full_name.ilike(search),
                Client.phone.ilike(search),
                Client.telegram.ilike(search),
                Client.email.ilike(search),
                Client.instagram.ilike(search)
            )
        ).order_by(Client.full_name).limit(limit).all()
    result = [{'id': c.id, 'full_name': c.full_name, 'phone': c.phone or '', 'telegram': c.telegram or '', 'email': c.email or '', 'instagram': c.instagram or ''} for c in clients]
    return jsonify(result)


@app.route('/api/investors/search')
@login_required
def api_investors_search():
    """Поиск инвесторов для автоподстановки"""
    q = request.args.get('q', '').strip()
    limit = min(int(request.args.get('limit', 20)), 50)
    commission_only = request.args.get('commission_only', '0') == '1'
    query = Investor.query
    if commission_only:
        query = query.filter(Investor.is_commission_client == True)
    if len(q) < 1:
        investors = query.order_by(Investor.name).limit(limit).all()
    else:
        search = f'%{q}%'
        investors = query.filter(
            or_(
                Investor.name.ilike(search),
                Investor.full_name.ilike(search),
                Investor.phone.ilike(search),
                Investor.email.ilike(search)
            )
        ).order_by(Investor.name).limit(limit).all()
    result = [{'id': inv.id, 'name': inv.name, 'full_name': inv.full_name or '', 'phone': inv.phone or '', 'email': inv.email or '',
               'is_commission_client': inv.is_commission_client} for inv in investors]
    return jsonify(result)


# УПРАВЛЕНИЕ ПРОДАВЦАМИ (только для админа)
@app.route('/manage_sellers', methods=['GET', 'POST'])
@admin_required
def manage_sellers():
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'add':
            name = request.form['name'].strip()
            username = request.form['username'].strip()
            password = request.form['password']
            
            if not name or not username or not password:
                flash('Все поля обязательны для заполнения', 'error')
                return redirect(url_for('manage_sellers'))
            
            if User.query.filter_by(username=username).first():
                flash('Пользователь с таким логином уже существует', 'error')
                return redirect(url_for('manage_sellers'))
            
            new_seller = User(name=name, username=username, is_admin=False)
            new_seller.set_password(password)
            db.session.add(new_seller)
            
            # Автоматически добавляем продавца в список сотрудников (Employee)
            # Проверяем, не существует ли уже сотрудник с таким именем
            existing_employee = Employee.query.filter_by(name=name).first()
            if not existing_employee:
                new_employee = Employee(name=name)
                db.session.add(new_employee)
            
            db.session.commit()
            flash(f'Продавец {name} успешно добавлен!', 'success')
            return redirect(url_for('manage_sellers'))
        
        elif action == 'add_top_manager':
            name = request.form['name'].strip()
            username = request.form['username'].strip()
            password = request.form['password']
            
            if not name or not username or not password:
                flash('Все поля обязательны для заполнения', 'error')
                return redirect(url_for('manage_sellers'))
            
            if User.query.filter_by(username=username).first():
                flash('Пользователь с таким логином уже существует', 'error')
                return redirect(url_for('manage_sellers'))
            
            new_top_manager = User(name=name, username=username, is_admin=False, is_top_manager=True)
            new_top_manager.set_password(password)
            db.session.add(new_top_manager)
            db.session.commit()
            flash(f'Топ-менеджер {name} успешно добавлен!', 'success')
            return redirect(url_for('manage_sellers'))
        
        elif action == 'delete_top_manager':
            top_manager_id = request.form.get('top_manager_id', type=int)
            top_manager = User.query.get(top_manager_id)
            if top_manager and top_manager.is_top_manager and not top_manager.is_admin:
                db.session.delete(top_manager)
                db.session.commit()
                flash('Топ-менеджер удален', 'success')
            else:
                flash('Нельзя удалить этого пользователя', 'error')
            return redirect(url_for('manage_sellers'))
        
        elif action == 'change_top_manager_password':
            top_manager_id = request.form.get('top_manager_id', type=int)
            new_password = request.form.get('new_password')
            top_manager = User.query.get(top_manager_id)
            if top_manager and top_manager.is_top_manager and new_password:
                top_manager.set_password(new_password)
                db.session.commit()
                flash('Пароль изменен', 'success')
            else:
                flash('Ошибка при изменении пароля', 'error')
            return redirect(url_for('manage_sellers'))
        
        elif action == 'add_service':
            name = request.form.get('name', '').strip()
            username = request.form.get('username', '').strip()
            password = request.form.get('password', '')
            if not name or not username or not password:
                flash('Все поля обязательны для заполнения', 'error')
                return redirect(url_for('manage_sellers'))
            if User.query.filter_by(username=username).first():
                flash('Пользователь с таким логином уже существует', 'error')
                return redirect(url_for('manage_sellers'))
            new_service = User(
                name=name,
                username=username,
                is_admin=False,
                is_investor=False,
                is_top_manager=False,
                is_service=True
            )
            new_service.set_password(password)
            db.session.add(new_service)
            db.session.commit()
            flash(f'Пользователь сервиса (ремонт) {name} успешно добавлен!', 'success')
            return redirect(url_for('manage_sellers'))
        
        elif action == 'delete_service':
            service_id = request.form.get('service_id', type=int)
            service_user = User.query.get(service_id)
            if service_user and getattr(service_user, 'is_service', False) and not service_user.is_admin:
                db.session.delete(service_user)
                db.session.commit()
                flash('Пользователь сервиса удален', 'success')
            else:
                flash('Нельзя удалить этого пользователя', 'error')
            return redirect(url_for('manage_sellers'))
        
        elif action == 'change_service_password':
            service_id = request.form.get('service_id', type=int)
            new_password = request.form.get('new_password')
            service_user = User.query.get(service_id)
            if service_user and getattr(service_user, 'is_service', False) and new_password:
                service_user.set_password(new_password)
                db.session.commit()
                flash('Пароль изменен', 'success')
            else:
                flash('Ошибка при изменении пароля', 'error')
            return redirect(url_for('manage_sellers'))
        
        elif action == 'delete':
            seller_id = request.form.get('seller_id', type=int)
            seller = User.query.get(seller_id)
            if seller and not seller.is_admin:
                db.session.delete(seller)
                db.session.commit()
                flash('Продавец удален', 'success')
            else:
                flash('Нельзя удалить этого пользователя', 'error')
            return redirect(url_for('manage_sellers'))
        
        elif action == 'change_password':
            seller_id = request.form.get('seller_id', type=int)
            new_password = request.form.get('new_password')
            seller = User.query.get(seller_id)
            if seller and not seller.is_admin and new_password:
                seller.set_password(new_password)
                db.session.commit()
                flash('Пароль изменен', 'success')
            else:
                flash('Ошибка при изменении пароля', 'error')
            return redirect(url_for('manage_sellers'))
        
        elif action == 'add_investor':
            name = request.form.get('name', '').strip()
            full_name = request.form.get('full_name', '').strip()
            phone = request.form.get('phone', '').strip()
            email = request.form.get('email', '').strip()

            if not name:
                flash('Имя (псевдоним) обязательно для заполнения', 'error')
                return redirect(url_for('manage_sellers'))

            # Проверяем, существует ли инвестор с таким именем
            if Investor.query.filter_by(name=name).first():
                flash('Инвестор с таким именем уже существует', 'error')
                return redirect(url_for('manage_sellers'))

            new_investor = Investor(
                name=name,
                full_name=full_name or None,
                phone=phone or None,
                email=email or None
            )
            db.session.add(new_investor)
            db.session.commit()
            flash(f'Инвестор {name} успешно добавлен!', 'success')
            return redirect(url_for('manage_sellers'))
        
        elif action == 'delete_investor':
            investor_user_id = request.form.get('investor_user_id', type=int)
            investor_user = User.query.get(investor_user_id)
            if investor_user and investor_user.is_investor and not investor_user.is_admin:
                # Удаляем пользователя
                db.session.delete(investor_user)
                # Инвестора из таблицы Investor не удаляем, так как могут быть связанные данные
                db.session.commit()
                flash('Инвестор удален', 'success')
            else:
                flash('Нельзя удалить этого пользователя', 'error')
            ref = request.referrer or url_for('manage_sellers')
            if '/stock_investors' in ref:
                return redirect(url_for('stock_investors'))
            return redirect(url_for('manage_sellers'))

        elif action == 'delete_investor_record':
            investor_id_del = request.form.get('investor_id_del', type=int)
            investor_rec = Investor.query.get(investor_id_del)
            if investor_rec:
                label = 'Комиссионный клиент удалён' if investor_rec.is_commission_client else 'Инвестор удалён'
                db.session.delete(investor_rec)
                db.session.commit()
                flash(label, 'success')
            else:
                flash('Запись не найдена', 'error')
            ref = request.referrer or url_for('manage_sellers')
            if '/stock_investors' in ref:
                return redirect(url_for('stock_investors'))
            if '/clients' in ref:
                return redirect(url_for('clients'))
            return redirect(url_for('manage_sellers'))
        
        elif action == 'change_investor_password':
            investor_user_id = request.form.get('investor_user_id', type=int)
            new_password = request.form.get('new_password')
            investor_user = User.query.get(investor_user_id)
            if investor_user and investor_user.is_investor and not investor_user.is_admin and new_password:
                investor_user.set_password(new_password)
                db.session.commit()
                flash('Пароль изменен', 'success')
            else:
                flash('Ошибка при изменении пароля', 'error')
            ref = request.referrer or url_for('manage_sellers')
            if '/stock_investors' in ref:
                return redirect(url_for('stock_investors'))
            return redirect(url_for('manage_sellers'))
        
        elif action == 'change_investor_username':
            investor_user_id = request.form.get('investor_user_id', type=int)
            new_username = request.form.get('new_username', '').strip()
            investor_user = User.query.get(investor_user_id)
            if investor_user and investor_user.is_investor and not investor_user.is_admin and new_username:
                # Проверяем, не занят ли логин другим пользователем
                existing_user = User.query.filter_by(username=new_username).first()
                if existing_user and existing_user.id != investor_user_id:
                    flash('Пользователь с таким логином уже существует', 'error')
                else:
                    investor_user.username = new_username
                    db.session.commit()
                    flash('Логин изменен', 'success')
            else:
                flash('Ошибка при изменении логина', 'error')
            ref = request.referrer or url_for('manage_sellers')
            if '/stock_investors' in ref:
                return redirect(url_for('stock_investors'))
            return redirect(url_for('manage_sellers'))
        
        elif action == 'change_investor_name':
            investor_user_id = request.form.get('investor_user_id', type=int)
            new_name = request.form.get('new_name', '').strip()
            investor_user = User.query.get(investor_user_id)
            if investor_user and investor_user.is_investor and not investor_user.is_admin and new_name:
                old_name = investor_user.name
                # Обновляем имя пользователя
                investor_user.name = new_name
                # Обновляем имя в таблице Investor, если есть запись
                investor_record = Investor.query.filter_by(name=old_name).first()
                if investor_record:
                    investor_record.name = new_name
                db.session.commit()
                flash('Имя изменено', 'success')
            else:
                flash('Ошибка при изменении имени', 'error')
            ref = request.referrer or url_for('manage_sellers')
            if '/stock_investors' in ref:
                return redirect(url_for('stock_investors'))
            return redirect(url_for('manage_sellers'))
        
        elif action == 'change_investor_username':
            investor_user_id = request.form.get('investor_user_id', type=int)
            new_username = request.form.get('new_username', '').strip()
            investor_user = User.query.get(investor_user_id)
            if investor_user and investor_user.is_investor and not investor_user.is_admin and new_username:
                # Проверяем, не занят ли логин другим пользователем
                existing_user = User.query.filter_by(username=new_username).first()
                if existing_user and existing_user.id != investor_user_id:
                    flash('Пользователь с таким логином уже существует', 'error')
                else:
                    investor_user.username = new_username
                    db.session.commit()
                    flash('Логин изменен', 'success')
            else:
                flash('Ошибка при изменении логина', 'error')
            ref = request.referrer or url_for('manage_sellers')
            if '/stock_investors' in ref:
                return redirect(url_for('stock_investors'))
            return redirect(url_for('manage_sellers'))
        
        elif action == 'change_investor_name':
            investor_user_id = request.form.get('investor_user_id', type=int)
            new_name = request.form.get('new_name', '').strip()
            investor_user = User.query.get(investor_user_id)
            if investor_user and investor_user.is_investor and not investor_user.is_admin and new_name:
                # Обновляем имя пользователя
                investor_user.name = new_name
                # Обновляем имя в таблице Investor, если есть запись
                investor_record = Investor.query.filter_by(name=investor_user.name).first()
                if investor_record:
                    investor_record.name = new_name
                db.session.commit()
                flash('Имя изменено', 'success')
            else:
                flash('Ошибка при изменении имени', 'error')
            ref = request.referrer or url_for('manage_sellers')
            if '/stock_investors' in ref:
                return redirect(url_for('stock_investors'))
            return redirect(url_for('manage_sellers'))
    
    sellers = User.query.filter(
        User.is_admin == False,
        User.is_investor == False,
        User.is_top_manager == False,
        User.is_service == False
    ).all()
    top_managers = User.query.filter_by(is_admin=False, is_investor=False, is_top_manager=True).all()
    service_users = User.query.filter_by(is_service=True).all()
    investors = Investor.query.order_by(Investor.name).all()
    is_top_manager = session.get('is_top_manager', False)
    return render_template('manage_sellers.html', sellers=sellers, top_managers=top_managers, service_users=service_users, investors=investors, is_top_manager=is_top_manager)


@app.route('/all_sales_summary')
@login_required
def all_sales_summary():
    # Получаем параметры фильтрации
    year_filter = request.args.get('year', 'all')
    month_filter = request.args.get('month', 'all')
    city_filter = request.args.get('city', 'all')

    # Базовый запрос (менеджер видит только свои продажи)
    sales_query = Sale.query
    current_employee = get_current_user_employee()
    if current_employee:
        sales_query = sales_query.filter(Sale.employee_id == current_employee.id)

    # Фильтр по году и месяцу
    if year_filter != 'all':
        try:
            year_int = int(year_filter)
            if month_filter != 'all':
                try:
                    month_int = int(month_filter)
                    start_date = date(year_int, month_int, 1)
                    end_date = start_date + relativedelta(months=1)
                except:
                    start_date = date(year_int, 1, 1)
                    end_date = date(year_int + 1, 1, 1)
            else:
                start_date = date(year_int, 1, 1)
                end_date = date(year_int + 1, 1, 1)
            sales_query = sales_query.filter(Sale.date >= start_date, Sale.date < end_date)
        except:
            pass

    # Фильтр по городу
    if city_filter != 'all':
        sales_query = sales_query.join(City).filter(City.name == city_filter)

    # Получаем все продажи
    all_sales = sales_query.all()

    # Группируем данные по городам и месяцам
    cities_data = {}

    for sale in all_sales:
        city_name = sale.city.name
        sale_date = sale.date.date() if isinstance(sale.date, datetime) else sale.date
        sale_month = sale_date.month
        sale_year = sale_date.year
        
        if city_name not in cities_data:
            cities_data[city_name] = {
                'count': 0,
                'total_buy': 0,
                'total_sell': 0,
                'total_expenses': 0,
                'gross_profit': 0,
                'net_profit': 0,
                'months': {}  # Группировка по месяцам
            }

        # Обновляем статистику города
        data = cities_data[city_name]
        data['count'] += 1
        data['total_buy'] += sale.buy_price
        data['total_sell'] += sale.sell_price

        # Расходы этой продажи
        sale_expenses = sum(e.amount for e in sale.expenses)
        data['total_expenses'] += sale_expenses
        
        # Группировка по месяцам
        month_key = f"{sale_year}-{sale_month:02d}"
        if month_key not in data['months']:
            data['months'][month_key] = {
                'count': 0,
                'gross_profit': 0,
                'net_profit': 0,
                'month': sale_month,
                'year': sale_year
            }
        
        month_data = data['months'][month_key]
        month_data['count'] += 1
        sale_bonus = sale.bonus if sale.bonus else 0.0
        month_data['gross_profit'] += (sale.sell_price - sale.buy_price)
        month_data['net_profit'] += (sale.sell_price - sale.buy_price - sale_expenses - sale_bonus)
    
    # Вычисляем итоговые прибыли и сортируем месяцы для каждого города
    for city_name, data in cities_data.items():
        total_bonuses = sum((s.bonus if s.bonus else 0.0) + (getattr(s, 'murad_bonus', 0) or 0.0) for s in all_sales if s.city.name == city_name)
        data['gross_profit'] = data['total_sell'] - data['total_buy']
        data['net_profit'] = data['gross_profit'] - data['total_expenses'] - total_bonuses
        data['months'] = dict(sorted(data['months'].items(), reverse=True))
    
    # Период для отображения
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    period_label = "Все продажи"
    if year_filter != 'all' and month_filter != 'all':
        try:
            month_name = months_ru[int(month_filter) - 1]
            period_label = f"{month_name} {year_filter}"
        except:
            period_label = f"{year_filter} год"
    elif year_filter != 'all':
        period_label = f"{year_filter} год"

    # Список годов для фильтра
    all_years = sorted(set(s.date.year for s in Sale.query.with_entities(Sale.date).all()), reverse=True)
    if not all_years:
        all_years = [date.today().year]
    all_cities = sorted([c.name for c in City.query.all()])

    return render_template('all_sales_summary.html',
                           cities_data=cities_data,
                           all_years=all_years,
                           all_cities=all_cities,
                           selected_year=year_filter,
                           selected_month=month_filter,
                           selected_city=city_filter,
                           period_label=period_label,
                           months_ru=months_ru,
                           is_admin=session.get('is_admin', False))


# СТРАНИЦА ЗАЯВОК (только для админа)
@app.route('/pending_requests')
@admin_required
def pending_requests():
    """Страница со всеми заявками на подтверждение"""
    pending_sales = PendingSale.query.filter_by(status='pending').order_by(PendingSale.created_at.desc()).all()
    # Разделяем заявки на сток на общий и комиссионный
    pending_stocks = PendingStock.query.filter_by(status='pending', komissionnyy=False).order_by(PendingStock.created_at.desc()).all()
    pending_commission_stocks = PendingStock.query.filter_by(status='pending', komissionnyy=True).order_by(PendingStock.created_at.desc()).all()
    
    investors = get_active_investors()
    employees = sorted(get_active_employees(), key=lambda e: (e.name or ''))
    is_admin = session.get('is_admin', False)
    is_top_manager = session.get('is_top_manager', False)
    return render_template('pending_requests.html', 
                         pending_sales=pending_sales,
                         pending_stocks=pending_stocks,
                         pending_commission_stocks=pending_commission_stocks,
                         investors=investors,
                         employees=employees,
                         is_admin=is_admin,
                         is_top_manager=is_top_manager)


@app.route('/api/pending_sale/<int:pending_sale_id>/add_extra_bonus', methods=['POST'])
@admin_required
def add_pending_sale_extra_bonus(pending_sale_id):
    """Добавить доп. бонус другому менеджеру в заявке на продажу"""
    if session.get('is_top_manager'):
        return jsonify({'ok': False, 'error': 'Нет прав'}), 403
    pending_sale = PendingSale.query.get_or_404(pending_sale_id)
    if pending_sale.status != 'pending':
        return jsonify({'ok': False, 'error': 'Заявка уже обработана'}), 400
    try:
        data = request.json if request.is_json else request.form
        employee_id = data.get('employee_id')
        amount = data.get('amount')
        if not employee_id:
            return jsonify({'ok': False, 'error': 'Укажите менеджера'}), 400
        employee_id = int(employee_id)
        amount = float(amount)
        if amount <= 0:
            return jsonify({'ok': False, 'error': 'Сумма должна быть больше 0'}), 400
        ab = PendingSaleAdditionalBonus(pending_sale_id=pending_sale_id, employee_id=employee_id, amount=amount)
        db.session.add(ab)
        db.session.commit()
        employee = Employee.query.get(employee_id)
        return jsonify({'ok': True, 'id': ab.id, 'employee_name': (employee.name if employee else '')})
    except (ValueError, TypeError) as e:
        return jsonify({'ok': False, 'error': 'Неверные данные'}), 400
    except Exception as e:
        db.session.rollback()
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/pending_sale/<int:pending_sale_id>/remove_extra_bonus/<int:bonus_id>', methods=['POST'])
@admin_required
def remove_pending_sale_extra_bonus(pending_sale_id, bonus_id):
    """Удалить доп. бонус из заявки"""
    if session.get('is_top_manager'):
        return jsonify({'ok': False, 'error': 'Нет прав'}), 403
    ab = PendingSaleAdditionalBonus.query.filter_by(id=bonus_id, pending_sale_id=pending_sale_id).first_or_404()
    try:
        db.session.delete(ab)
        db.session.commit()
        return jsonify({'ok': True})
    except Exception as e:
        db.session.rollback()
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/complete_sale_request/<int:pending_sale_id>', methods=['POST'])
@admin_required
def complete_sale_request(pending_sale_id):
    """Дополнить заявку на продажу: цена покупки и/или инвестор (если менеджер не указал)"""
    if session.get('is_top_manager'):
        flash('У топ-менеджера нет прав для взаимодействия с заявками на продажу', 'error')
        return redirect(url_for('pending_requests'))
    pending_sale = PendingSale.query.get_or_404(pending_sale_id)
    
    if pending_sale.status != 'pending':
        flash('Эта заявка уже обработана', 'error')
        return redirect(url_for('pending_requests'))
    
    try:
        buy_price = request.form.get('buy_price')
        investor_id = request.form.get('investor_id')
        
        updated = False
        
        if buy_price:
            try:
                val = float(buy_price)
                if val <= 0:
                    flash('Укажите цену покупки больше 0.', 'error')
                    return redirect(url_for('pending_requests'))
                pending_sale.buy_price = val
                updated = True
            except (ValueError, TypeError):
                flash('Проверьте цену покупки: должно быть число.', 'error')
                return redirect(url_for('pending_requests'))
        
        if investor_id is not None and investor_id != '':
            try:
                pending_sale.investor_id = int(investor_id)
                updated = True
            except (ValueError, TypeError):
                flash('Проверьте выбранного инвестора.', 'error')
                return redirect(url_for('pending_requests'))
        else:
            # Явно «без инвестора»
            pending_sale.investor_id = None
            updated = True
        
        if pending_sale.buy_price is None or pending_sale.buy_price == 0:
            flash('Укажите цену покупки перед сохранением (инвестор необязателен).', 'error')
            return redirect(url_for('pending_requests'))
        
        db.session.commit()
        try:
            from telegram_bot import update_telegram_message_for_sale
            update_telegram_message_for_sale(pending_sale_id, 'completed')
        except Exception as e:
            print(f"Ошибка при обновлении Telegram: {e}")
        flash('Заявка дополнена! Теперь можно принять или отклонить.', 'success')
    except Exception as e:
        db.session.rollback()
        flash('Ошибка при сохранении', 'error')
    
    return redirect(url_for('pending_requests'))


@app.route('/approve_sale_request/<int:pending_sale_id>', methods=['POST'])
@admin_required
def approve_sale_request(pending_sale_id):
    """Одобрить заявку на продажу с сайта"""
    if session.get('is_top_manager'):
        flash('У топ-менеджера нет прав для взаимодействия с заявками на продажу', 'error')
        return redirect(url_for('pending_requests'))
    pending_sale = PendingSale.query.get_or_404(pending_sale_id)
    
    if pending_sale.status != 'pending':
        flash('Эта заявка уже обработана', 'error')
        return redirect(url_for('pending_requests'))
    
    if pending_sale.buy_price is None or pending_sale.buy_price == 0:
        flash('Дополните цену покупки на странице заявок (или в боте) перед одобрением.', 'error')
        return redirect(url_for('pending_requests'))
    
    try:
        # Получаем итоговый бонус из формы (уже с учетом текущего)
        bonus = float(request.form.get('bonus', 0) or 0)
        
        # Создаем реальную продажу из временной заявки
        initial = getattr(pending_sale, 'initial_bonus', None) or 0.0
        murad_bonus = getattr(pending_sale, 'murad_bonus', None)
        if murad_bonus is None:
            murad_bonus = get_murad_bonus_for_city(
                pending_sale.city_id,
                getattr(pending_sale, 'category', None)
            )
        new_sale = Sale(
            photo=pending_sale.photo,
            product_name=pending_sale.product_name,
            reference=pending_sale.reference,
            item_year=pending_sale.item_year,
            buy_price=pending_sale.buy_price,
            bonus=bonus,
            initial_bonus=initial,
            murad_bonus=murad_bonus or 0.0,
            sell_price=pending_sale.sell_price,
            komplektatsiya=getattr(pending_sale, 'komplektatsiya', None),
            komissionnyy=getattr(pending_sale, 'komissionnyy', False),
            category=getattr(pending_sale, 'category', None),
            city_id=pending_sale.city_id,
            employee_id=pending_sale.employee_id,
            investor_id=pending_sale.investor_id,
            client_id=pending_sale.client_id,
            date=pending_sale.date
        )
        db.session.add(new_sale)
        db.session.flush()  # Получаем ID новой продажи
        
        # Переносим расходы
        pending_expenses = PendingSaleExpense.query.filter_by(pending_sale_id=pending_sale.id).all()
        for pending_exp in pending_expenses:
            expense = Expense(
                sale_id=new_sale.id,
                expense_type_id=pending_exp.expense_type_id,
                amount=pending_exp.amount,
                comment=pending_exp.comment
            )
            db.session.add(expense)
        
        # Копируем доп. бонусы другим менеджерам
        extra_bonus_recipients = []
        for ab in getattr(pending_sale, 'additional_bonuses', []) or []:
            db.session.add(SaleAdditionalBonus(sale_id=new_sale.id, employee_id=ab.employee_id, amount=ab.amount))
            extra_bonus_recipients.append((ab.employee_id, ab.amount))
        
        # Если продажа из стока, помечаем сток как проданный
        if pending_sale.stock_id:
            stock_item = StockItem.query.get(pending_sale.stock_id)
            if stock_item:
                stock_item.sold = True
        
        # Обновляем статус заявки перед обновлением сообщения в боте
        pending_sale.status = 'approved'
        db.session.commit()
        
        # Обновляем сообщения в Telegram для всех администраторов (ПЕРЕД удалением заявки)
        try:
            from telegram_bot import update_telegram_message_for_sale, send_bonus_notification_to_manager_async, send_extra_bonus_notification_async
            update_telegram_message_for_sale(pending_sale.id, 'approved')
            # Отправляем уведомление основному менеджеру о бонусе (если есть)
            if new_sale.bonus and new_sale.bonus > 0:
                send_bonus_notification_to_manager_async(new_sale.id)
            # Уведомления доп. менеджерам о начисленном доп. бонусе
            for emp_id, amount in extra_bonus_recipients:
                if amount and amount > 0:
                    send_extra_bonus_notification_async(new_sale.id, emp_id, amount)
        except Exception as e:
            print(f"Ошибка при обновлении сообщения в Telegram: {e}")
            import traceback
            traceback.print_exc()
        
        # Удаляем временную заявку после одобрения
        PendingSaleExpense.query.filter_by(pending_sale_id=pending_sale.id).delete()
        db.session.delete(pending_sale)
        db.session.commit()
        
        flash('Заявка на продажу одобрена и добавлена в базу!', 'success')
    except Exception as e:
        db.session.rollback()
        print(f"Ошибка при одобрении заявки: {e}")
        import traceback
        traceback.print_exc()
        flash('Ошибка при обработке заявки', 'error')
    
    return redirect(url_for('pending_requests'))


@app.route('/reject_sale_request/<int:pending_sale_id>', methods=['POST'])
@admin_required
def reject_sale_request(pending_sale_id):
    """Отклонить заявку на продажу с сайта"""
    if session.get('is_top_manager'):
        flash('У топ-менеджера нет прав для взаимодействия с заявками на продажу', 'error')
        return redirect(url_for('pending_requests'))
    pending_sale = PendingSale.query.get_or_404(pending_sale_id)
    
    if pending_sale.status != 'pending':
        flash('Эта заявка уже обработана', 'error')
        return redirect(url_for('pending_requests'))
    
    try:
        # Удаляем фото, если оно было загружено специально для этой заявки
        if pending_sale.photo and not pending_sale.stock_id:
            photo_path = os.path.join(app.config['UPLOAD_FOLDER'], pending_sale.photo)
            if os.path.exists(photo_path):
                try:
                    os.remove(photo_path)
                except:
                    pass
        
        # Обновляем статус заявки
        pending_sale.status = 'rejected'
        db.session.commit()
        
        # Обновляем сообщения в Telegram для всех администраторов
        try:
            from telegram_bot import update_telegram_message_for_sale
            update_telegram_message_for_sale(pending_sale.id, 'rejected')
        except Exception as e:
            print(f"Ошибка при обновлении сообщения в Telegram: {e}")
            import traceback
            traceback.print_exc()
        
        # Удаляем заявку и связанные расходы
        PendingSaleExpense.query.filter_by(pending_sale_id=pending_sale.id).delete()
        db.session.delete(pending_sale)
        db.session.commit()
        
        flash('Заявка на продажу отклонена и удалена!', 'success')
    except Exception as e:
        db.session.rollback()
        print(f"Ошибка при отклонении заявки: {e}")
        import traceback
        traceback.print_exc()
        flash('Ошибка при обработке заявки', 'error')
    
    return redirect(url_for('pending_requests'))


@app.route('/complete_stock_request/<int:pending_stock_id>', methods=['POST'])
@admin_required
def complete_stock_request(pending_stock_id):
    """Дополнить заявку на сток: для общего стока - цена покупки и инвестор, для комиссионного - имя, телефон и выплата"""
    pending_stock = PendingStock.query.get_or_404(pending_stock_id)
    
    if pending_stock.status != 'pending':
        flash('Эта заявка уже обработана', 'error')
        return redirect(url_for('pending_requests'))
    
    try:
        if pending_stock.komissionnyy:
            # Для комиссионного стока: выбор инвестора (владельца) и выплата
            investor_id = request.form.get('investor_id')
            buy_price = request.form.get('buy_price')  # Здесь это выплата
            
            if investor_id:
                try:
                    inv = Investor.query.get(int(investor_id))
                    if inv:
                        pending_stock.investor_id = inv.id
                        pending_stock.client_full_name = inv.full_name or inv.name
                        pending_stock.client_phone = inv.phone
                except (ValueError, TypeError):
                    pass
            if buy_price:
                pending_stock.buy_price = float(buy_price)
        else:
            # Для общего стока: цена покупки и инвестор
            buy_price = request.form.get('buy_price')
            investor_id = request.form.get('investor_id')
            
            if buy_price:
                pending_stock.buy_price = float(buy_price)
            if investor_id:
                pending_stock.investor_id = int(investor_id)
        
        db.session.commit()
        try:
            from telegram_bot import update_telegram_message_for_stock
            update_telegram_message_for_stock(pending_stock_id, 'completed')
        except Exception as e:
            print(f"Ошибка при обновлении Telegram: {e}")
        flash('Заявка дополнена! Теперь можно принять или отклонить.', 'success')
    except (ValueError, TypeError) as e:
        flash('Проверьте введённые данные: цена должна быть числом.', 'error')
    
    return redirect(url_for('pending_requests'))


@app.route('/approve_stock_request/<int:pending_stock_id>', methods=['POST'])
@admin_required
def approve_stock_request(pending_stock_id):
    """Одобрить заявку на сток с сайта"""
    pending_stock = PendingStock.query.get_or_404(pending_stock_id)
    
    if pending_stock.status != 'pending':
        flash('Эта заявка уже обработана', 'error')
        return redirect(url_for('pending_requests'))
    
    # Проверяем в зависимости от типа стока
    if pending_stock.komissionnyy:
        # Для комиссионного стока: проверяем выплату (buy_price используется как выплата)
        if pending_stock.buy_price is None:
            flash('Дополните выплату владельцу на странице заявок перед одобрением.', 'error')
            return redirect(url_for('pending_requests'))
    else:
        # Для общего стока: проверяем только цену покупки (инвестор необязателен)
        if pending_stock.buy_price is None:
            flash('Дополните цену покупки на странице заявок перед одобрением.', 'error')
            return redirect(url_for('pending_requests'))
    
    try:
        # Создаем реальный сток из временной заявки
        if pending_stock.komissionnyy:
            # Для комиссионного стока используем данные клиента из заявки
            new_stock = StockItem(
                photo=pending_stock.photo,
                product_name=pending_stock.product_name,
                reference=pending_stock.reference,
                item_year=pending_stock.item_year,
                buy_price=pending_stock.buy_price,  # Выплата
                expected_sell_price=pending_stock.expected_sell_price,
                quantity=pending_stock.quantity,
                komplektatsiya=getattr(pending_stock, 'komplektatsiya', None),
                komissionnyy=True,
                city_id=pending_stock.city_id,
                investor_id=pending_stock.investor_id,  # Опционально: если менеджер выбрал инвестора
                client_full_name=pending_stock.client_full_name,
                client_phone=pending_stock.client_phone,
                client_instagram=None,
                client_telegram=None,
                client_email=None
            )
        else:
            # Для общего стока
            new_stock = StockItem(
                photo=pending_stock.photo,
                product_name=pending_stock.product_name,
                reference=pending_stock.reference,
                item_year=pending_stock.item_year,
                buy_price=pending_stock.buy_price,
                expected_sell_price=pending_stock.expected_sell_price,
                quantity=pending_stock.quantity,
                komplektatsiya=getattr(pending_stock, 'komplektatsiya', None),
                komissionnyy=False,
                city_id=pending_stock.city_id,
                investor_id=pending_stock.investor_id
            )
        db.session.add(new_stock)
        db.session.flush()  # Получаем ID нового стока
        
        # Переносим расходы
        pending_expenses = PendingStockExpense.query.filter_by(pending_stock_id=pending_stock.id).all()
        for pending_exp in pending_expenses:
            stock_expense = StockExpense(
                stock_item_id=new_stock.id,
                expense_type_id=pending_exp.expense_type_id,
                amount=pending_exp.amount,
                comment=pending_exp.comment
            )
            db.session.add(stock_expense)
        
        # Обновляем статус заявки
        pending_stock.status = 'approved'
        db.session.commit()
        
        # Обновляем сообщения в Telegram для всех администраторов
        try:
            from telegram_bot import update_telegram_message_for_stock
            update_telegram_message_for_stock(pending_stock.id, 'approved')
        except Exception as e:
            print(f"Ошибка при обновлении сообщения в Telegram: {e}")
            import traceback
            traceback.print_exc()
        
        # Удаляем временную заявку после одобрения
        PendingStockExpense.query.filter_by(pending_stock_id=pending_stock.id).delete()
        db.session.delete(pending_stock)
        db.session.commit()
        
        flash('Заявка на сток одобрена и добавлена в базу!', 'success')
    except Exception as e:
        db.session.rollback()
        print(f"Ошибка при одобрении заявки: {e}")
        import traceback
        traceback.print_exc()
        flash('Ошибка при обработке заявки', 'error')
    
    return redirect(url_for('pending_requests'))


@app.route('/reject_stock_request/<int:pending_stock_id>', methods=['POST'])
@admin_required
def reject_stock_request(pending_stock_id):
    """Отклонить заявку на сток с сайта"""
    pending_stock = PendingStock.query.get_or_404(pending_stock_id)
    
    if pending_stock.status != 'pending':
        flash('Эта заявка уже обработана', 'error')
        return redirect(url_for('pending_requests'))
    
    try:
        # Удаляем фото, если оно было загружено
        if pending_stock.photo:
            photo_path = os.path.join(app.config['UPLOAD_FOLDER'], pending_stock.photo)
            if os.path.exists(photo_path):
                try:
                    os.remove(photo_path)
                except:
                    pass
        
        # Обновляем статус заявки
        pending_stock.status = 'rejected'
        db.session.commit()
        
        # Обновляем сообщения в Telegram для всех администраторов
        try:
            from telegram_bot import update_telegram_message_for_stock
            update_telegram_message_for_stock(pending_stock.id, 'rejected')
        except Exception as e:
            print(f"Ошибка при обновлении сообщения в Telegram: {e}")
            import traceback
            traceback.print_exc()
        
        # Удаляем заявку и связанные расходы
        PendingStockExpense.query.filter_by(pending_stock_id=pending_stock.id).delete()
        db.session.delete(pending_stock)
        db.session.commit()
        
        flash('Заявка на сток отклонена и удалена!', 'success')
    except Exception as e:
        db.session.rollback()
        print(f"Ошибка при отклонении заявки: {e}")
        import traceback
        traceback.print_exc()
        flash('Ошибка при обработке заявки', 'error')
    
    return redirect(url_for('pending_requests'))


@app.route('/calculator')
@login_required
def calculator():
    """Страница калькулятора стоимости"""
    return render_template('calculator.html')


# РЕМОНТ
@app.route('/repair')
@login_required
def repair_main():
    """Главная страница ремонта - выбор типа (Часы/Ювелирные изделия)"""
    all_repairs = Repair.query.all()
    repair_count = len(all_repairs)
    repair_income = sum(r.sell_price for r in all_repairs)
    repair_profit = sum(r.sell_price - r.buy_price for r in all_repairs)
    return render_template('repair_main.html',
                           repair_count=repair_count,
                           repair_income=repair_income,
                           repair_profit=repair_profit,
                           is_admin=session.get('is_admin', False))


@app.route('/repair/<category>')
@login_required
def repair_list(category):
    """Список ремонтов по категории (watches или jewelry)"""
    if category not in ['watches', 'jewelry']:
        flash('Неверная категория ремонта', 'error')
        return redirect(url_for('repair_main'))
    
    # Параметры фильтрации
    city_filter = request.args.get('city', 'all')
    year_filter = request.args.get('year', 'all')
    month_filter = request.args.get('month', 'all')
    client_filter = request.args.get('client', 'all')
    repair_type_filter = request.args.get('repair_type', 'all')
    
    # Базовый запрос
    repairs_query = Repair.query.filter_by(repair_category=category)
    
    # Фильтр по городу
    if city_filter != 'all':
        repairs_query = repairs_query.join(City).filter(City.name == city_filter)
    
    # Фильтр по клиенту
    if client_filter != 'all':
        try:
            client_id = int(client_filter)
            repairs_query = repairs_query.filter(Repair.client_id == client_id)
        except:
            pass
    
    # Фильтр по типу ремонта
    if repair_type_filter != 'all':
        repairs_query = repairs_query.filter(Repair.repair_type == repair_type_filter)
    
    # Фильтр по году и месяцу
    if year_filter != 'all':
        try:
            year_int = int(year_filter)
            if month_filter != 'all':
                try:
                    month_int = int(month_filter)
                    start_date = date(year_int, month_int, 1)
                    end_date = start_date + relativedelta(months=1)
                    repairs_query = repairs_query.filter(Repair.date >= start_date, Repair.date < end_date)
                except:
                    start_date = date(year_int, 1, 1)
                    end_date = date(year_int + 1, 1, 1)
                    repairs_query = repairs_query.filter(Repair.date >= start_date, Repair.date < end_date)
            else:
                start_date = date(year_int, 1, 1)
                end_date = date(year_int + 1, 1, 1)
                repairs_query = repairs_query.filter(Repair.date >= start_date, Repair.date < end_date)
        except:
            pass
    
    repairs = repairs_query.order_by(Repair.date.desc()).all()

    # Статистика по отфильтрованным ремонтам
    stat_count = len(repairs)
    stat_income = sum(r.sell_price for r in repairs)
    stat_profit = sum(r.sell_price - r.buy_price for r in repairs)
    
    # Данные для фильтров
    all_cities = sorted([c.name for c in City.query.all()])
    all_clients = Client.query.order_by(Client.full_name).all()
    # Получаем годы из всех ремонтов
    all_repairs = Repair.query.with_entities(Repair.date).all()
    all_years = sorted(set(r.date.year for r in all_repairs if r.date), reverse=True)
    if not all_years:
        all_years = [date.today().year]
    
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    
    category_name = 'Часы' if category == 'watches' else 'Ювелирные изделия'
    
    return render_template('repair_list.html',
                         repairs=repairs,
                         category=category,
                         category_name=category_name,
                         city_filter=city_filter,
                         client_filter=client_filter,
                         year_filter=year_filter,
                         month_filter=month_filter,
                         repair_type_filter=repair_type_filter,
                         all_cities=all_cities,
                         all_clients=all_clients,
                         all_years=all_years,
                         months_ru=months_ru,
                         stat_count=stat_count,
                         stat_income=stat_income,
                         stat_profit=stat_profit,
                         is_admin=session.get('is_admin', False))


@app.route('/repair/<category>/add', methods=['GET', 'POST'])
@login_required
def add_repair(category):
    """Добавление ремонта"""
    if category not in ['watches', 'jewelry']:
        flash('Неверная категория ремонта', 'error')
        return redirect(url_for('repair_main'))
    
    cities = City.query.all()
    
    if request.method == 'POST':
        product_name = request.form['product_name']
        buy_price = float(request.form['buy_price'])
        sell_price = float(request.form['sell_price'])
        city_id = int(request.form['city_id'])
        client_id = request.form.get('client_id')
        repair_date_str = request.form.get('date')
        repair_type = request.form['repair_type'].strip()  # Обязательное поле
        repair_type_other = request.form.get('repair_type_other', '').strip() or None  # Необязательное поле-комментарий
        comment = request.form.get('comment', '').strip() or None
        
        # Обработка даты
        if repair_date_str:
            repair_date = datetime.strptime(repair_date_str, '%Y-%m-%d')
        else:
            repair_date = datetime.utcnow()
        
        # Получаем данные клиента для автоматической подстановки телефона и тг
        phone = None
        telegram = None
        if client_id:
            client = Client.query.get(int(client_id))
            if client:
                phone = client.phone
                telegram = client.telegram
        
        # Обработка фото
        photo_filename = None
        if 'photo' in request.files:
            file = request.files['photo']
            if file and file.filename and allowed_file(file.filename):
                photo_filename = generate_unique_filename(file.filename)
                full_path = os.path.join(app.config['UPLOAD_FOLDER'], photo_filename)
                file.save(full_path)
                # Оптимизируем изображение после сохранения
                optimize_image(full_path)
        
        repair = Repair(
            photo=photo_filename,
            product_name=product_name,
            buy_price=buy_price,
            sell_price=sell_price,
            city_id=city_id,
            client_id=int(client_id) if client_id else None,
            phone=phone,
            telegram=telegram,
            date=repair_date,
            repair_type=repair_type,
            repair_type_other=repair_type_other,
            repair_category=category,
            comment=comment
        )
        
        db.session.add(repair)
        db.session.commit()
        
        flash('Ремонт успешно добавлен!', 'success')
        return redirect(url_for('repair_list', category=category))
    
    category_name = 'Часы' if category == 'watches' else 'Ювелирные изделия'
    return render_template('add_repair.html',
                         category=category,
                         category_name=category_name,
                         cities=cities)


@app.route('/repair/<category>/edit/<int:repair_id>', methods=['GET', 'POST'])
@login_required
def edit_repair(category, repair_id):
    """Редактирование ремонта"""
    if category not in ['watches', 'jewelry']:
        flash('Неверная категория ремонта', 'error')
        return redirect(url_for('repair_main'))
    
    repair = Repair.query.get_or_404(repair_id)
    if repair.repair_category != category:
        flash('Неверная категория ремонта', 'error')
        return redirect(url_for('repair_main'))
    
    cities = City.query.all()
    clients_list = Client.query.order_by(Client.full_name).all()
    
    if request.method == 'POST':
        repair.product_name = request.form['product_name']
        repair.buy_price = float(request.form['buy_price'])
        repair.sell_price = float(request.form['sell_price'])
        repair.city_id = int(request.form['city_id'])
        client_id = request.form.get('client_id')
        repair.client_id = int(client_id) if client_id else None
        
        # Обработка даты
        repair_date_str = request.form.get('date')
        if repair_date_str:
            repair.date = datetime.strptime(repair_date_str, '%Y-%m-%d')
        
        # Телефон и тг автоматически подтягиваются из клиента
        if repair.client_id:
            client = Client.query.get(repair.client_id)
            if client:
                repair.phone = client.phone
                repair.telegram = client.telegram
        else:
            repair.phone = None
            repair.telegram = None
        
        repair.repair_type = request.form['repair_type'].strip()  # Обязательное поле
        repair.repair_type_other = request.form.get('repair_type_other', '').strip() or None  # Необязательное поле-комментарий
        repair.comment = request.form.get('comment', '').strip() or None
        
        # Обработка фото
        if 'photo' in request.files:
            file = request.files['photo']
            if file and file.filename and allowed_file(file.filename):
                # Удаляем старое фото если есть
                if repair.photo:
                    old_path = os.path.join(app.config['UPLOAD_FOLDER'], repair.photo)
                    if os.path.exists(old_path):
                        os.remove(old_path)
                
                repair.photo = generate_unique_filename(file.filename)
                full_path = os.path.join(app.config['UPLOAD_FOLDER'], repair.photo)
                file.save(full_path)
                # Оптимизируем изображение после сохранения
                optimize_image(full_path)
        
        db.session.commit()
        flash('Ремонт успешно обновлен!', 'success')
        return redirect(url_for('repair_list', category=category))
    
    category_name = 'Часы' if category == 'watches' else 'Ювелирные изделия'
    return render_template('edit_repair.html',
                         repair=repair,
                         category=category,
                         category_name=category_name,
                         cities=cities,
                         clients_list=clients_list)


@app.route('/repair/<category>/delete/<int:repair_id>', methods=['POST'])
@login_required
def delete_repair(category, repair_id):
    """Удаление ремонта"""
    if category not in ['watches', 'jewelry']:
        flash('Неверная категория ремонта', 'error')
        return redirect(url_for('repair_main'))
    
    repair = Repair.query.get_or_404(repair_id)
    if repair.repair_category != category:
        flash('Неверная категория ремонта', 'error')
        return redirect(url_for('repair_main'))
    
    # Удаляем фото если есть
    if repair.photo:
        photo_path = os.path.join(app.config['UPLOAD_FOLDER'], repair.photo)
        if os.path.exists(photo_path):
            os.remove(photo_path)
    
    db.session.delete(repair)
    db.session.commit()
    
    flash('Ремонт успешно удален!', 'success')
    return redirect(url_for('repair_list', category=category))


# ПРЕДОПЛАТА (заглушка)
@app.route('/prepayment')
@login_required
def prepayment():
    """Главная страница предоплаты - список заявок"""
    # Параметры из запроса
    city_filter = request.args.get('city', 'all')
    year_filter = request.args.get('year', 'all')
    month_filter = request.args.get('month', 'all')
    employee_filter = request.args.get('employee', 'all')
    client_filter = request.args.get('client', 'all')
    status_filter = request.args.get('status', 'all')
    
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    
    # Базовый запрос (менеджер видит только свои заявки)
    prepayments_query = PendingPrepayment.query
    if current_employee and not is_admin:
        prepayments_query = prepayments_query.filter(PendingPrepayment.employee_id == current_employee.id)
    
    # Фильтр по городу
    if city_filter != 'all':
        prepayments_query = prepayments_query.join(City).filter(City.name == city_filter)
    
    # Фильтр по сотруднику
    if employee_filter != 'all':
        try:
            employee_id = int(employee_filter)
            prepayments_query = prepayments_query.filter(PendingPrepayment.employee_id == employee_id)
        except:
            pass
    
    # Фильтр по клиенту
    if client_filter != 'all':
        try:
            client_id = int(client_filter)
            prepayments_query = prepayments_query.filter(PendingPrepayment.client_id == client_id)
        except:
            pass
    
    # Фильтр по статусу
    if status_filter != 'all':
        prepayments_query = prepayments_query.filter(PendingPrepayment.status == status_filter)
    
    # Фильтр по году и месяцу
    if month_filter != 'all':
        try:
            month_int = int(month_filter)
            if year_filter != 'all':
                try:
                    year_int = int(year_filter)
                    start_date = date(year_int, month_int, 1)
                    end_date = start_date + relativedelta(months=1)
                    prepayments_query = prepayments_query.filter(PendingPrepayment.date >= start_date, PendingPrepayment.date < end_date)
                except:
                    pass
            else:
                # Фильтр только по месяцу (для всех годов)
                # Используем extract для фильтрации по месяцу
                prepayments_query = prepayments_query.filter(extract('month', PendingPrepayment.date) == month_int)
        except:
            pass
    elif year_filter != 'all':
        # Фильтр только по году (без месяца)
        try:
            year_int = int(year_filter)
            start_date = date(year_int, 1, 1)
            end_date = date(year_int + 1, 1, 1)
            prepayments_query = prepayments_query.filter(PendingPrepayment.date >= start_date, PendingPrepayment.date < end_date)
        except:
            pass
    
    # Сортируем по дате создания (новые сверху)
    prepayments = prepayments_query.order_by(PendingPrepayment.created_at.desc()).all()
    
    # Список всех уникальных городов для фильтра
    all_cities = sorted([c.name for c in City.query.all()])
    
    # Список всех сотрудников для фильтра
    all_employees = sorted(get_active_employees(), key=lambda e: e.name)
    
    # Список всех клиентов для фильтра
    all_clients = Client.query.order_by(Client.full_name).all()
    
    # Список всех годов для фильтра
    all_years = sorted(set(p.date.year for p in PendingPrepayment.query.with_entities(PendingPrepayment.date).all()), reverse=True)
    if not all_years:
        all_years = [date.today().year]
    
    # Для менеджера фиксируем фильтр по себе в отображении
    if current_employee:
        employee_filter = str(current_employee.id)
    
    months_ru = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    
    return render_template('prepayment_list.html', 
                         prepayments=prepayments,
                         city_filter=city_filter if city_filter != 'all' else 'all',
                         employee_filter=employee_filter if employee_filter != 'all' else 'all',
                         client_filter=client_filter if client_filter != 'all' else 'all',
                         year_filter=year_filter if year_filter != 'all' else 'all',
                         month_filter=month_filter if month_filter != 'all' else 'all',
                         status_filter=status_filter if status_filter != 'all' else 'all',
                         all_cities=all_cities,
                         all_employees=all_employees,
                         all_clients=all_clients,
                           all_years=all_years,
                           months_ru=months_ru,
                           product_categories=PRODUCT_CATEGORIES,
                         is_admin=is_admin)


@app.route('/prepayment/add', methods=['GET', 'POST'])
@login_required
def add_prepayment():
    """Добавление новой заявки на предоплату"""
    cities = City.query.all()
    expense_types = ExpenseType.query.all()
    employees = get_active_employees()
    investors = get_active_investors()
    clients_list = Client.query.order_by(Client.full_name).all()
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    
    # Менеджер может создать предоплату только от своего имени
    if not is_admin and not current_employee:
        flash('Обратитесь к администратору для настройки вашего профиля продавца.', 'error')
        return redirect(url_for('prepayment'))
    
    if request.method == 'POST':
        product_name = request.form['product_name']
        reference = request.form.get('reference', '')
        item_year = int(request.form['item_year']) if request.form.get('item_year') else None
        buy_price = float(request.form.get('buy_price', 0) or 0) if request.form.get('buy_price') else None
        prepayment_amount = float(request.form['prepayment_amount'])  # Заход (предоплата)
        sell_price = float(request.form['sell_price'])
        bonus = float(request.form.get('bonus', 0) or 0)
        komplektatsiya = request.form.get('komplektatsiya', '').strip() or None
        komissionnyy = request.form.get('komissionnyy') == '1'  # Комиссионный товар
        city_id = int(request.form['city_id'])
        employee_id = int(request.form['employee_id']) if is_admin else current_employee.id
        investor_id = int(request.form.get('investor_id')) if request.form.get('investor_id') else None
        client_id = int(request.form.get('client_id')) if request.form.get('client_id') else None
        date_str = request.form['date']
        date = datetime.strptime(date_str, '%Y-%m-%d') if date_str else datetime.utcnow()
        comment = request.form.get('comment', '').strip() or None
        
        photo_path = None
        photo = request.files['photo']
        if photo and allowed_file(photo.filename):
            filename = generate_unique_filename(photo.filename)
            photo_path = filename
            full_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            photo.save(full_path)
            # Оптимизируем изображение после сохранения
            optimize_image(full_path)
        
        # Создаем заявку на предоплату
        comment = request.form.get('comment', '').strip() or None
        new_prepayment = PendingPrepayment(
            photo=photo_path,
            product_name=product_name,
            reference=reference,
            item_year=item_year,
            buy_price=buy_price,
            prepayment_amount=prepayment_amount,
            sell_price=sell_price,
            bonus=bonus,
            initial_bonus=bonus,
            komplektatsiya=komplektatsiya,
            komissionnyy=komissionnyy,
            city_id=city_id,
            employee_id=employee_id,
            investor_id=investor_id,
            client_id=client_id,
            date=date,
            status='pending',
            comment=comment
        )
        db.session.add(new_prepayment)
        db.session.flush()
        
        # Добавление расходов из формы
        expense_type_ids = request.form.getlist('expense_type_id')
        amounts = request.form.getlist('expense_amount')
        comments = request.form.getlist('expense_comment')
        for et_id, amt, comment in zip(expense_type_ids, amounts, comments):
            if et_id and amt:
                expense = PendingPrepaymentExpense(
                    pending_prepayment_id=new_prepayment.id,
                    expense_type_id=int(et_id),
                    amount=float(amt),
                    comment=comment if comment else None
                )
                db.session.add(expense)
        
        db.session.commit()
        flash('Заявка на предоплату успешно добавлена!', 'success')
        return redirect(url_for('prepayment'))
    
    new_client_id = request.args.get('new_client_id', type=int)
    new_client = Client.query.get(new_client_id) if new_client_id else None
    
    return render_template('add_prepayment.html', 
                         cities=cities, 
                         expense_types=expense_types, 
                         employees=employees, 
                         investors=investors, 
                         clients=clients_list,
                         new_client=new_client,
                         is_admin=is_admin,
                         current_user_employee=current_employee)


@app.route('/prepayment/edit/<int:prepayment_id>', methods=['GET', 'POST'])
@login_required
def edit_prepayment(prepayment_id):
    """Редактирование заявки на предоплату"""
    prepayment = PendingPrepayment.query.get_or_404(prepayment_id)
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    
    # Менеджер может редактировать только свои заявки
    if not is_admin and current_employee and prepayment.employee_id != current_employee.id:
        flash('У вас нет прав для редактирования этой заявки.', 'error')
        return redirect(url_for('prepayment'))
    
    cities = City.query.all()
    expense_types = ExpenseType.query.all()
    employees = get_active_employees()
    investors = get_active_investors()
    clients_list = Client.query.order_by(Client.full_name).all()
    
    if request.method == 'POST':
        prepayment.product_name = request.form['product_name']
        prepayment.reference = request.form.get('reference', '')
        prepayment.item_year = int(request.form['item_year']) if request.form.get('item_year') else None
        prepayment.buy_price = float(request.form.get('buy_price', 0) or 0) if request.form.get('buy_price') else None
        prepayment.prepayment_amount = float(request.form['prepayment_amount'])
        prepayment.sell_price = float(request.form['sell_price'])
        prepayment.bonus = float(request.form.get('bonus', 0) or 0)
        prepayment.komplektatsiya = request.form.get('komplektatsiya', '').strip() or None
        prepayment.city_id = int(request.form['city_id'])
        if is_admin:
            prepayment.employee_id = int(request.form['employee_id'])
        if is_admin:
            prepayment.investor_id = int(request.form.get('investor_id')) if request.form.get('investor_id') else None
        prepayment.client_id = int(request.form.get('client_id')) if request.form.get('client_id') else None
        date_str = request.form['date']
        prepayment.date = datetime.strptime(date_str, '%Y-%m-%d') if date_str else datetime.utcnow()
        prepayment.comment = request.form.get('comment', '').strip() or None
        
        photo = request.files['photo']
        if photo and allowed_file(photo.filename):
            # Удаляем старое фото если есть
            if prepayment.photo:
                old_path = os.path.join(app.config['UPLOAD_FOLDER'], prepayment.photo)
                if os.path.exists(old_path):
                    os.remove(old_path)
            
            filename = generate_unique_filename(photo.filename)
            full_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            photo.save(full_path)
            # Оптимизируем изображение после сохранения
            optimize_image(full_path)
            prepayment.photo = filename
        
        # Обновление расходов
        for exp in prepayment.expenses_data:
            db.session.delete(exp)
        expense_type_ids = request.form.getlist('expense_type_id')
        amounts = request.form.getlist('expense_amount')
        comments = request.form.getlist('expense_comment')
        for et_id, amt, comment in zip(expense_type_ids, amounts, comments):
            if et_id and amt:
                expense = PendingPrepaymentExpense(
                    pending_prepayment_id=prepayment.id,
                    expense_type_id=int(et_id),
                    amount=float(amt),
                    comment=comment if comment else None
                )
                db.session.add(expense)
        
        db.session.commit()
        flash('Заявка на предоплату обновлена!', 'success')
        return redirect(url_for('prepayment'))
    
    new_client_id = request.args.get('new_client_id', type=int)
    new_client = Client.query.get(new_client_id) if new_client_id else None
    
    return render_template('edit_prepayment.html', 
                         prepayment=prepayment,
                         cities=cities, 
                         expense_types=expense_types, 
                         employees=employees, 
                         investors=investors, 
                         clients=clients_list,
                         new_client=new_client,
                         is_admin=is_admin)


@app.route('/prepayment/delete/<int:prepayment_id>', methods=['POST'])
@login_required
def delete_prepayment(prepayment_id):
    """Удаление заявки на предоплату"""
    prepayment = PendingPrepayment.query.get_or_404(prepayment_id)
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    
    # Менеджер может удалять только свои заявки
    if not is_admin and current_employee and prepayment.employee_id != current_employee.id:
        flash('У вас нет прав для удаления этой заявки.', 'error')
        return redirect(url_for('prepayment'))
    
    if prepayment.photo:
        photo_path = os.path.join(app.config['UPLOAD_FOLDER'], prepayment.photo)
        if os.path.exists(photo_path):
            os.remove(photo_path)
    
    db.session.delete(prepayment)
    db.session.commit()
    flash('Заявка на предоплату удалена!', 'success')
    return redirect(url_for('prepayment'))


@app.route('/prepayment/complete/<int:prepayment_id>', methods=['POST'])
@login_required
def complete_prepayment(prepayment_id):
    """Завершение сделки - перенаправление на страницу создания продажи из предоплаты"""
    prepayment = PendingPrepayment.query.get_or_404(prepayment_id)
    current_employee = get_current_user_employee()
    is_admin = session.get('is_admin', False)
    
    # Менеджер может завершать только свои заявки
    if not is_admin and current_employee and prepayment.employee_id != current_employee.id:
        flash('У вас нет прав для завершения этой сделки.', 'error')
        return redirect(url_for('prepayment'))
    
    if prepayment.status == 'converted_to_sale':
        flash('Эта предоплата уже переведена в продажу.', 'info')
        return redirect(url_for('prepayment'))
    
    # Перенаправляем на страницу добавления продажи с данными из предоплаты
    return redirect(url_for('add_sale', prepayment_id=prepayment_id))
    """Страница предоплаты (заглушка)"""
    flash('Функционал предоплаты в разработке', 'info')
    return redirect(url_for('main'))


if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    with app.app_context():
        db.create_all()
    app.run(debug=True)