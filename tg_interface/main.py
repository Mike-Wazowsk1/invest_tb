import asyncio

from User import User
import keyring
tg_token = keyring.get_password('tg','nikolay')
import logging
from tinkoff.invest import Client
from telegram import ReplyKeyboardMarkup, Update, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup, \
    InputMediaPhoto, InputMedia
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
)
from threading import Thread
from multiprocessing import Pool
from multiprocessing import Process

from strategy import support_offset

logging.basicConfig(format='%(levelname)s - %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)


async def start(update: Update, context) -> None:
    user = update.effective_user

    keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                [KeyboardButton(text='Инструкция')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('Данный бот предназначен для алгоритмической торговли с помощью брокера '
                                    'Тинькофф.Инвестиции', reply_markup=rep)


async def menu(update: Update, context) -> None:
    trade_flag = context.user_data.get('sup_process', None) or context.user_data.get('sup_process', None)
    if trade_flag:
        keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                    [KeyboardButton(text='Инструкция')], [KeyboardButton(text='Прекратить торговать')]]
        rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    else:
        keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                    [KeyboardButton(text='Инструкция')]]
        rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('Меню', reply_markup=rep)


async def get_token(update: Update, context) -> None:
    keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                [KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('Укажите токен', reply_markup=rep)


async def my_profile(update: Update, context) -> None:
    token_flag = context.user_data.get('token_flag', None)
    if token_flag:
        keyboard = [[KeyboardButton(text='Доходность по всем счетам')], [KeyboardButton(text='Тариф')],
                    [KeyboardButton(text='Свободных средств по всем счетам')],
                    [KeyboardButton(text='Меню')]]
        rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(text=f"""Выбери действие""", reply_markup=rep)
    else:
        keyboard = [[KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
        rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("""
                                 Для полного функционала необходимо указать свой токен API Tinkoff.\n\nЕсли вы не знаете,как это сделать, можете воспользоваться инструкцией""",
                                        reply_markup=rep)


async def all_profit(update: Update, context) -> None:
    invest_token = context.user_data.get('invest_token', None)
    token_flag = context.user_data.get('token_flag', None)
    keyboard = [[KeyboardButton(text='Доходность по всем счетам')], [KeyboardButton(text='Тариф')],
                [KeyboardButton(text='Свободных средств по всем счетам')],
                [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    if token_flag:
        with Client(invest_token) as client:
            user = User(client)
            balance = {k.name: f'{round(float(v), 3)}%' for k, v in zip(user.accounts, user.expected_yield)}
            balance_ans = ' '.join(f'{k}: {v}\n' for k, v in balance.items())
            await update.message.reply_text(text=f"""Доходность по счетам:\n {balance_ans}""", reply_markup=rep)
    else:
        keyboard = [[KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
        rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("""
                         Для полного функционала необходимо указать свой токен API Tinkoff.\n\nЕсли вы не знаете,как это сделать, можете воспользоваться инструкцией""",
                                        reply_markup=rep)


async def tariff(update: Update, context) -> None:
    token_flag = context.user_data.get('token_flag', False)
    invest_token = context.user_data.get('invest_token', None)
    keyboard = [[KeyboardButton(text='Доходность по всем счетам')], [KeyboardButton(text='Тариф')],
                [KeyboardButton(text='Свободных средств по всем счетам')],
                [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    if token_flag:
        with Client(invest_token) as client:
            user = User(client)
            await update.message.reply_text(
                text=f"""Тариф: {user.tariff.capitalize()}\nКомиссия: {float(user.fee)}%\nПлатеж: {user.pay} руб./мес.""",
                reply_markup=rep)
    else:
        keyboard = [[KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
        rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("""
                 Для полного функционала необходимо указать свой токен API Tinkoff.\n\nЕсли вы не знаете,как это сделать, можете воспользоваться инструкцией""",
                                        reply_markup=rep)


async def free_money(update: Update, context) -> None:
    token_flag = context.user_data.get('token_flag', False)
    invest_token = context.user_data.get('invest_token', None)
    keyboard = [[KeyboardButton(text='Доходность по всем счетам')], [KeyboardButton(text='Тариф')],
                [KeyboardButton(text='Свободных средств по всем счетам')],
                [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    if token_flag:
        with Client(invest_token) as client:
            user = User(client)
            balance = {k.name: f'{round(float(v[1]), 3)} руб.' for k, v in zip(user.accounts, user.available_money)}
            balance_ans = ' '.join(f'{k}: {v}\n' for k, v in balance.items())
            await update.message.reply_text(text=f"""Свободных средств по счетам:\n {balance_ans}""", reply_markup=rep)
    else:
        keyboard = [[KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
        rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("""
                 Для полного функционала необходимо указать свой токен API Tinkoff.\n\nЕсли вы не знаете,как это сделать, можете воспользоваться инструкцией""",
                                        reply_markup=rep)


async def init_token(update: Update, context) -> None:
    context.user_data['invest_token'] = update.message.text
    context.user_data['token_flag'] = True
    keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                [KeyboardButton(text='Инструкция')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('Токен сохранен', reply_markup=rep)


async def send_token(update: Update, context) -> None:
    invest_token = context.user_data.get('invest_token', None)
    token_flag = context.user_data.get('token_flag', None)
    keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                [KeyboardButton(text='Инструкция')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    if token_flag:
        await update.message.reply_text('Твой токен:', reply_markup=rep)
        await update.message.reply_text(f'{invest_token}', reply_markup=rep)
    else:
        await update.message.reply_text('Токен не указан', reply_markup=rep)


async def send_account(update: Update, context) -> None:
    account_id = context.user_data.get('account_id', None)
    keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                [KeyboardButton(text='Инструкция')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    if account_id:
        await update.message.reply_text(f'{context.user_data.get(account_id)}', reply_markup=rep)
    else:
        await update.message.reply_text('Аккаунт не выбран', reply_markup=rep)


async def reset_token(update: Update, context) -> None:
    token_flag = context.user_data.get('token_flag', None)
    keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                [KeyboardButton(text='Инструкция')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    if token_flag:
        context.user_data['invest_token'] = None
        context.user_data['token_flag'] = None
        await update.message.reply_text('Токен успешно сброшен', reply_markup=rep)
    else:
        await update.message.reply_text('Токен не указан', reply_markup=rep)


async def reset_account(update: Update, context) -> None:
    account_id = context.user_data.get('account_id', None)
    keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                [KeyboardButton(text='Инструкция')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    if account_id:
        context.user_data['account_id'] = None
        await update.message.reply_text('Аккаунт успешно сброшен', reply_markup=rep)
    else:
        await update.message.reply_text('Аккаунт не выбран', reply_markup=rep)


async def sup_offset(update: Update, context) -> None:
    invest_token = context.user_data.get('invest_token', None)
    token_flag = context.user_data.get('token_flag', None)

    keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                [KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    if not token_flag:
        keyboard = [[KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
        rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("""
                Для полного функционала необходимо указать свой токен API Tinkoff.\n\nЕсли вы не знаете,как это сделать, можете воспользоваться инструкцией""",
                                        reply_markup=rep)
    else:
        if not context.user_data.get('account_id', None):
            with Client(invest_token) as client:
                user = User(client)
                accounts = {acc.name: acc.id for acc in user.accounts}
                names = []
                for name in accounts.keys():
                    context.user_data[f'{name}'] = accounts.get(name, 0)
                    context.user_data[f'{accounts.get(name, 0)}'] = name
                    names.append([KeyboardButton(text=f'Аккаунт: {name}')])
                names.append([KeyboardButton(text='Меню')])
                rep = ReplyKeyboardMarkup(names, resize_keyboard=True)
                await update.message.reply_text('Выбери аккаунт, на котором будут происходить торговля',
                                                reply_markup=rep)
        else:
            keyboard = [[KeyboardButton(text='Начать торговлю от уровней')], [KeyboardButton(text='Сменить аккаунт')],
                        [KeyboardButton(text='Стратегии')], [KeyboardButton(text='Меню')]]
            rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(text='Начинаем?', reply_markup=rep)


async def ma(update: Update, context) -> None:
    invest_token = context.user_data.get('invest_token', None)
    token_flag = context.user_data.get('token_flag', None)
    keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                [KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    if not token_flag:
        keyboard = [[KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
        rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("""
                Для полного функционала необходимо указать свой токен API Tinkoff.\n\nЕсли вы не знаете,как это сделать, можете воспользоваться инструкцией""",
                                        reply_markup=rep)
    else:
        if not context.user_data.get('account_id', None):
            with Client(invest_token) as client:
                user = User(client)
                accounts = {acc.name: acc.id for acc in user.accounts}
                names = []
                for name in accounts.keys():
                    context.user_data[f'{name}'] = accounts.get(name, 0)
                    names.append([KeyboardButton(text=f'Аккаунт: {name}')])
                names.append([KeyboardButton(text='Меню')])
                rep = ReplyKeyboardMarkup(names, resize_keyboard=True)
                await update.message.reply_text('Выбери аккаунт, на котором будут происходить торговля',
                                                reply_markup=rep)
        else:
            keyboard = [[KeyboardButton(text='Начать торговлю от скользящих средних')],
                        [KeyboardButton(text='Сменить аккаунт')],
                        [KeyboardButton(text='Стратегии')],
                        [KeyboardButton(text='Меню')]]
            rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(text='Начинаем?', reply_markup=rep)


async def init_account(update: Update, context) -> None:
    account_id = context.user_data.get(update.message.text.replace('Аккаунт: ', ''))
    context.user_data['account_id'] = account_id
    keyboard = [[KeyboardButton(text='Мой профиль')], [KeyboardButton(text='Стратегии')],
                [KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('Аккаунт выбран', reply_markup=rep)


async def sup_start(update: Update, context, sup_trading=True) -> None:
    keyboard = [[KeyboardButton(text='Мой профиль')],
                [KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    def sup_off_trading(update: Update, context) -> None:
        invest_token = context.user_data.get('invest_token', None)

        account_id = context.user_data.get('account_id', None)

        strat = support_offset.Support_Offset(token=invest_token)

        strat.start(True, account_id=account_id)

    await update.message.reply_text('Робот начал тоговать', reply_markup=rep)
    p = Process(target=sup_off_trading, args=(update, context))
    context.user_data['sup_process'] = p
    p.start()


async def ma_trading(update: Update, context) -> None:
    invest_token = context.user_data.get('invest_token', None)
    account_id = context.user_data.get('account_id', None)

    keyboard = [[KeyboardButton(text='Мой профиль')],
                [KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
    strat = support_offset.Support_Offset(token=invest_token)
    asyncio.create_task(strat.start(True, account_id=account_id))
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('Аккаунт выбран', reply_markup=rep)


async def instruction(update: Update, context) -> None:
    keyboard = [[KeyboardButton(text='Как получить токен')], [KeyboardButton(text='Как пользоваться ботом')],
                [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('С чем нужна помощь?', reply_markup=rep)


async def instruction_token(update: Update, context) -> None:
    keyboard = [[KeyboardButton(text='Как пользоваться ботом')],
                [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    photos = [InputMediaPhoto(open('settings.jpg', 'rb')), InputMediaPhoto(open('select_token.jpg', 'rb')),
              InputMediaPhoto(open('generate_token.jpg', 'rb'),
                              caption="Для получения токена API Тинькофф Инвестиции необходимо:\n\n\n"
                                      "1) Зайти на сайт Тинькофф Инвестиции(https://www.tinkoff.ru/invest/settings/) в раздел Настройки\n\n"
                                      "2) Найти пункт Токены Tinkoff Invest API и нажать на кнопку Создать токен\n\n"
                                      "3) Выбрать Полный доступ ко всем счетам и нажать на кнопку Выпустить токен")]
    await update.message.reply_media_group(photos)


async def instruction_bot(update: Update, context) -> None:
    keyboard = [[KeyboardButton(text='Как получить токен')],
                [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(text="""
            Для того,чтобы пользоваться ботом, необходимо:\n\n\n1) Вставить в чат токен Tinkoff Инвестиции API.\n\n2) Выбрать стратегию, с помощью которой вы планирутете работать.\n\n3) Выбрать счет, на котором будет происходить торговля.""",
                                    reply_markup=rep)


async def select_strategy(update: Update, context) -> None:
    keyboard = [[KeyboardButton(text='От уровней')],
                [KeyboardButton(text='Скользящие средние')], [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('На данный момент доступны следующие стратегии:', reply_markup=rep)


async def stop_trading(update: Update, context) -> None:
    keyboard = [[KeyboardButton(text='Остановить торговлю от уровней')],
                [KeyboardButton(text='Остановить торговлю по скользящим средним')], [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('Какого робота остановим?', reply_markup=rep)


async def sup_stop(update: Update, context) -> None:
    sup_process = context.user_data.get('sup_process')
    sup_process.terminate()
    sup_process.join()
    keyboard = [[KeyboardButton(text='Остановить торговлю от уровней')],
                [KeyboardButton(text='Остановить торговлю по скользящим средним')], [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('Робот остановлен', reply_markup=rep)


async def ave_me(update: Update, context) -> None:
    keyboard = [[KeyboardButton(text='Мой профиль')],
                [KeyboardButton(text='Инструкция')], [KeyboardButton(text='Меню')]]
    rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text('Гений с большим членом: @god_cant_see_me', reply_markup=rep)


def trading():
    pass


def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(tg_token).build()

    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("get_token", send_token))
    application.add_handler(CommandHandler("get_account", send_account))
    application.add_handler(CommandHandler("reset_account", reset_account))
    application.add_handler(CommandHandler("reset_token", reset_token))
    application.add_handler(CommandHandler("ave_me", ave_me))
    application.add_handler(MessageHandler(filters.Regex('Стратегии'), select_strategy))
    application.add_handler(MessageHandler(filters.Regex('От уровней'), sup_offset))
    application.add_handler(MessageHandler(filters.Regex('Скользящие средние'), ma))
    application.add_handler(MessageHandler(filters.Regex('Указать токен'), get_token))
    application.add_handler(MessageHandler(filters.Regex('Мой профиль'), my_profile))
    application.add_handler(MessageHandler(filters.Regex('Доходность по всем счетам'), all_profit))
    application.add_handler(MessageHandler(filters.Regex('Тариф'), tariff))
    application.add_handler(MessageHandler(filters.Regex('Свободных средств'), free_money))
    application.add_handler(MessageHandler(filters.Regex('Аккаунт'), init_account))
    application.add_handler(MessageHandler(filters.Regex('Начать торговлю от уровней'), sup_start))
    application.add_handler(MessageHandler(filters.Regex('Остановить торговлю от уровней'), sup_stop))
    application.add_handler(MessageHandler(filters.Regex('Меню'), menu))
    application.add_handler(MessageHandler(filters.Regex('Прекратить торговать'), stop_trading))
    application.add_handler(MessageHandler(filters.Regex('Инструкция'), instruction))
    application.add_handler(MessageHandler(filters.Regex('Как получить токен'), instruction_token))
    application.add_handler(MessageHandler(filters.Regex('Сменить аккаунт'), reset_account))
    application.add_handler(MessageHandler(filters.Regex('Как пользоваться ботом'), instruction_bot))  #
    application.add_handler(MessageHandler(filters.Regex('Начать торговлю от скользящих средних'), sup_start))
    application.add_handler(MessageHandler(filters.Regex('-'), init_token))
    # on non command i.e message - echo the message on Telegram
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, start))

    # Run the bot until the user presses Ctrl-C

    application.run_polling()


if __name__ == "__main__":
    main()
