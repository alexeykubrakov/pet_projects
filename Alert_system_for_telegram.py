import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
from airflow.decorators import dag, task
import pandahouse as ph
from datetime import date, time, timedelta
import io
import os

# Подключение к telegram боту
my_token = os.environ.get('REPORT_BOT_TOKEN')
bot = telegram.Bot(token=my_token)

# ID чата, в который будут пересылаться сообщения (здесь отсутствует реальный ID чата)
chat_id = -1

# База данных ClickHouse, с которой будет взаимодействие (здесь отсутствует реальная база)
database_name = 'database_name'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'database': database_name,
              'user': os.environ.get('DB_CONNECTION_USERNAME'),
              'password': os.environ.get('DB_CONNECTION_PASSWORD')}


# Функция для чтения данных из ClickHouse
def get_df_from_database(query):
    result = ph.read_clickhouse(query, connection=connection)
    return result

# Функция для вставки данных в ClickHouse
def insert_df_into_database(data, table_name):
    result = ph.to_clickhouse(data, table_name, index=False, connection=connection)
    return result

# Параметры, которые прокидываются в таски по умолчанию
default_args = {
    'owner': 'alexey-kubrakov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 1, 30),
}

# Интервал запуска DAG каждый день каждые 15 минут
schedule_interval = '15 * * * *'

# Число 15-минуток, по которым будут анализироваться метрики за вчерашний день и за 7 дней назад
n_time_periods = 3

# Получаем названия выбираемых столбцов/метрик
def get_select_vars(mertics, metrics_list, group_by_slices_list, slice, all_data=False):
    date_select = "toDate(time) as date, "

    time_select = '''toStartOfFifteenMinutes(time) as time, 
                     formatDateTime(toStartOfFifteenMinutes(time), '%R') as ts,'''

    select_expression_list = []
    for metric in mertics:
        formula, alias = '', ''
        
        # Если мы хотим посмотреть данные в разрезе по некоторому полю, то добавляем дополнительное поле в выборку
        if slice != 'total':
            formula = group_by_slices_list[slice]['formula']
            alias = group_by_slices_list[slice]['alias']
        else:
            formula = metrics_list[metric]['formula']
            alias = metrics_list[metric]['alias']
        
        expression = f'{formula} as {alias}'
        select_expression_list.append(expression)

    # Разделим по запятым все поля, чтобы не получить ошибку из-за финальной запятой перед блоком FROM
    select_expression = ','.join(select_expression_list)

    # Решаем, требуется ли нам выбирать поля со значением времени
    if not all_data:
        select_expression = date_select + select_expression
    else:
        select_expression = date_select + time_select + select_expression
    
    return select_expression

# Получаем названия таблиц для метрик
def get_from_table_name(mertics, metrics_list):
    table_name_list = []
    for metric in mertics:
        table_name = metrics_list[metric]['table_name']
        table_name_list.append(table_name)

    table_name_list = list(set(table_name_list))[0]
    table_name = str(table_name_list)
    return table_name

# Получаем условия фильтрации таблиц для метрик
def get_where_expression(where_expression_template, group_by_slices_list, time, slice, all_data=False):
    where_expression = ''' {where_expression_template} and {alias} in ({group_levels}) '''

    time_interval = 15
    day_0_min_time = time
    day_0_max_time = time + pd.Timedelta(minutes=time_interval)
    day_1_min_time = time - pd.Timedelta(minutes=time_interval) - pd.Timedelta(days=1)
    day_1_max_time = time + pd.Timedelta(minutes=(n_time_periods-1)*time_interval) - pd.Timedelta(days=1)
    day_7_min_time = time - pd.Timedelta(minutes=time_interval) - pd.Timedelta(days=7)
    day_7_max_time = time + pd.Timedelta(minutes=(n_time_periods-1)*time_interval) - pd.Timedelta(days=7)

    time_expression = f''' and ((time >= '{day_0_min_time}' and time  < '{day_0_max_time}') 
                            or (time >= '{day_1_min_time}' and time < '{day_1_max_time}')
                            or (time >= '{day_7_min_time}' and time < '{day_7_max_time}'))'''

    # Решаем, требуется ли нам фильтровать данные по времени
    if not all_data:
        where_expression_template = where_expression_template + time_expression
    else:
        where_expression_template = where_expression_template

    if slice == 'total':
        where_expression = where_expression_template
    else:
        group_levels = group_by_slices_list[slice]['group_levels']
        alias = group_by_slices_list[slice]['alias']
        group_levels = "'" + "', '".join(group_levels) + "'"

        where_expression = where_expression.format(where_expression_template=where_expression_template,
                                                   alias=alias,
                                                   group_levels=group_levels)
    return where_expression

# Получаем условия группировки таблиц для метрик
def get_group_by_expression(slice, group_by_slices_list, all_data=False):
    if not all_data:
        group_by_expression = ' date'
    else:
        group_by_expression = ' date, toStartOfFifteenMinutes(time) as time, ts'
    if slice != 'total':
        alias = group_by_slices_list[slice]['alias']
        group_by_expression += ',' + alias
    return group_by_expression

# Отсылаем алерт в виде текстового сообщения и снимка графика, показывающего динамику изменения метрики
def send_alert(plot_df, group, metric_name, metric_alias, slice_name, chat_id):
    day_0 = date.today().strftime("%Y-%m-%d")
    day_1 = (date.today() - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
    day_7 = (date.today() - pd.DateOffset(days=7)).strftime("%Y-%m-%d")

    sns.set(rc={'figure.figsize': (16, 10)})
    sns.set(font_scale=2)
    sns.set_style("white")
    plt.tight_layout()

    ax = sns.lineplot(data=plot_df[plot_df['date'].isin([day_0, day_1, day_7])].sort_values(['date', 'ts']),
                      x='ts', y=metric_alias,
                      hue='date',
                      hue_order=[day_7, day_1, day_0],
                      style='date',
                      style_order=[day_0, day_1, day_7],
                      size='date',
                      sizes=[4, 3, 3],
                      size_order=[day_0, day_1, day_7])
    ax.legend([day_7, day_1, day_0])

    # Выводим только некоторые подписи по оси X
    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 15 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)
    ax.set(xlabel='time')
    ax.set(ylabel=metric_name)
    ax.set_title('{}\n{}'.format(group, metric_name))

    # Ограничим значения по оси Y только положительными значениями
    ax.set(ylim=(0, None))

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = f'{metric_name}.png'
    plot_object.seek(0)
    plt.close()

    if slice_name == 'total':
        text = f'''Проблема с метрикой {metric_name} - сильное отклонение от вчера/неделю назад!\nГрафик алерта:'''
    else:
        text = f'''Проблема с метрикой {metric_name} - сильное отклонение от вчера/неделю назад\nв срезе {slice_name} - {}\nГрафик алерта:'''
    bot.sendMessage(chat_id=chat, text=text)
    bot.sendPhoto(chat_id=chat, photo=plot_object)

# Получаем данные для алерта в случае наличия аномальных данных по определённой таблице в определённом срезе
def get_alert(metrics_list, group_by_slices_list, where_expression_template, alert_data, time, database_name, chat_id):
    mertics = alert_data['mertics']
    slice = alert_data['slice']

    query = '''SELECT {select_vars} 
               FROM {from_table_name} 
               WHERE {where_expression} 
               GROUP BY {group_by_expression}'''

    # Заполняем шаблон SQL-запроса
    select_vars = get_select_vars(mertics, metrics_list, group_by_slices_list, slice)
    from_table_name = get_from_table_name(mertics, metrics_list)
    where_expression = get_where_expression(where_expression_template, group_by_slices_list, time, slice)
    group_by_expression = get_group_by_expression(slice, group_by_slices_list)

    alert_query = query.format(select_vars=select_vars,
                               from_table_name=from_table_name,
                               where_expression=where_expression,
                               group_by_expression=group_by_expression)

    all_data = get_df_from_database(alert_query)

    data_to_alerts = {}

    if slice != 'total':
        group_levels = group_by_slices_list[slice]['group_levels']
        alias = group_by_slices_list[slice]['alias']

        for group_level in group_levels:
            sliced_data = all_data[all_data[alias] == group_level].copy()
            data_to_alerts[group_level] = {'df': sliced_data, 'slice': slice, 'group_level': group_level}
    else:
        data_to_alerts['total'] = {'df': all_data, 'slice': 'total', 'group_level': 'total'}

    # Итерируемся для каждого среза по всем метрикам
    for group in data_to_alerts:
        alert_df = data_to_alerts[group]['df']
        slice_name = data_to_alerts[group]['slice']

        for metric in mertics:
            metric_alias = metrics_list[metric]['alias']
            metric_name = metrics_list[metric]['metric_name']
            alert_data = get_metric_alert(alert_df, metric, metric_alias, slice, group, day_threshhold=0.5)

            alert_data['metric'] = metric
            alert_data['metric_name'] = metric_name
            alert_data['slice'] = slice
            alert_data['group'] = group
            alert_data['group_level'] = group
            alert_data['time'] = time

            alerts_log_df = pd.DataFrame(alert_data, index=[0]).fillna(0)
            alerts_log_df = alerts_log_df[['time', 'day_0_value', 'day_1_value', 'day_7_value',
                                           'day_1_diff', 'day_7_diff', 'slice', 'group_level',
                                           'metric', 'metric_name', 'is_alert']]

            # Вставляем данные в базу логов
            insert_df_into_database(alerts_log_df, 'alerts_log')

            # Переходим к следующему этапу цикла, если метрика неаномальна
            if not alert_data['is_alert']:
                continue

            select_vars = get_select_vars(mertics, metrics_list, group_by_slices_list, slice, True)
            from_table_name = get_from_table_name(mertics, metrics_list)
            where_expression = get_where_expression(where_expression_template, group_by_slices_list, time, slice, True)
            group_by_expression = get_group_by_expression(slice, group_by_slices_list, True)

            plot_df_query = query.format(select_vars=select_vars,
                                         from_table_name=from_table_name,
                                         where_expression=where_expression,
                                         group_by_expression=group_by_expression)

            plot_df = get_df_from_database(plot_df_query)

            if slice != 'total':
                plot_df = plot_df.query(f"{slice} == '{group}'")

            plot_df['time'] = pd.to_datetime(plot_df['time'])
            plot_df = plot_df[plot_df['time'] <= time].copy()

            send_alert(plot_df, group, metric_name, metric_alias, slice_name, chat_id)


def get_metric_alert(alert_df, metric, metric_alias, slice, group, day_threshhold=0.3):
    alert_data = {}
    is_alert = 0

    # Для обнаружения аномалий сравниваем данные за сегодняшние 15 минут со вчерашним значением и значением за 7 дней назад
    day_0 = date.today().strftime("%Y-%m-%d")
    day_1 = (date.today() - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
    day_7 = (date.today() - pd.DateOffset(days=7)).strftime("%Y-%m-%d")
    day_0_value = alert_df[alert_df['date'] == day_0][metric].iloc[0]
    day_1_value = alert_df[alert_df['date'] == day_1][metric].iloc[0]
    day_7_value = alert_df[alert_df['date'] == day_7][metric].iloc[0]

    if metric in ('users_feed', 'users_msg', 'likes', 'views', 'messages'):
        day_1_value = day_1_value/(n_time_periods)
        day_7_value = day_7_value/(n_time_periods)

    if day_0_value <= day_7_value:
        day_7_diff = abs(day_0_value / day_7_value - 1)
    else:
        day_7_diff = abs(day_7_value / day_0_value - 1)

    if day_0_value <= day_1_value:
        day_1_diff = abs(day_0_value / day_1_value - 1)
    else:
        day_1_diff = abs(day_1_value / day_0_value - 1)

    alert_data['day_0_value'] = day_0_value
    alert_data['day_1_value'] = day_1_value
    alert_data['day_7_value'] = day_7_value
    alert_data['day_1_diff'] = day_1_diff
    alert_data['day_7_diff'] = day_7_diff
    alert_data['is_alert'] = is_alert

    # Если алерт отправлен, то в течении 3 часов мы не будем присылать его снова, чтобы не заспамить чат.
    query = f"""SELECT max(time) as last_time FROM simulator.alerts_log 
           WHERE is_alert = 1 and slice = '{slice}' and metric = '{metric_alias}' and group_level = '{group}' """
    last_time = get_df_from_database(query)
    last_time = last_time['last_time'].iloc[0]
    last_time = pd.to_datetime(last_time, format='%Y-%m-%d %H:%M:%S', errors='ignore')

    if last_time > pd.to_datetime('1970-01-01', format='%Y-%m-%d') and pd.Timedelta(pd.Timestamp('now') - last_time, unit='hours') < timedelta(hours=3):
        return alert_data

    if (day_1_value <= day_0_value <= day_7_value) or (day_7_value <= day_0_value <= day_1_value):
        return alert_data
    
    # Мы посчитаем значение аномальным и получим алерт только в том случае,
    # если мы привысили порог по значению отношения величины изменения метрики к ее величине
    # на определённое количество процентов (30% по умолчанию)
    if day_7_diff >= day_threshhold and day_1_diff >= day_threshhold:
        is_alert = 1
        alert_data['is_alert'] = is_alert
        return alert_data
    else:
        return alert_data


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_system(database_name, chat_id):
    
    # Получаем справочную информацию и шаблоны для построения алертов
    @task
    def get_alert_info(database_name):
        table_list = {'feed': 'feed_actions', 'messages': 'message_actions'}
        metrics_list = {
            'users_feed': {
                'alias': 'users_feed',
                'formula': 'uniqExact(user_id)',
                'metric_name': 'Users from feed',
                'table_name': f'{database_name}.{table_list.feed}'
            },
            'likes': {
                'alias': 'likes',
                'formula': "countIf(user_id, action='like')",
                'metric_name': 'Likes',
                'table_name': f'{database_name}.{table_list.feed}'
            },
            'views': {
                'alias': 'views',
                'formula': "countIf(user_id, action='view')",
                'metric_name': 'Views',
                'table_name': f'{database_name}.{table_list.feed}'
            },
            'ctr': {
                'alias': 'ctr',
                'formula': "countIf(user_id, action='like') / countIf(user_id, action='view')",
                'metric_name': 'CTR',
                'table_name': f'{database_name}.{table_list.feed}'
            },
            'lpu': {
                'alias': 'lpu',
                'formula': "countIf(user_id, action='like') / uniqExact(user_id)",
                'metric_name': 'Likes per user',
                'table_name': f'{database_name}.{table_list.feed}'
            },
            'vpu': {
                'alias': 'vpu',
                'formula': "countIf(user_id, action='view') / uniqExact(user_id)",
                'metric_name': 'Views per user',
                'table_name': f'{database_name}.{table_list.feed}'
            },
            'users_msg': {
                'alias': 'users_msg',
                'formula': 'uniqExact(user_id)',
                'metric_name': 'Users from messenger',
                'table_name': f'{database_name}.{table_list.messages}'
            },
            'messages': {
                'alias': 'messages',
                'formula': 'count(user_id)',
                'metric_name': 'Messages',
                'table_name': f'{database_name}.{table_list.messages}'
            },
            'mpu': {
                'alias': 'mpu',
                'formula': 'count(user_id) / uniqExact(user_id)',
                'metric_name': 'Messages per user',
                'table_name': f'{database_name}.{table_list.messages}'
            },
        }
        group_by_slices_list = {
            'os': {
                'alias': 'os',
                'group_levels': ['iOS', 'Android'],
                'formula': 'os',
            }
        }
        where_expression_template = ''' 
             toDate(time) in (today(), today() - 1, today() - 7) and formatDateTime(time, '%R') >= '01:00' '''

        return metrics_list, group_by_slices_list, where_expression_template

    # Запускаем систему алертов, проверяя в цикле все интересующие нас метрики
    @task
    def launch_alert_system(alerts, metrics_list, group_by_slices_list, where_expression_template):
        query = f'''select toStartOfFifteenMinutes(max(time)) - interval 15 minute as max_time
                  from {database_name}.feed_actions where toDate(time) >= today() - 1'''
        time = get_df_from_database(query)
        max_time = time['max_time'].iloc[0]

        # На каждом шаге итерации мы проверяем данные по определённой таблице в определённом срезе на наличие аномалий
        # и отправляем алерт, если нужно
        for alert_data in alerts:
            get_alert(metrics_list, group_by_slices_list, where_expression_template, alert_data, max_time, database_name, chat_id=chat_id)
    
    metrics_list, group_by_slices_list, where_expression_template = get_alert_info(database_name)
    
    # Указываем все интересующие нас метрики и разрезы, которые будет отслеживать наша система
    feed_mertics = ["users_feed", "likes", "views", "ctr", "lpu", "vpu"]
    message_mertics = ["users_msg", "messages", "mpu"]
    alerts = [
        {"mertics": feed_mertics, "slice": "total"},
        {"mertics": feed_mertics, "slice": "os"},
        {"mertics": message_mertics, "slice": "total"},
        {"mertics": message_mertics, "slice": "os"},
    ]
    
    launch_alert_system(alerts, metrics_list, group_by_slices_list, where_expression_template)

alert_system = alert_system(database_name, chat_id)
