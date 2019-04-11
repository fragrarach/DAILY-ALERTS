from sigm import *


ALERT_TABLE_NAMES = [
    'order_header',
    'order_line'
]


def get_user_names(table_name):
    sql_exp = f'SELECT DISTINCT user_name FROM {table_name}'
    result_set = sql_query(sql_exp, log_db_cursor)
    user_names = tabular_data(result_set)
    return user_names


def get_order_numbers(table_name, user_name):
    sql_exp = f"SELECT DISTINCT ord_no FROM {table_name} WHERE user_name = '{user_name}'"
    result_set = sql_query(sql_exp, log_db_cursor)
    order_numbers = tabular_data(result_set)
    return order_numbers


def check_alerts(order_number):
    alerts = []

    sql_exp = f"SELECT * FROM alert_zero_quantity({order_number})"
    result_set = sql_query(sql_exp, sigm_db_cursor)
    zero_quantity_check = tabular_data(result_set)
    if zero_quantity_check:
        alerts.append('ZERO QUANTITY')

    sql_exp = f"SELECT * FROM alert_complete_blanket({order_number})"
    result_set = sql_query(sql_exp, sigm_db_cursor)
    complete_blanket_check = tabular_data(result_set)
    if complete_blanket_check:
        alerts.append('COMPLETE BLANKET')

    return alerts


def set_user_name_list():
    user_names = []
    for table_name in ALERT_TABLE_NAMES:
        for row in get_user_names(table_name):
            user_name = row[0]
            if user_name not in user_names:
                user_names.append(user_name)
    return user_names


def set_order_number_list(user_name):
    order_numbers = []
    for table_name in ALERT_TABLE_NAMES:
        for row in get_order_numbers(table_name, user_name):
            order_number = row[0]
            if order_number not in order_numbers:
                order_numbers.append(order_number)
    return order_numbers


def set_user_order_list():
    user_orders = []
    user_names = set_user_name_list()
    for user_name in user_names:
        user = [user_name]
        order_numbers = set_order_number_list(user_name)
        user.append(order_numbers)
        user_orders.append(user)
    return user_orders


def main():
    global sigm_connection, sigm_db_cursor, log_connection, log_db_cursor
    sigm_connection, sigm_db_cursor = sigm_connect()
    log_connection, log_db_cursor = log_connect()

    add_sql_files()

    user_orders = set_user_order_list()

    master = []
    for user in user_orders:
        user_name = [user[0]]
        order_numbers = user[1]
        for order_number in order_numbers:
            alerts = check_alerts(order_number)
            if alerts:
                order_alerts = [order_number, alerts]
                user_name.append(order_alerts)
        master.append(user_name)
    print(master)


main()
