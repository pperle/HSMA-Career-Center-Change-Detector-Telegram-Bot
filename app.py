import json
import sqlite3
from sqlite3 import Connection, Cursor
from typing import List, Tuple, Union

import pandas as pd
import telegram
from diff_match_patch import diff_match_patch
from pandas.core.frame import DataFrame
from pandas.core.series import Series

URL = r'https://www.career.hs-mannheim.de/fuer-studierende/veranstaltungsangebot/themenuebersicht.html'
DB = 'bot.db'


def setup_bot():
    """
    Setup bot. Read token from json.

    :return: bot
    """
    with open("config.json") as json_file:
        json_data = json.load(json_file)

    return telegram.Bot(token=json_data['token'])


def setup_sqlite() -> Tuple[Connection, Cursor]:
    """
    Create table if table does not yet exist.

    :return: Connection, Cursor
    """
    conn = sqlite3.connect(DB)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS "CareerCenter"
(
    "Thema"             TEXT,
    "Zeitlicher_Umfang" TEXT,
    "Anerkennung"       TEXT,
    "Termin"            TEXT,
    "Uhrzeit"           TEXT,
    "Raum"              TEXT,
    "Anmeldung"         TEXT,
    "table_num"         INTEGER,
    PRIMARY KEY ("Thema", "table_num")
);''')
    conn.commit()

    return conn, cursor


def table_preprocessing(df_table: DataFrame, idx: int) -> DataFrame:
    """
    Group by 'Thema' and add 'table_num' in order to eliminate duplicate entries.

    :param df_table: input DataFrame
    :param idx: table_num
    :return: DataFrame grouped by 'Thema'
    """
    df_table = df_table.where((pd.notnull(df_table)), '')
    filtered_columns = list(df_table)[1:]
    df_grouped = df_table.groupby(list(df_table)[:1], as_index=False)[filtered_columns].agg(lambda col: ', '.join(col))
    df_grouped['table_num'] = idx
    return df_grouped


def check_for_change(row: Series, conn: Connection, cursor: Cursor) -> None:
    """
    Check if row differs from information saved in database.

    :param row: table row from HTML
    :param conn: db Connection
    :param cursor: db Cursor
    :return:
    """
    if row.isnull().all():
        return

    result_html = parse_row(row)
    if result_html[0].strip() == '':
        return

    cursor.execute('SELECT * FROM CareerCenter WHERE Thema=? AND table_num=?', (result_html[0], int(result_html[-1])))
    result_db = cursor.fetchone()

    if result_db is None:
        cursor.execute('INSERT INTO CareerCenter VALUES (?,?,?,?,?,?,?,?)', result_html)
        conn.commit()

        generate_message(result_html)
    else:
        result = ()
        change_detected = False
        for entry_db, entry_html in zip(result_db, result_html):
            diff = calc_str_diff(str(entry_db), str(entry_html))

            thema, ue, bv, termine, uhrzeit, raum, anmeldung, table_num = result_html
            num_changed_elements = sum([elem[0] != 0 for elem in diff])
            if num_changed_elements > 0:
                change_detected = True
                result += (generate_pretty_diff(diff),)
            else:
                result += (entry_db,)

        if change_detected:
            cursor.execute('''UPDATE CareerCenter
                SET Zeitlicher_Umfang=?,
                    Anerkennung=?,
                    Termin=?,
                    Uhrzeit=?,
                    Raum=?,
                    Anmeldung=?
                WHERE Thema = ?
                  AND table_num = ?''', (ue, bv, termine, uhrzeit, raum, anmeldung, thema, table_num))
            conn.commit()

            generate_message(result)


def parse_row(row: Series) -> Tuple[str, Union[str, None], Union[str, None], Union[str, None], Union[str, None], Union[str, None], Union[str, None], int]:
    """
    Rows can easily have different names. This function tries to find the corresponding row.

    :param row: Row from HTML
    :return: row with predefined columns
    """
    thema = row.filter(like="Thema").get(0, '')
    ue = row.filter(like="(UE)").get(0, '')
    bv = row.filter(like="Anerkennung").get(0, '')
    termine = row.filter(like="Termin").get(0, '')
    uhrzeit = row.filter(like="Uhrzeit").get(0, '')
    raum = row.filter(like="Raum").get(0, '')
    anmeldung = row.filter(like="Anmeldung").get(0, '')
    table_num = row.filter(like="table_num").get(0, '')
    return thema, ue, bv, termine, uhrzeit, raum, anmeldung, table_num


def generate_message(result_html: Tuple[str, str, str, str, str, str, str, int]) -> None:
    """
    Send message via Telegram.

    :param result_html: data to send to the bot
    :return:
    """
    message = ''
    for title, info in zip(['Thema', 'UE', 'Fakultät', 'Termin(e)', 'Uhrzeit', 'Raum', 'Anmeldung'], result_html):
        message += title + ': ' + info + "\n"
    print(message)
    print("--------------")
    # bot.send_message(chat_id="@HSMACCCD", text=message, parse_mode=telegram.ParseMode.MARKDOWN)


def calc_str_diff(original_str: str, new_str: str) -> List[Tuple[int, str]]:
    """
    Calculate String diff between `original_str` and `new_str`.

    :param original_str: original (db) entry
    :param new_str: entry from html
    :return: List of changes.
    """
    dmp = diff_match_patch()
    diff = dmp.diff_main(original_str, new_str)
    dmp.diff_cleanupSemantic(diff)
    return diff


def generate_pretty_diff(diff):
    """
    Create a visualization of the diff.

    :param diff:
    :return:
    """
    pretty_dif = ''
    for elem in diff:
        if elem[0] == 0:
            pretty_dif += elem[1]
        elif elem[0] == -1:
            pretty_dif += ' \u0336'.join(elem[1]) + '\u0336'  # strikethrough text
        elif elem[0] == 1:
            pretty_dif += '**' + elem[1] + '**'  # bold text
    return pretty_dif


def main() -> None:
    global bot
    bot = setup_bot()
    conn, cursor = setup_sqlite()
    tables = pd.read_html(URL)
    for idx, df_table in enumerate(tables):
        df_table = table_preprocessing(df_table, idx)
        df_table.apply(lambda row: check_for_change(row, conn, cursor), axis=1)
    conn.close()


if __name__ == '__main__':
    main()
