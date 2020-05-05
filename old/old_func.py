def get_ohlcv_v2(exchange: object, symbol: str, t_frame: str, since: int):
    """
     Fetch OHLCV data from exchange.

     :param exchange: ccxt broker object
     :param t_frame: timeframe wanted
     :param since: data endtime, timestamp in millisecs
     :return: data as list | 0 -> error
     """
    if exchange.has['fetchOHLCV']:
        # check the t_frame parameter in timeframes list of the exchange
        if not (t_frame in exchange.timeframes.keys()):
            raise MyError('t_frame is not in ' + exchange.id + ' timeframes.')

        buff_time = exchange.milliseconds()
        # since = buff_time - n_days * 86400000  # -1 day from now # since = exchange.parse8601('2020-04-01T00:00:00Z')

        time_loop = since
        # Pagination method to fetch historial OHLCV
        all_OHLCV = []
        while time_loop < buff_time:
            OHLCV = exchange.fetch_ohlcv(symbol, t_frame, time_loop)
            if len(OHLCV):
                time_loop = OHLCV[len(OHLCV) - 1][0] + convert_to_ms(t_frame)  # widhdraw t_frame
                all_OHLCV += OHLCV
                print('Fetching:', symbol, t_frame, len(all_OHLCV))
            else:
                break
            time.sleep(exchange.rateLimit / 1000)

        # Data lenght check
        print('all_OHLCV:', len(all_OHLCV))
        print('taill calc:', ((buff_time - since) // (convert_to_ms(t_frame))) + 1)

        if len(all_OHLCV) == ((buff_time - since) // (convert_to_ms(t_frame))) + 1:
            return all_OHLCV
        else:
            MyWarning('data lenght asked does not match the data returned')
            return all_OHLCV
    else:
        MyWarning('No fetchOHLCV available for: ' + exchange.id)
        return 0


def get_ohlcv_db(exchange: object, symbol: str, t_frame: str, n_days: int, cursor: object):
    """NOT USED ANYMORE"""
    data = get_ohlcv(exchange, symbol, t_frame, n_days)
    if data != 0:
        # Get the pair_id from exchange.json in PATH_EXCH_INFO folder
        exchange_info = read_json(PATH_EXCH_INFO + exchange.id + '.json')
        pair_id = exchange_info[symbol]['id']

        # Name table : exchange_symbol_timeframe
        table_name = exchange.id + '_' + pair_id + '_' + t_frame
        create_table_db(table_name, cursor)

        # Insert data into db
        insert_pair_db(table_name, data, cursor)
        return 1
    else:
        return 0


def get_ohlcv(exchange: object, symbol: str, t_frame: str, n_days: int):
    """NOT USED ANYMORE"""
    if exchange.has['fetchOHLCV']:
        # check the t_frame parameter in timeframes list of the exchange
        if not (t_frame in exchange.timeframes.keys()):
            raise MyError('t_frame is not in ' + exchange.id + ' timeframes.')

        buff_time = exchange.milliseconds()
        since = buff_time - n_days * 86400000  # -1 day from now # since = exchange.parse8601('2020-04-01T00:00:00Z')

        # Pagination method to fetch historial OHLCV
        all_OHLCV = []
        while since < (buff_time):
            OHLCV = exchange.fetch_ohlcv(symbol, t_frame, since)
            if len(OHLCV):
                since = OHLCV[len(OHLCV) - 1][0] + convert_to_ms(t_frame)  # widhdraw t_frame
                all_OHLCV += OHLCV
            else:
                break
            time.sleep(exchange.rateLimit / 1000)

        # Data lenght check
        if len(all_OHLCV) != n_days:
            return all_OHLCV
        else:
            raise MyError('data lenght asked does not match the data returned')
            return 0
    else:
        MyWarning('No fetchOHLCV available for: ' + exchange.id)
        return 0


def get_ohlcv_db_v2(exchange: object, symbol: str, t_frame: str, tstamp_s_max: int, old_date: str, cursor: object):
    """
    Fetch OHLCV data from exchange and store it in database.

    :param exchange: ccxt exchange object
    :param symbol: symbol of the pair to fetch
    :param t_frame: timeframe wanted
    :param tstamp_s_max: start time timestamp in secs
    :param old_date: end time, format:'2020-04-01T00:00:00Z'
    :param cursor: cursor of the database
    :return: datat lenght | -1 -> error | 0 -> data aldready up to date
    """
    table_name = get_table_name(exchange, symbol, t_frame)

    tstamp_ms_max = tstamp_s_max * 1000
    tstamp_ms_min = exchange.parse8601(old_date)

    # Check if a table has aldready been created and fills it if not
    # Check if that table has aldready been filled and fills it if not
    if not (table_exists_db(table_name, cursor)) or not (data_exists_db(table_name, cursor)):
        data = get_ohlcv_v2(exchange, symbol, t_frame, tstamp_ms_min)
        if data != 0:
            create_table_db(table_name, cursor)
            # Insert data into db
            insert_pair_db(table_name, data, cursor)
            print('inside 1')
            return len(data)
        else:
            return -1

    t_frame_ms = convert_to_ms(t_frame)

    # Get the oldest and newest row time.
    max_tstamp_ms_db = max_column_db(table_name, 'tstamp', cursor)
    min_tstamp_ms_db = min_column_db(table_name, 'tstamp', cursor)

    if ((min_tstamp_ms_db - tstamp_ms_min) > t_frame_ms) or ((tstamp_ms_max - max_tstamp_ms_db) > t_frame_ms):
        # check oldest row in database
        if tstamp_ms_min < min_tstamp_ms_db:
            # delete all data in the table
            delete_all_row_db(table_name, cursor)
            # Fetch again all data from exchange server
            data = get_ohlcv_v2(exchange, symbol, t_frame, tstamp_ms_min)
            if data != 0:
                create_table_db(table_name, cursor)
                # Insert data into db
                insert_pair_db(table_name, data, cursor)
                print('inside 2')
                return len(data)
            else:
                return -1
        elif tstamp_ms_max > max_tstamp_ms_db:
            data = get_ohlcv_v2(exchange, symbol, t_frame, max_tstamp_ms_db + t_frame_ms)
            if data != 0:
                create_table_db(table_name, cursor)
                # Insert data into db
                insert_pair_db(table_name, data, cursor)
                print('inside 3')
                return len(data)
            else:
                return -1
    else:
        print('all data aldready in database:', table_name)
        return 0

    def test_bench1():
        '''This function is only here to validate subfuncitons in case of major changes in those'''
        db = DataBase()
        conn = DataBase().conn
        cursor = conn.cursor()

        print('\n########TESTS########>:\n')

        exch_list = ['binance', 'bitfinex', 'kraken']
        exch = {}
        for exch_id in exch_list:
            exchange_class = getattr(ccxt, exch_id)
            exch[exch_id] = exchange_class(key[exch_id])

        exchange = exch['binance']
        symbol = 'BTC/USDT'
        t_frame = '1d'
        newest_date = time.time()
        oldest_date = '2020-04-01T00:00:00Z'
        table_name = get_table_name(exchange, symbol, t_frame)

        get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor)

        print("test : delete row + add data")
        delete_all_row_db('binance_BTCUSDT_1d', cursor)
        get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor)
        print('test ok\n')

        print('test : drop table + add data')
        cursor.execute("""DROP TABLE %s""" % (table_name))
        get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor)
        print('test ok\n')

        # Remplacer 52 par le nb de jour au lancement du test
        print('test : get old data - 31 days')
        oldest_date = '2020-03-01T00:00:00Z'
        if get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor) == 52:
            print('test ok\n')
        else:
            print('test ko\n')

        print('test : fetch only newest data not in database')
        delete_last_6d_row_db(table_name, cursor)
        if get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor) == 6:
            print('test ok\n')
        else:
            print('test ko\n')

        t_frame = '5m'
        oldest_date = '2020-03-11T00:00:00Z'
        table_name2 = get_table_name(exchange, symbol, t_frame)

        print('test : timeframe = 5m')
        oldest_date = '2020-03-11T00:00:00Z'
        print('data_lenght:', get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor))

        print('test : timeframe = 5m, drop 5days of data')
        delete_last_6d_row_db(table_name2, cursor)
        print('data_lenght:', get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor))

        print('test: fetch only new and old data not in database')

        print('clean data base')
        cursor.execute("""DROP TABLE %s""" % (table_name))
        cursor.execute("""DROP TABLE %s""" % (table_name2))

        conn.commit()  # Save arbot database changes
        conn.close()  # Closes arbot database