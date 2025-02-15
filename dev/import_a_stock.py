import os
import sqlite3
from configparser import ConfigParser
from pytdx.hq import TdxHq_API
from pytdx.config.hosts import hq_hosts
import akshare as ak
from akshare.stock_feature.stock_hist_em import stock_zh_a_hist
from hikyuu.data.common_h5 import open_h5file, get_h5table, update_hdf5_extern_data
from hikyuu.util.mylog import *
from hikyuu.data.pytdx_to_h5 import import_one_stock_data
from hikyuu.data.common_sqlite3 import get_stktype_list, get_codepre_list, get_marketid


def remove_h5table(h5file, market, code):
    try:
        group = h5file.get_node("/", "data")
    except:
        return

    tablename = market.upper() + code
    try:
        h5file.remove_node(group, tablename, True)
    except:
        return


def get_stock_record(connect, code):
    assert len(code) == 8, 'code must set as market+code, eg: sh600001/sz300300'
    marketid = get_marketid(connect, code[0:2].upper())
    code = code[2:]
    stktype_list = get_stktype_list(['stock', 'fund', 'bond'])
    sql = f'''select stockid, marketid, code, valid, type, startDate, endDate from stock where marketid={marketid} and code="{code}" and type in {stktype_list}'''
    cur = connect.cursor()
    a = cur.execute(sql).fetchall()
    connect.commit()
    cur.close()
    return a


def add_del_stocks(market):
    ''' 插入退市股信息到基础信息数据库
    :param: market
        市场字母代码，目前仅支持SH/SZ
    '''

    assert market.upper() in ['SH', 'SZ'], 'only implemented for market SH/SZ'
    config = ConfigParser()
    config.read(os.path.expanduser('~/.hikyuu/importdata-gui.ini'), encoding='utf-8')
    sqlite_file = f"{config['hdf5']['dir']}/stock.db"

    # 连接数据库
    connect = sqlite3.connect(sqlite_file, timeout=1800)
    cursor = connect.cursor()

    try:
        marketid = get_marketid(connect, market)
        codepre_types = get_codepre_list(connect, marketid, ['stock'])

        # 获取退市股列表
        dllist = ak.stock_info_sh_delist() if market.upper() == 'SH' else ak.stock_info_sz_delist('终止上市公司')

        # 批量插入或更新
        for code, name, d1, d2 in dllist.values:
            startDate = str(d1).replace('-', '')
            endDate = str(d2).replace('-', '')

            for codepre in codepre_types:
                length = len(codepre[0])
                if code[:length] == codepre[0]:
                    # 使用参数化查询避免 SQL 注入
                    check_sql = """
                        SELECT COUNT(*) FROM Stock WHERE marketid = ? AND code = ?
                    """
                    cursor.execute(check_sql, (marketid, code))
                    exists = cursor.fetchone()[0] > 0

                    if exists:
                        # 如果存在，执行 UPDATE 操作
                        update_sql = """
                            UPDATE Stock 
                            SET name = ?, type = ?, valid = 0, startDate = ?, endDate = ?
                            WHERE marketid = ? AND code = ?
                        """
                        cursor.execute(update_sql, (name, codepre[1], startDate, endDate, marketid, code))
                    else:
                        # 否则执行 INSERT 操作
                        insert_sql = """
                            INSERT INTO Stock(marketid, code, name, type, valid, startDate, endDate)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                        cursor.execute(insert_sql, (marketid, code, name, codepre[1], 0, startDate, endDate))

        # 提交事务
        connect.commit()

    except Exception as e:
        hku_error(f"Error: {e}")
        connect.rollback()

    finally:
        # 确保资源关闭
        cursor.close()
        connect.close()


def import_one_delstock_data(h5file, market, stock_record, forceall=False):
    ''' 获取单支退市股日线数据 '''
    stockid, marketid, code, valid, stktype, startDate, endDate = stock_record
    latest_kldate = str(startDate)
    table = get_h5table(h5file, market, code)

    # 清理已有数据（如果设置 forceall）
    if forceall and table.nrows > 0:
        remove_h5table(h5file, market, code)
        table = get_h5table(h5file, market, code)
    elif table.nrows > 0:
        hku_info(f'退市股票 {code} 已有历史数据，不处理。如果需要重新获取数据需设置 forceall=True')
        return 0

    # 获取退市股票日线数据
    dhist = stock_zh_a_hist(code, start_date=latest_kldate)

    if dhist.empty:
        hku_info(f'退市股票 {code} 无历史数据')
        return 0

    # 数据处理并批量插入
    add_record_count = 0
    row = table.row
    for d, code, o, c, h, l, vol, amt, zf, zdf, zd, hsl in dhist.values:
        row['datetime'] = (d.year * 10000 + d.month * 100 + d.day) * 10000  # datetime
        row['openPrice'] = o * 1000
        row['highPrice'] = h * 1000
        row['lowPrice'] = l * 1000
        row['closePrice'] = c * 1000
        row['transAmount'] = round(amt * 0.001)
        row['transCount'] = round(vol)
        row.append()
        add_record_count += 1

    if add_record_count > 0:
        table.flush()
        update_hdf5_extern_data(h5file, market.upper() + code, 'DAY')
        hku_info(f'import f{add_record_count} rows for {code}')

    return add_record_count


def import_delstocks_kdata(market):
    ''' 导入退市股日线数据 '''
    config = ConfigParser()
    config.read(os.path.expanduser('~/.hikyuu/importdata-gui.ini'), encoding='utf-8')
    sqlite_file = f"{config['hdf5']['dir']}/stock.db"

    # 连接数据库
    connect = sqlite3.connect(sqlite_file, timeout=1800)
    marketid = get_marketid(connect, market)
    stktype_list = get_stktype_list(['stock', 'fund', 'bond'])

    # 确保 stktype_list 为元组
    if isinstance(stktype_list, list):
        stktype_list = tuple(stktype_list)

    sql = f'''
        SELECT stockid, marketid, code, valid, type, startDate, endDate 
        FROM stock 
        WHERE marketid=? AND valid=? AND endDate<? AND type IN ({','.join(['?'] * len(stktype_list))})
    '''
    cur = connect.cursor()
    cur.execute(sql, (marketid, 0, 99999999, *stktype_list))
    delrecords = cur.fetchall()
    cur.close()
    connect.close()

    # 处理 HDF5 文件
    dest_dir = config['hdf5']['dir']
    h5file = open_h5file(dest_dir, market, 'DAY')

    for rec in delrecords:
        import_one_delstock_data(h5file, market, rec)

    h5file.close()


def import_stock_kline(code, ktype='DAY', forceall=False):
    '''获取单支股票的K线数据, 本地数据有错误的话可以使用，退市股只能获取日线数据.

    :param ktype: K线类型, DAY | 1MIN | 5MIN
    :param forceall: 是否强制更新所有数据

        True 删除已存数据(注意：小于日线级别的数据删除后可能无法获取全部数据)，重新获取全部数据

        False 追加数据
    :rtype: int
    '''
    config = ConfigParser()
    config.read(os.path.expanduser('~/.hikyuu/importdata-gui.ini'), encoding='utf-8')
    sqlite_file = f"{config['hdf5']['dir']}/stock.db"
    connect = sqlite3.connect(sqlite_file, timeout=1800)

    stock_record = get_stock_record(connect, code)
    if len(stock_record) == 0:
        print('no record found, import stock data or add delstocks first!')
        return

    market = code[0:2]
    dest_dir = config['hdf5']['dir']
    h5file = open_h5file(dest_dir, market, ktype)
    stock_record = stock_record[0]
    if (stock_record[3] == 0 and stock_record[6] < 99999999):
        import_one_delstock_data(h5file, market, stock_record, forceall)
        h5file.close()
        return

    api = TdxHq_API()
    host = hq_hosts[5]
    api.connect(host[1], host[2])

    if forceall:
        table = get_h5table(h5file, market, code[2:])
        if table is not None and table.nrows > 0:
            remove_h5table(h5file, market, code[2:])
            table = get_h5table(h5file, market, code[2:])

    add_count = import_one_stock_data(connect, api, h5file, market, ktype, stock_record)
    h5file.close()
    if add_count > 0:
        hku_info(f'successfully import {add_count} records')
    hku_info(f'import finished {code}')


if __name__ == '__main__':
    c = 'sh880011'
    import_stock_kline(c, forceall=True)
