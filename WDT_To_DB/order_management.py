#-*- coding:utf-8 -*-
import sys
import requests
from datetime import datetime, timedelta
import time
import math
sys.path.append("../..")
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from public.basic_function import DataBase_baidu, RandomUserAgent, MailGunner, PlatformSign



def import_into_database(wb_data,table_name, sid, db_baidu):
    conn = db_baidu.conn
    cur = db_baidu.cur

    sql_db = "select trade_id, sid from %s" % (table_name)
    cur.execute(sql_db)
    conn.commit()
    res_db = cur.fetchall()

    sql_insert_orders = f"""
        insert ignore into {table_name} (
        trade_id, trade_no, platform_id, shop_platform_id, shop_no, shop_name, shop_remark, warehouse_type, warehouse_no, src_tids, trade_status,consign_status,trade_type, refund_status, trade_time, pay_time,stockout_no, trade_from,trade_from_CN, modified, created, fenxiao_tid
        ) values (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    sql_update_orders = f"""
        update {table_name} 
        set trade_no, platform_id, shop_platform_id, shop_no, shop_name, shop_remark, warehouse_type, warehouse_no, src_tids, trade_status,consign_status,trade_type, refund_status, trade_time, pay_time,stockout_no, trade_from,trade_from_CN, modified, created, fenxiao_tid 
        where trade_id=%s and sid=%s
    """


    platform_mapping = {
        '0': '线下', '1': '淘宝', '2': '淘宝分销', '3': '京东', '14': '唯品会', '32': '微店', '39': '拼多多',
        '69': '放心购(抖店）', '139': '抖音供销'
    }

    trade_status_mapping = {
        '10': '未确认', '20': '待尾款', '30': '待发货', '40': '部分发货', '50': '已发货',
        '60': '已签收', '70': '已完成', '80': '已退款', '90': '已关闭'
    }


    # 设置每隔多少条SQL语句提交一次事务
    commit_interval = 500
    commit_counter = 0

    order_list = wb_data['response']['trades']
    # print(wb_data)

    for order_single in order_list:
        trade_id = order_single['trade_id']
        trade_no = order_single['trade_no']
        platform_id = order_single['platform_id']
        platform_id = str(platform_id)
        shop_platform_id = order_single['shop_platform_id']
        shop_no = order_single['shop_no']
        shop_name = order_single['shop_name']
        shop_remark = order_single['shop_remark']
        warehouse_type = order_single['warehouse_type']
        warehouse_no = order_single['warehouse_no']
        src_tids = order_single['src_tids']
        trade_status = order_single['trade_status']
        consign_status = order_single['consign_status']
        trade_type = order_single['trade_type']
        refund_status = order_single['refund_status']
        trade_time = order_single['trade_time']
        pay_time = order_single['pay_time']
        stockout_no = order_single['stockout_no']
        trade_from = str(order_single['trade_from'])
        if trade_from == '1':
            trade_from_CN = 'API抓单'
        elif trade_from == '2':
            trade_from_CN = '手工建单'
        elif trade_from == '3':
            trade_from_CN = 'excel导入'
        elif trade_from == '4':
            trade_from_CN = 'API抓单'
        else:
            trade_from_CN = '现款销售'
        modified = order_single['modified']
        created = order_single['created']
        fenxiao_tid = order_single['fenxiao_tid']

        if (str(trade_id),str(sid)) in [(d[0],d[1]) for d in res_db]:
            values2 = (
            trade_no, platform_id, shop_platform_id, shop_no, shop_name, shop_remark, warehouse_type, warehouse_no, src_tids, trade_status,consign_status,trade_type, refund_status, trade_time, pay_time,stockout_no, trade_from,trade_from_CN, modified, created, fenxiao_tid, trade_id, sid)
            cur.execute(sql_update_orders, values2)
        else:
            values3 = (trade_id, trade_no, platform_id, shop_platform_id, shop_no, shop_name, shop_remark, warehouse_type, warehouse_no, src_tids, trade_status,consign_status,trade_type, refund_status, trade_time, pay_time,stockout_no, trade_from,trade_from_CN, modified, created, fenxiao_tid)
            cur.execute(sql_insert_orders, values3)
        #上面的if else结束后，就可以增加commit_counter的次数
        commit_counter += 1
        if commit_counter % commit_interval == 0:
            conn.commit()
            commit_counter = 0  # 重置计数器

    # 如果有剩余的SQL语句未提交，则在最后提交一次事务
    if commit_counter % commit_interval != 0:
        conn.commit()




def get_response_five_times(url, j, appsecret, params, headers):
    keys_to_remove = ['timestamp', 'page_no', 'sign']
    params = {key: value for key, value in params.items() if key not in keys_to_remove}
    now_no_strf = datetime.now()
    now = now_no_strf.strftime("%Y-%m-%d %H:%M:%S")
    params['timestamp'] = now
    params['page_no'] = j
    sign = PlatformSign().get_QiMen_sign(params, appsecret)
    params['sign'] = sign
    trytimes = 5  # 重试的次数
    for i in range(trytimes):
        # print('try ' + str(i) + ' time')
        try:
            time.sleep(1)
            res = requests.get(url, params=params, headers=headers, timeout=5)
            # 注意此处也可能是302等状态码
            # print(res.status_code)
            if res.status_code == 200:
                wb_data = res.json()
                return wb_data
                break
        except Exception as e:
            print(e)
            print('cannot get the response from WDT Original Orders API, the erros are as above')
    # 如果尝试五次后，依然不能获得res.status=200，抛出异常
    raise Exception('Failed to get response from WDT Original Orders API after {} attempts'.format(trytimes))
def i_days_ago_date(i):
    beijing_time = datetime.datetime.now()
    oneday = datetime.timedelta(days=i)
    yesterday_datetime = beijing_time - oneday
    yesterday = yesterday_datetime.strftime("%Y-%m-%d")
    return yesterday

if __name__ == '__main__':
    try:
        print("starting script....")
        print("\n---------- Starting script at {} ----------------".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        hours_List = [('00:00:00', '01:00:00'), ('01:00:00', '02:00:00'), ('02:00:00', '03:00:00'),
                      ('03:00:00', '04:00:00'), ('04:00:00', '05:00:00'),
                      ('05:00:00', '06:00:00'), ('06:00:00', '07:00:00'), ('07:00:00', '08:00:00'),
                      ('08:00:00', '09:00:00'), ('09:00:00', '10:00:00'),
                      ('10:00:00', '11:00:00'), ('11:00:00', '12:00:00'), ('12:00:00', '13:00:00'),
                      ('13:00:00', '14:00:00'), ('14:00:00', '15:00:00'),
                      ('15:00:00', '16:00:00'), ('16:00:00', '17:00:00'), ('17:00:00', '18:00:00'),
                      ('18:00:00', '19:00:00'), ('19:00:00', '20:00:00'),
                      ('20:00:00', '21:00:00'), ('21:00:00', '22:00:00'), ('22:00:00', '23:00:00'),
                      ('23:00:00', '23:59:59')]
        # hours_List = [('00:00:00', '12:00:00'),  ('12:00:00', '23:59:59')]
        sid_List = ["gfdz2"]
        for sid in sid_List:
            table_name = "WDT_order_management"
            print("采集卖家账号为{}的信息".format(sid))
            db_baidu = DataBase_baidu()
            userAgent = RandomUserAgent()

            account_info = db_baidu.get_account_info(sid)
            if account_info:
                sid, appkey, target_app_key, appsecret = account_info
                #### get the time scope
                now_no_strf = datetime.now()
                now = now_no_strf.strftime("%Y-%m-%d %H:%M:%S")
                print("Current Time:  ", now)

                j = 12
                while j > 0:
                    now_no_strf = datetime.now()
                    now = now_no_strf.strftime("%Y-%m-%d %H:%M:%S")
                    startDate = (now_no_strf - timedelta(days=j, hours=0, minutes=0)).strftime("%Y-%m-%d")
                    for hours in hours_List:
                        start_hour = hours[0]
                        end_hour = hours[1]
                        startTime = str(startDate) + ' ' + str(start_hour)
                        endTime = str(startDate) + ' ' + str(end_hour)

                        print("Start Time: ", startTime)
                        print("End Time: ", endTime)
                        print("采集最后修改时间为此时间段的订单管理", startTime, '-', endTime)

                        ## get the total_number of the InventoryMovement in this time period
                        date_int = int(time.time())
                        user_agent = userAgent.get_random_userAgent()
                        ## vip_api_trade_query.php（查询原始订单）
                        url = 'http://hu3cgwt0tc.api.taobao.com/router/qm'
                        params = {"method": "wdt.trade.query",
                                  "app_key": appkey,
                                  "target_app_key": target_app_key,
                                  "sid": sid,
                                  "start_time": startTime,
                                  "end_time": endTime,
                                  "page_size": 100,
                                  "format": 'json',
                                  "v": '2.0',
                                  "sign_method": 'md5'
                                  }
                        i = 0
                        wb_data = get_response_five_times(url, i, appsecret, params, {"user-agent": user_agent})
                        # print(wb_data)
                        if 'errorcode' in str(wb_data):
                            if wb_data['response']['errorcode'] == 0:
                                orders_quantity = wb_data['response']['total_count']
                                ### import all the inventoryMovent into db
                                if orders_quantity == 0:
                                    print('本时间段内没有新的原始订单')
                                elif orders_quantity <= 100 and orders_quantity > 0:
                                    wb_data = wb_data
                                    print("本次时间段共有{}条原始订单，只有一页。采集该页。".format(str(orders_quantity)))
                                    import_into_database(wb_data, table_name, sid, db_baidu)
                                else:
                                    total_page_No = math.ceil(float(orders_quantity) / float(100))
                                    print("本次时间段共有{}条原始订单，共{}页，数据从第0页开始".format(orders_quantity, total_page_No))
                                    for j in range(total_page_No - 1, -1, -1):
                                        print(f"采集第{j}页的数据")
                                        user_agent1 = userAgent.get_random_userAgent()
                                        headers = {"user-agent": user_agent1}
                                        wb_data = get_response_five_times(url, j, appsecret, params, headers)
                                        import_into_database(wb_data, table_name, sid, db_baidu)
                            else:
                                raise Exception("Error: cannot call WDT Original Orders API successfully. {}".format(wb_data))
                        else:
                            raise Exception("Error: cannot call WDT Original Orders API successfully. {}".format(wb_data))
                    j = j - 1
            else:
                raise Exception("Error: the sid {} doesn't exist.".format(sid))
        print("script Finish ....")
        print("\n---------- Ending script at {} ----------------".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    except Exception as e:
        mailgunner = MailGunner()
        mailgunner.send_WDT_script_error_alert(sid, "original_orders.py", str(e))
        print('报错信息{}'.format(e))



