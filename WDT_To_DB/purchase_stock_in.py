#-*- coding:utf-8 -*-
import requests
# import simplejson
import hashlib
import random
import pymysql
from datetime import datetime, timedelta
import time
import math
import sys
sys.path.append("../..")
import socket
# hostname = socket.gethostname()
# print("Host name: ", hostname)
# if hostname == "instance-9njygsiq":
#     sys.path.append('/root/scripts/WDT_InventoryMovement')
# else:
#     sys.path.append('../../')
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from public.basic_function import DataBase_baidu, RandomUserAgent, MailGunner, PlatformSign
from public.basic_function import warehouse_no_List


def import_into_database(wb_data, table_name, sid, db_baidu):
    conn = db_baidu.conn
    cur = db_baidu.cur
    sql1 = "select DISTINCT stockin_id, sid from %s" % (table_name)
    # print(sql1)
    cur.execute(sql1)
    conn.commit()
    res1 = cur.fetchall()

    sql_insert = f"""
                    INSERT INTO {table_name} (
                        stockin_id, order_no, status, warehouse_no, warehouse_name,
                        stockin_time, check_time, created_time, src_order_no, remark,
                        stockin_reason, order_type, order_type_name, goods_amount,
                        total_price, discount, tax_amount, post_fee, other_fee,
                        logistics_type, logistics_code, logistics_name, logistics_no,
                        purchase_no, provider_no, provider_name, outer_no, goods_count,
                        right_nums, right_price, stockin_no, warehouse_id, modified,
                        status_CN, detail_rec_id, detail_spec_no, detail_goods_count,
                        detail_cost_price, detail_src_price, detail_tax_price,
                        detail_tax_amount, detail_tax, detail_total_cost, detail_remark,
                        detail_right_num, detail_right_price, detail_right_cost,
                        detail_brand_no, detail_brand_name, detail_goods_name,
                        detail_goods_no, sid
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """

    stockin_list = wb_data['response']['stockin_list']

    for stock_in_single in stockin_list:
        sid = sid
        stockin_id = str(stock_in_single['stockin_id'])  #入库单主键
        if (stockin_id, sid) not in res1:
            ###必须转成str，因为数据库里这个字段是str类型，要去重的话，必须字段保持一个类型
            stockin_id = str(stockin_id)
            order_no = stock_in_single['order_no']   # 入库单号
            status = stock_in_single['status']  #状态
            warehouse_no = stock_in_single['warehouse_no']   # 仓库编号
            warehouse_name = stock_in_single['warehouse_name']   # 仓库
            stockin_time = stock_in_single['stockin_time']
            check_time = stock_in_single['check_time']
            created_time = stock_in_single['created_time']
            src_order_no = stock_in_single['src_order_no']
            remark = stock_in_single['remark']
            stockin_reason = stock_in_single['stockin_reason']
            order_type = stock_in_single['order_type']
            order_type_name = stock_in_single['order_type_name']
            goods_amount = stock_in_single['goods_amount']
            total_price =stock_in_single['total_price']
            discount = stock_in_single['discount']
            tax_amount = stock_in_single['tax_amount']
            post_fee = stock_in_single['post_fee']
            other_fee = stock_in_single['other_fee']
            logistics_type = stock_in_single['logistics_type']
            logistics_code = stock_in_single['logistics_code']
            logistics_name = stock_in_single['logistics_name']
            logistics_no = stock_in_single['logistics_no']
            purchase_no = stock_in_single['purchase_no']
            provider_no = stock_in_single['provider_no']
            provider_name = stock_in_single['provider_name']
            outer_no = stock_in_single['outer_no']
            goods_count = stock_in_single['goods_count']
            right_nums = stock_in_single['right_nums']
            right_price = stock_in_single['right_price']
            stockin_no = stock_in_single['stockin_no']
            warehouse_id = stock_in_single['warehouse_id']
            modified = stock_in_single['modified']
            details_list = stock_in_single['details_list']
            if status == '80':
                status_CN = '已完成'
                for detail_single in details_list:
                    detail_rec_id = detail_single['rec_id']  # 入库单明细主键
                    detail_spec_no = detail_single['spec_no']
                    detail_goods_count = detail_single['goods_count']
                    detail_cost_price = detail_single['cost_price']
                    detail_src_price = detail_single['src_price']
                    detail_tax_price = detail_single['tax_price']
                    detail_tax_amount = detail_single['tax_amount']
                    detail_tax = detail_single['tax']
                    if detail_tax == 0:
                        detail_tax = 0.13
                    detail_total_cost = detail_single['total_cost']
                    detail_remark = detail_single['remark']
                    detail_right_num = detail_single['right_num']
                    detail_right_price = detail_single['right_price']
                    detail_right_cost = detail_single['right_cost']
                    detail_brand_no = detail_single['brand_no']
                    detail_brand_name = detail_single['brand_name']
                    detail_goods_name = detail_single['goods_name']
                    detail_goods_no = detail_single['goods_no']
                    ###必须转成str，因为数据库里这个字段是str类型，要去重的话，必须字段保持一个类型
                    detail_rec_id = str(detail_rec_id)
                    if warehouse_no in warehouse_no_List:
                        values = (
                            stockin_id, order_no, status, warehouse_no, warehouse_name,
                            stockin_time, check_time, created_time, src_order_no, remark,
                            stockin_reason, order_type, order_type_name, goods_amount,
                            total_price, discount, tax_amount, post_fee, other_fee,
                            logistics_type, logistics_code, logistics_name, logistics_no,
                            purchase_no, provider_no, provider_name, outer_no, goods_count,
                            right_nums, right_price, stockin_no, warehouse_id, modified,
                            status_CN, detail_rec_id, detail_spec_no, detail_goods_count,
                            detail_cost_price, detail_src_price, detail_tax_price,
                            detail_tax_amount, detail_tax, detail_total_cost, detail_remark,
                            detail_right_num, detail_right_price, detail_right_cost,
                            detail_brand_no, detail_brand_name, detail_goods_name,
                            detail_goods_no, sid
                        )
                        cur.execute(sql_insert, values)

                        # Commit every 1000 rows
                        if cur.rowcount % 1000 == 0:
                            conn.commit()
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
    # print(params)
    trytimes = 5  # 重试的次数
    for i in range(trytimes):
        # print('try ' + str(i) + ' time')
        try:
            time.sleep(1)
            res = requests.get(url, params=params, headers=headers, timeout=5)
            # print(res.status_code)
            if res.status_code == 200:
                wb_data = res.json()
                # 注意此处也可能是302等状态码
                return wb_data
                break
        except Exception as e:
            print(e)
            print('cannot get the response from WDT Purchase Stock In API, the erros are as above')
    # 如果尝试五次后，依然不能获得res.status=200，抛出异常
    raise Exception('Failed to get response from WDT Original Orders API after {} attempts'.format(trytimes))



###  只导入已完成状态的采购入库单
if __name__ == '__main__':
    try:
        print("starting script....")
        print("\n---------- Starting script at {} ----------------".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        sid_List = ["gfdz2"]
        for sid in sid_List:
            table_name = 'Crayola_WDT_purchase_stock_in'
            print("采集卖家账号为{}的采购入库单信息".format(sid))
            db_baidu = DataBase_baidu()
            userAgent = RandomUserAgent()

            account_info = db_baidu.get_account_info(sid)
            if account_info:
                sid, appkey, target_app_key, appsecret = account_info
                #### get the time scope
                now_no_strf = datetime.now()
                now = now_no_strf.strftime("%Y-%m-%d %H:%M:%S")
                print("Current Time:  ", now)

                endTime = now
                startTime = (now_no_strf - timedelta(days=2, hours=0, minutes=0)).strftime("%Y-%m-%d %H:%M:%S")
                print("Start Time: ", startTime)
                print("End Time: ", endTime)


                print("采集最后修改时间为此时间段的采购入库单", startTime, '-', endTime)

                ## get the total_number of the InventoryMovement in this time period

                date_int = int(time.time())
                user_agent = userAgent.get_random_userAgent()
                ##stockout_order_query_trade.php（查询采购入库单）
                url = 'http://hu3cgwt0tc.api.taobao.com/router/qm'
                params = {
                          "method": "wdt.stockin.order.query.purchase",
                          "sid": sid,
                          "app_key": appkey,
                          "target_app_key": target_app_key,
                          "timestamp": date_int,
                          "start_time": startTime,
                          "end_time": endTime,
                          "page_size": 50,
                          "status": 80,
                          "format": 'json',
                          "v": '2.0',
                          "sign_method": 'md5'
                          }
                # print(params)
                i = 0
                wb_data = get_response_five_times(url,i, appsecret, params,{"user-agent": user_agent})
                if 'errorcode' in str(wb_data):
                    if wb_data['response']['errorcode'] == 0:
                        stock_in_quantity = wb_data['response']['total_count']
                        ### import all the inventoryMovent into db
                        if stock_in_quantity == 0:
                            print('本时间段内没有新的采购入库单')
                        elif stock_in_quantity <= 50 and stock_in_quantity > 0:
                            wb_data = wb_data
                            print('本次时间段共有{}条采购入库单，只有一页。采集该页。'.format(str(stock_in_quantity)))
                            import_into_database(wb_data, table_name, sid, db_baidu)
                        else:
                            total_page_No = math.ceil(float(stock_in_quantity) / float(50))
                            print("本次时间段共有{}条采购入库单，共{}页，数据从第0页开始".format(stock_in_quantity, total_page_No))
                            # 从total_page_No-1开始采集，每次采集后，页数减1，一直采集到第0页结束
                            for j in range(total_page_No - 1, -1, -1):
                                print(f"采集第{j}页的数据")
                                user_agent1 = userAgent.get_random_userAgent()
                                headers = {"user-agent": user_agent1}
                                wb_data = get_response_five_times(url, j, appsecret, params, headers)
                                import_into_database(wb_data, table_name, sid, db_baidu)
                    else:
                        raise Exception("Error: cannot call WDT Purchase Stock In API successfully. {}".format(wb_data))
                else:
                    raise Exception("Error: cannot call WDT Purchase Stock In API successfully. {}".format(wb_data))
            else:
                raise Exception("Error: the sid {} doesn't exist.".format(sid))
        print("script Finish ....")
        print("\n---------- Ending script at {} ----------------".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        print("\n")
    except Exception as e:
        mailgunner = MailGunner()
        mailgunner.send_WDT_script_error_alert(sid, "crayola_purchase_stock_in.py", str(e))
        print('报错信息{}'.format(e))




