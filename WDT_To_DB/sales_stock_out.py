import sys
import requests
# import simplejson
import hashlib
from datetime import datetime, timedelta
import time
import random
import pymysql
import math
# sys.path.append("../..")
# hostname = socket.gethostname()
# print("Host name: ", hostname)
import os
import re

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from public.basic_function import DataBase_baidu, RandomUserAgent, MailGunner, PlatformSign
from public.basic_function import warehouse_no_List
import traceback


def extract_order_number(remark):
    # 先判断字符串中是否包含“售后返修换新”
    if "售后返修换新" not in remark:
        return None
    match = re.search(r"原订单号:(\d+)", remark)
    if match:
        return match.group(1)  # 返回匹配的数字序列
    else:
        return None



# # 判断京东的n次换货情况

def import_into_database(wb_data, table_name_trade_stock_out, table_name_trade_stock_out_detail, sid, db_baidu):
    conn = db_baidu.conn
    cur = db_baidu.cur

    sql1 = "select stockout_id, sid from %s" % (table_name_trade_stock_out)
    cur.execute(sql1)
    conn.commit()
    res1 = cur.fetchall()

    sql2 = "select detail_rec_id, sid from %s" % (table_name_trade_stock_out_detail)
    cur.execute(sql2)
    conn.commit()
    res2 = cur.fetchall()

    sql_insert_stock_out_detail = f"""
        insert into {table_name_trade_stock_out_detail} (
            stockout_id, order_no, src_order_no, warehouse_no, consign_time, order_type, trade_status, order_type_name, trade_type, trade_type_name, subtype, goods_count, goods_total_amount, receivable, paid, refund_status, warehouse_name, created, remark, outer_no, trade_no, src_trade_no, trade_time, pay_time, status, status_CN, shop_name, shop_no, post_amount, modified, platform_id, trade_id, stockout_no, wms_status, warehouse_id, consign_status, discount, src_tids, tax, tax_rate, currency, stock_check_time, detail_rec_id, detail_stockout_id, detail_spec_no, detail_sell_price, detail_goods_count, detail_brand_no, detail_brand_name, detail_goods_type, detail_gift_type, detail_goods_name, detail_goods_no, detail_spec_name, detail_spec_code, detail_suite_no, detail_cost_price, detail_total_amount, detail_goods_id, detail_spec_id, detail_paid, detail_refund_status, detail_market_price, detail_discount, detail_share_amount, detail_tax_rate, detail_barcode, detail_sale_order_id, detail_share_post, detail_src_oid, detail_src_tid, detail_modified, detail_platform_id, trade_status_name, sid,invoice_type
            ) values (
            %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    sql_update_stock_out_detail = f"""
        update {table_name_trade_stock_out_detail} 
        set stockout_id=%s, order_no=%s, src_order_no=%s, warehouse_no=%s, consign_time=%s, order_type=%s, trade_status=%s, order_type_name=%s, trade_type=%s, trade_type_name=%s, subtype=%s, goods_count=%s, goods_total_amount=%s, receivable=%s, paid=%s, refund_status=%s, warehouse_name=%s, created=%s, remark=%s, outer_no=%s, trade_no=%s, src_trade_no=%s, trade_time=%s, pay_time=%s, `status`=%s, status_CN=%s, shop_name=%s, shop_no=%s, post_amount=%s, modified=%s, platform_id=%s, trade_id=%s, stockout_no=%s, wms_status=%s, warehouse_id=%s, consign_status=%s, discount=%s, src_tids=%s, tax=%s, tax_rate=%s, currency=%s, stock_check_time=%s, detail_stockout_id=%s, detail_spec_no=%s, detail_sell_price=%s, detail_goods_count=%s, detail_brand_no=%s, detail_brand_name=%s, detail_goods_type=%s, detail_gift_type=%s, detail_goods_name=%s, detail_goods_no=%s, detail_spec_name=%s, detail_spec_code=%s, detail_suite_no=%s, detail_cost_price=%s, detail_total_amount=%s, detail_goods_id=%s, detail_spec_id=%s, detail_paid=%s, detail_refund_status=%s, detail_market_price=%s, detail_discount=%s, detail_share_amount=%s, detail_tax_rate=%s, detail_barcode=%s, detail_sale_order_id=%s, detail_share_post=%s, detail_src_oid=%s, detail_src_tid=%s, detail_modified=%s, detail_platform_id=%s, trade_status_name=%s ,invoice_type = %s
        where detail_rec_id=%s and sid=%s
    """

    sql_insert_stock_out = f"""
        insert into {table_name_trade_stock_out} (
        stockout_id, order_no, src_order_no, warehouse_no, consign_time, order_type, trade_status, order_type_name, trade_type, trade_type_name, subtype, goods_count, goods_total_amount, receivable, paid, refund_status, warehouse_name, created, remark, outer_no, trade_no, src_trade_no, trade_time, pay_time, status, status_CN, shop_name, shop_no, post_amount, modified, platform_id, trade_id, stockout_no, wms_status, warehouse_id, consign_status, discount, src_tids, tax, tax_rate, currency, stock_check_time, trade_status_name, sid,invoice_type,cs_remark
        ) values (
        %s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    sql_update_stock_out = f"""
        update {table_name_trade_stock_out} 
        set order_no=(%s), src_order_no=(%s), warehouse_no=(%s), consign_time=(%s), order_type=(%s), trade_status=(%s), order_type_name=(%s), trade_type=(%s), trade_type_name=(%s), subtype=(%s), goods_count=(%s), goods_total_amount=(%s), receivable=(%s), paid=(%s), refund_status=(%s), warehouse_name=(%s), created=(%s), remark=(%s), outer_no=(%s), trade_no=(%s), src_trade_no=(%s), trade_time=(%s), pay_time=(%s), status=(%s), status_CN=(%s), shop_name=(%s), shop_no=(%s), post_amount=(%s), modified=(%s), platform_id=(%s), trade_id=(%s), stockout_no=(%s), wms_status=(%s), warehouse_id=(%s), consign_status=(%s), discount=(%s), src_tids=(%s), tax=(%s), tax_rate=(%s), currency=(%s), stock_check_time=(%s), trade_status_name=(%s) ,invoice_type=(%s),cs_remark=(%s)
        where stockout_id=%s and sid=%s
    """

    # 设置每隔多少条SQL语句提交一次事务
    commit_interval = 500
    commit_counter = 0

    stockout_list = wb_data['response']['stockout_list']
    for stock_out_single in stockout_list:
        stockout_id = stock_out_single['stockout_id']
        ###必须转成str，因为数据库里这个字段是str类型，要去重的话，必须字段保持一个类型
        stockout_id = str(stockout_id)
        order_no = stock_out_single['order_no']
        src_order_no = stock_out_single['src_order_no']
        warehouse_no = stock_out_single['warehouse_no']
        warehouse_no = str(warehouse_no)
        consign_time = stock_out_single['consign_time']
        order_type = stock_out_single['order_type']
        order_type_name = stock_out_single['order_type_name']
        status = stock_out_single['status']
        if status >= 95:
            status = str(status)
            if status == '95':
                status_CN = '已发货'
            elif status == '105':
                status_CN = '部分打款'
            elif status == '110':
                status_CN = '已完成'
            elif status == '113':
                status_CN = '异常发货'
            elif status == '115':
                status_CN = '回传失败'
            elif status == '120':
                status_CN = '回传成功'
            else:
                status_CN = '未知状态'
            trade_status = stock_out_single['trade_status']
            trade_status = str(trade_status)
            if trade_status == '95':
                trade_status_name = '已发货'
            elif trade_status == '105':
                trade_status_name = '部分打款'
            elif trade_status == '110':
                trade_status_name = '已完成'
            elif trade_status == '113':
                trade_status_name = '异常发货'
            else:
                trade_status_name = '未知状态'
            trade_type = stock_out_single['trade_type']
            trade_type = str(trade_type)
            if trade_type == '1':
                trade_type_name = '网店销售'
            elif trade_type == '2':
                trade_type_name = '线下零售'
            elif trade_type == '3':
                trade_type_name = '售后换货'
            elif trade_type == '4':
                trade_type_name = '批发业务'
            elif trade_type == '5':
                trade_type_name = '保修换新'
            elif trade_type == '6':
                trade_type_name = '保修完成'
            elif trade_type == '7':
                trade_type_name = '订单补发'
            elif trade_type == '8':
                trade_type_name = '供销补发'
            elif trade_type == '101':
                trade_type_name = '达人样品'
            elif trade_type == '102':
                trade_type_name = '寄样'
            else:
                trade_type_name = '未知类型'
            subtype = stock_out_single['subtype']
            goods_count = stock_out_single['goods_count']
            goods_total_amount = stock_out_single['goods_total_amount']
            receivable = stock_out_single['receivable']
            paid = stock_out_single['paid']
            refund_status = stock_out_single['refund_status']
            warehouse_name = stock_out_single['warehouse_name']
            created = stock_out_single['created']
            remark = stock_out_single['remark']
            outer_no = stock_out_single['outer_no']
            trade_no = stock_out_single['trade_no']
            src_trade_no = stock_out_single['src_trade_no']
            trade_time = stock_out_single['trade_time']
            pay_time = stock_out_single['pay_time']

            shop_name = stock_out_single['shop_name']
            shop_no = stock_out_single['shop_no']
            post_amount = stock_out_single['post_amount']
            modified = stock_out_single['modified']
            platform_id = stock_out_single['platform_id']
            trade_id = stock_out_single['trade_id']
            stockout_no = stock_out_single['stockout_no']
            wms_status = stock_out_single['wms_status']
            warehouse_id = stock_out_single['warehouse_id']
            consign_status = stock_out_single['consign_status']
            discount = stock_out_single['discount']
            src_tids = stock_out_single['src_tids']
            tax = stock_out_single['tax']
            tax_rate = stock_out_single['tax_rate']
            currency = stock_out_single['currency']
            stock_check_time = stock_out_single['stock_check_time']
            invoice_type = stock_out_single["invoice_type"]
            cs_remark = stock_out_single['cs_remark']
            details_list = stock_out_single['details_list']
            time.sleep(1)

            for details_single in details_list:
                detail_rec_id = details_single['rec_id']
                ###必须转成str，因为数据库里这个字段是str类型，要去重的话，必须字段保持一个类型
                detail_rec_id = str(detail_rec_id)
                detail_stockout_id = details_single['stockout_id']
                detail_spec_no = details_single['spec_no']
                detail_sell_price = details_single['sell_price']
                detail_goods_count = details_single['goods_count']
                detail_brand_no = details_single['brand_no']
                detail_brand_name = details_single['brand_name']
                detail_goods_type = details_single['goods_type']
                detail_gift_type = details_single['gift_type']
                detail_goods_name = details_single['goods_name']
                detail_goods_no = details_single['goods_no']
                detail_spec_name = details_single['spec_name']
                detail_spec_code = details_single['spec_code']
                detail_suite_no = details_single['suite_no']
                detail_cost_price = details_single['cost_price']
                detail_total_amount = details_single['total_amount']
                detail_goods_id = details_single['goods_id']
                detail_spec_id = details_single['spec_id']
                detail_paid = details_single['paid']
                detail_refund_status = details_single['refund_status']
                detail_market_price = details_single['market_price']
                detail_discount = details_single['discount']
                detail_share_amount = details_single['share_amount']
                detail_tax_rate = details_single['tax_rate']
                detail_barcode = details_single['barcode']
                detail_sale_order_id = details_single['sale_order_id']
                detail_share_post = details_single['share_post']
                detail_src_oid = details_single['src_oid']
                detail_src_tid = details_single['src_tid']
                detail_modified = details_single['modified']
                detail_platform_id = details_single['platform_id']
                detail_platform_id = str(detail_platform_id)
                if warehouse_no in warehouse_no_List:
                    if (str(stockout_id), str(sid)) in [(d[0], d[1]) for d in res1]:
                        values1 = (stockout_id, order_no, src_order_no, warehouse_no,
                                   consign_time, order_type, trade_status, order_type_name, trade_type, trade_type_name, subtype,
                                   goods_count, goods_total_amount, receivable, paid, refund_status, warehouse_name, created,
                                   remark, outer_no, trade_no, src_trade_no, trade_time, pay_time, status, status_CN, shop_name,
                                   shop_no, post_amount, modified, platform_id, trade_id, stockout_no, wms_status, warehouse_id,
                                   consign_status, discount, src_tids, tax, tax_rate, currency, stock_check_time,
                                   detail_stockout_id, detail_spec_no, detail_sell_price, detail_goods_count, detail_brand_no,
                                   detail_brand_name, detail_goods_type, detail_gift_type, detail_goods_name, detail_goods_no,
                                   detail_spec_name, detail_spec_code, detail_suite_no, detail_cost_price, detail_total_amount,
                                   detail_goods_id, detail_spec_id, detail_paid, detail_refund_status, detail_market_price,
                                   detail_discount, detail_share_amount, detail_tax_rate, detail_barcode, detail_sale_order_id,
                                   detail_share_post, detail_src_oid, detail_src_tid, detail_modified, detail_platform_id,
                                   trade_status_name, invoice_type, detail_rec_id, sid)
                        cur.execute(sql_update_stock_out_detail, values1)
                        commit_counter += 1
                        if commit_counter % commit_interval == 0:
                            conn.commit()
                            commit_counter = 0  # 重置计数器
                    else:
                        values2 = (stockout_id, order_no, src_order_no, warehouse_no, consign_time, order_type, trade_status, order_type_name, trade_type, trade_type_name, subtype, goods_count, goods_total_amount, receivable, paid, refund_status, warehouse_name, created, remark, outer_no, trade_no,
                                   src_trade_no, trade_time, pay_time, status, status_CN, shop_name, shop_no, post_amount, modified, platform_id, trade_id, stockout_no, wms_status, warehouse_id, consign_status, discount, src_tids, tax, tax_rate, currency, stock_check_time,
                                   detail_rec_id, detail_stockout_id, detail_spec_no, detail_sell_price, detail_goods_count, detail_brand_no, detail_brand_name, detail_goods_type, detail_gift_type, detail_goods_name, detail_goods_no, detail_spec_name, detail_spec_code, detail_suite_no,
                                   detail_cost_price, detail_total_amount, detail_goods_id, detail_spec_id, detail_paid, detail_refund_status, detail_market_price, detail_discount, detail_share_amount, detail_tax_rate, detail_barcode, detail_sale_order_id, detail_share_post, detail_src_oid,
                                   detail_src_tid, detail_modified, detail_platform_id, trade_status_name, sid, invoice_type)
                        cur.execute(sql_insert_stock_out_detail, values2)
                        commit_counter += 1
                        if commit_counter % commit_interval == 0:
                            conn.commit()
                            commit_counter = 0  # 重置计数器
            if warehouse_no in warehouse_no_List:
                if (str(detail_rec_id), str(sid)) in [(d[0], d[1]) for d in res2]:
                    values3 = (order_no, src_order_no, warehouse_no, consign_time, order_type,
                               trade_status, order_type_name, trade_type, trade_type_name, subtype, goods_count,
                               goods_total_amount, receivable, paid, refund_status, warehouse_name, created, remark, outer_no,
                               trade_no, src_trade_no, trade_time, pay_time, status, status_CN, shop_name, shop_no, post_amount,
                               modified, platform_id, trade_id, stockout_no, wms_status, warehouse_id, consign_status, discount,
                               src_tids, tax, tax_rate, currency, stock_check_time, trade_status_name, invoice_type, cs_remark, stockout_id, sid)
                    cur.execute(sql_update_stock_out, values3)
                    commit_counter += 1
                    if commit_counter % commit_interval == 0:
                        conn.commit()
                        commit_counter = 0  # 重置计数器
                else:
                    values4 = (
                        stockout_id, order_no, src_order_no, warehouse_no, consign_time, order_type, trade_status, order_type_name, trade_type, trade_type_name, subtype, goods_count, goods_total_amount, receivable, paid, refund_status, warehouse_name, created, remark, outer_no, trade_no,
                        src_trade_no,
                        trade_time, pay_time, status, status_CN, shop_name, shop_no, post_amount, modified, platform_id, trade_id, stockout_no, wms_status, warehouse_id, consign_status, discount, src_tids, tax, tax_rate, currency, stock_check_time, trade_status_name, sid, invoice_type,cs_remark)
                    cur.execute(sql_insert_stock_out, values4)
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
            # print(res.status_code)
            # 注意此处也可能是302等状态码
            if res.status_code == 200:
                wb_data = res.json()
                return wb_data
                break
        except Exception as e:
            print('cannot get the response from WDT Sales Stock Out API, the erros are as above')
    # 如果尝试五次后，依然不能获得res.status=200，抛出异常
    raise Exception('Failed to get response from WDT Original Orders API after {} attempts'.format(trytimes))



if __name__ == '__main__':
    # db_baidu = DataBase_baidu()
    # conn = db_baidu.conn
    # cur = db_baidu.cur
    # # print(update_exchange_history(cur, "gfdz2", "168153421186945261-1"))
    # print(jingdong_exchange_number(cur, '295615778717', 'gstar2', '295513776594'))
    try:
        print("starting script....")
        print("\n---------- Starting script at {} ----------------".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        sid_List = ["gfdz2"]
        for sid in sid_List:
            table_name_trade_stock_out = "Crayola_WDT_sales_stock_out"
            table_name_trade_stock_out_detail = "Crayola_WDT_sales_stock_out_detail"
            print("采集卖家账号为{}的信息".format(sid))
            db_baidu = DataBase_baidu()
            userAgent = RandomUserAgent()

            account_info = db_baidu.get_account_info(sid)
            if account_info:
                sid, appkey, target_app_key, appsecret = account_info
                #### get the time scope
                now_no_strf = datetime.now()
                now = now_no_strf.strftime("%Y-%m-%d %H:%M:%S")
                print("Current Time:  ", now)  # ['2022-11-01 00:00:00', '2022-11-08 00:00:00'], ['2022-11-08 00:00:00', '2022-11-15 00:00:00'],['2022-11-15 00:00:00', '2022-11-30 00:00:00'],['2022-11-30 00:00:00', '2022-12-25 00:00:00'],
                # lis = [
                # ['2022-12-25 00:00:00', '2023-01-20 00:00:00'], ['2023-01-20 00:00:00', '2023-02-15 00:00:00'], ['2023-02-15 00:00:00', '2023-03-10 00:00:00'], ['2023-03-10 00:00:00', '2023-03-23 00:00:00'],['2023-03-23 00:00:00', '2023-04-21 00:00:00'], ['2023-04-21 00:00:00', '2023-05-20 00:00:00'], ['2023-05-20 00:00:00', '2023-06-18 00:00:00'], ['2023-06-18 00:00:00', '2023-07-16 00:00:00'], ['2023-07-16 00:00:00', '2023-08-14 00:00:00'], ['2023-08-14 00:00:00', '2023-09-11 00:00:00'], ['2023-09-11 00:00:00', '2023-10-09 00:00:00'], ['2023-10-09 00:00:00', '2023-11-06 00:00:00'], ['2023-11-06 00:00:00', '2023-12-03 00:00:00'], ['2023-12-03 00:00:00', '2024-01-01 00:00:00'],
                # ['2024-01-01 00:00:00', '2024-01-29 00:00:00'], ['2024-01-29 00:00:00', '2024-02-25 00:00:00'], ['2024-02-25 00:00:00', '2024-03-24 00:00:00'], ['2024-03-24 00:00:00', '2024-04-22 00:00:00'], ['2024-04-22 00:00:00', '2024-05-20 00:00:00']]
                ## ['2024-06-12 00:00:00', '2024-06-15 00:00:00'], ['2024-06-15 00:00:00', '2024-07-10 05:54:31'],
                ## ['2024-07-01 00:00:00', '2024-07-02 00:00:00']]
                endTime = now
                startTime = (now_no_strf - timedelta(days=2, hours=0, minutes=0)).strftime("%Y-%m-%d %H:%M:%S")
                # startTime = (now_no_strf - timedelta(days=29, hours=0, minutes=0)).strftime("%Y-%m-%d %H:%M:%S")
                time.sleep(5)
                print("Start Time: ", startTime)
                print("End Time: ", endTime)
                print("采集最后修改时间为此时间段的销售出库单", startTime, '-', endTime)

                ## get the total_number of the InventoryMovement in this time period

                # date_params = time.time()
                user_agent = userAgent.get_random_userAgent()

                # date_int = int(time.time())
                ##stockout_order_query_trade.php（查询销售出库单）

                url = 'http://hu3cgwt0tc.api.taobao.com/router/qm'
                # is_by_modified这里传入1，是按照最后修改时间来查询的
                params = {"method": "wdt.stockout.order.query.trade",
                          "app_key": appkey,
                          "target_app_key": target_app_key,
                          "sid": sid,
                          "start_time": startTime,
                          "end_time": endTime,
                          "page_size": 100,
                          "format": 'json',
                          "v": '2.0',
                          "sign_method": 'md5',
                          "is_by_modified": 1
                          }
                i = 0
                wb_data = get_response_five_times(url, i, appsecret, params, {"user-agent": user_agent})
                if 'errorcode' in str(wb_data):
                    if wb_data['response']['errorcode'] == 0:
                        stock_out_quantity = wb_data['response']['total_count']
                        ## import all the inventoryMovent into db
                        if stock_out_quantity == 0:
                            print('本时间段内没有新的销售出库单')
                        elif stock_out_quantity <= 100 and stock_out_quantity > 0:
                            wb_data = wb_data
                            print("本次时间段共有{}条销售出库单，只有一页。采集该页。".format(str(stock_out_quantity)))
                            import_into_database(wb_data, table_name_trade_stock_out, table_name_trade_stock_out_detail, sid, db_baidu)

                        else:
                            total_page_No = math.ceil(float(stock_out_quantity) / float(100))
                            print("本次时间段共有{}条销售出库单，共{}页，数据从第0页开始".format(stock_out_quantity, total_page_No))
                            for j in range(total_page_No - 1, -1, -1):
                                print(f"采集第{j}页的数据")
                                user_agent1 = userAgent.get_random_userAgent()
                                headers = {"user-agent": user_agent1}
                                wb_data = get_response_five_times(url, j, appsecret, params, headers)
                                import_into_database(wb_data, table_name_trade_stock_out, table_name_trade_stock_out_detail, sid, db_baidu)
                    else:
                        raise Exception("Error: cannot call WDT Sales Stock Out API successfully. {}".format(wb_data))
                else:
                    raise Exception("Error: cannot call WDT Sales Stock Out API successfully. {}".format(wb_data))
            else:
                raise Exception("Error: the sid {} doesn't exist.".format(sid))
        print("script Finish ....")
        print("\n---------- Ending script at {} ----------------".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    except Exception as e:
        mailgunner = MailGunner()
        mailgunner.send_WDT_script_error_alert(sid, "sales_stock_out", str(e))
