import pandas as pd
import numpy as np
import pymysql
from public import basic_function
from datetime import timedelta
import datetime


# 销售出库单
def sales_summay(db_baidu, stock_out_detail, start_date, end_date, warehouse_name):
    sql1 = f'''SELECT
                    CASE
                        WHEN GROUPING(
                            IFNULL(CASE
                                WHEN TRIM(s.trade_type_name) IN ('线下零售', '订单补发') THEN '网店销售'
                                ELSE TRIM(s.trade_type_name)
                            END, '汇总')
                        ) = 1 THEN '汇总'
                        ELSE IFNULL(CASE
                                WHEN TRIM(s.trade_type_name) IN ('线下零售', '订单补发') THEN '网店销售'
                                ELSE TRIM(s.trade_type_name)
                            END, '汇总')
                    END AS trade_type_name,
                    SUM(s.detail_goods_count) AS total_goods_count,
                    ROUND(SUM(s.detail_total_amount), 2) AS total_amount,
                    ROUND(SUM(s.detail_goods_count * i.cost_price), 2) AS total_cost_price,
                    ROUND(SUM(s.detail_goods_count * i.cost_price)/SUM(s.detail_total_amount), 2) AS cost_price_div_total_amount,
                    ROUND(SUM(s.detail_total_amount)-SUM(s.detail_goods_count * i.cost_price), 2) AS profit
                FROM
                    {stock_out_detail} s
                LEFT JOIN (
                    SELECT spec_no, MAX(cost_price) AS cost_price
                    FROM WDT_Inventory_management
                    GROUP BY spec_no
                ) i
                ON
                    s.detail_spec_no = i.spec_no
                WHERE DATE(s.consign_time) BETWEEN '{start_date}' AND '{end_date}' AND s.status_CN IN ('已完成','已发货') AND s.warehouse_name IN ({', '.join([f"'{name}'" for name in warehouse_name])})
                GROUP BY
                IFNULL(CASE
                        WHEN TRIM(s.trade_type_name) IN ('线下零售', '订单补发') THEN '网店销售'
                        ELSE TRIM(s.trade_type_name)
                    END, '汇总')
                WITH ROLLUP;
    '''
    print(1111, sql1)
    # 执行查询
    db_baidu.execute_query(sql1,)
    sales_summay_table = db_baidu.fetchall()

    return sales_summay_table


def sales_summay_online(db_baidu, stock_out_detail, warehouse_name, trade_type_name, start_date, end_date):
    # conn = db_baidu.conn
    # cur = db_baidu.cur
    # sql1 = f'SELECT shop_name, SUM(detail_goods_count) ,SUM(detail_total_amount)FROM {stock_out_detail} WHERE warehouse_name = %s and trade_type_name = %s GROUP BY shop_name'
    sql1 = f'''SELECT
                IFNULL(s.shop_name, '汇总') AS shop_name,
                SUM(s.detail_goods_count) AS total_goods_count,
                ROUND(SUM(s.detail_total_amount), 2) AS total_amount,
                ROUND(SUM(s.detail_goods_count * i.cost_price), 2) AS total_cost_price,
                ROUND(SUM(s.detail_goods_count * i.cost_price)/SUM(s.detail_total_amount), 2) AS cost_price_div_total_amount,
                ROUND(SUM(s.detail_total_amount)-SUM(s.detail_goods_count * i.cost_price), 2) AS profit
            FROM
                {stock_out_detail} s
            LEFT JOIN (
                SELECT spec_no, MAX(cost_price) AS cost_price
                FROM WDT_Inventory_management
                GROUP BY spec_no
            ) i
            ON
                s.detail_spec_no = i.spec_no
            WHERE
                s.warehouse_name IN ({', '.join([f"'{name}'" for name in warehouse_name])}) and s.trade_type_name IN ({', '.join([f"'{name}'" for name in trade_type_name])}) AND(DATE(s.consign_time) BETWEEN '{start_date}' AND '{end_date}') AND s.status_CN IN ('已完成','已发货')
            GROUP BY
                s.shop_name WITH ROLLUP
            ORDER BY total_goods_count;

    '''
    print(222, sql1)
    db_baidu.execute_query(sql1,)
    sales_summay_online_table = db_baidu.fetchall()
    # db_baidu.close()
    return sales_summay_online_table


def sales_summay_offline(db_baidu, stock_out_detail, stock_out_table, trade_type_name, start_date, end_date):
    sql1 = f'''SELECT  
                CASE 
                    WHEN NOT GROUPING(CASE 
                        WHEN notd.cs_remark LIKE '%绘画乐%' THEN '线下-绘画乐' 
                        ELSE '线下-功夫' 
                    END) THEN
                        CASE 
                            WHEN notd.cs_remark LIKE '%绘画乐%' THEN '线下-绘画乐' 
                            ELSE '线下-功夫' 
                        END
                    ELSE '汇总'
                END AS group_name,
                SUM(d.detail_goods_count) AS total_goods_count,
                ROUND(SUM(d.detail_total_amount), 2) AS total_amount,
                ROUND(SUM(d.detail_goods_count * i.cost_price), 2) AS total_cost,
                ROUND(SUM(d.detail_goods_count * i.cost_price)/SUM(d.detail_total_amount), 2) AS cost_price_div_total_amount,
                ROUND(SUM(d.detail_total_amount)-SUM(d.detail_goods_count * i.cost_price), 2) AS profit
            FROM 
                {stock_out_detail} d
            LEFT JOIN 
                {stock_out_table} notd ON d.trade_no = notd.trade_no
            LEFT JOIN (
                SELECT spec_no, MAX(cost_price) AS cost_price
                FROM WDT_Inventory_management
                GROUP BY spec_no
            ) i ON d.detail_spec_no = i.spec_no
            WHERE 
                d.trade_type_name in {trade_type_name} AND(DATE(d.consign_time) BETWEEN '{start_date}' AND '{end_date}') AND d.status_CN IN ('已完成','已发货')
            GROUP BY 
                CASE 
                    WHEN notd.cs_remark LIKE '%绘画乐%' THEN '线下-绘画乐' 
                    ELSE '线下-功夫' 
                END WITH ROLLUP;
'''
    print(sql1)
    db_baidu.execute_query(sql1)
    sales_summay_offline_table = db_baidu.fetchall()
    # db_baidu.close()
    return sales_summay_offline_table


def inventory_summary(db_baidu, warehouse_name, inventory_table, stock_out_detail, start_date_week, end_date):
    sql2 = f'''
        SELECT
            IFNULL(inv.class_name, '汇总') AS class_name,
            inv.total_stock_num,
            inv.total_cost,
            sale.total_count,
            sale.total_amount
        FROM
            (
                SELECT
                    CASE 
                        WHEN GROUPING(p.class_name) = 1 THEN '汇总'
                        ELSE IFNULL(p.class_name, '未知分类')
                    END AS class_name,
                    SUM(i.stock_num) AS total_stock_num,
                    ROUND(SUM(i.stock_num * i.cost_price), 2) AS total_cost
                FROM
                    {inventory_table} i
                LEFT JOIN
                    WDT_integration.WDT_Products p
                ON
                    i.spec_no = p.spec_no
                WHERE
                    i.warehouse_name IN ({', '.join([f"'{name}'" for name in warehouse_name])})
                GROUP BY
                    p.class_name WITH ROLLUP
            ) inv
        LEFT JOIN
            (
                SELECT
                    CASE 
                        WHEN GROUPING(p.class_name) = 1 THEN '汇总'
                        ELSE IFNULL(p.class_name, '未知分类')
                    END AS class_name,
                    ROUND(SUM(s.detail_total_amount), 2) AS total_amount,
                    ROUND(SUM(s.detail_goods_count), 2) AS total_count
                FROM
                    {stock_out_detail} s
                LEFT JOIN
                    WDT_integration.WDT_Products p
                ON
                    s.detail_spec_no = p.spec_no
                WHERE 
                    (DATE(s.consign_time) BETWEEN {start_date_week} AND {end_date}) 
                    AND s.status_CN IN ('已完成','已发货')
                GROUP BY
                    p.class_name WITH ROLLUP
            ) sale
        ON
            inv.class_name = sale.class_name
        ORDER BY 
            inv.total_stock_num
        '''
    print(sql2)
    db_baidu.execute_query(sql2,)
    inventory_summary_table = db_baidu.fetchall()
    # db_baidu.close()
    return inventory_summary_table


def sales_detail(db_baidu, sales_detail, trade_type_name, start_date, end_date, warehouse_name):
    sql1 = f'''SELECT
                s.detail_spec_no,
                p.goods_name,
                SUM(s.detail_goods_count) AS total_count,
                ROUND(SUM(s.detail_total_amount), 2) AS total_amount,
                ROUND(SUM(s.detail_goods_count * COALESCE(i.cost_price, 0)), 2) AS total_cost_price,
                COALESCE(i.total_stock_count, 0) AS total_stock_count
            FROM
                {sales_detail} s
            LEFT JOIN 
                (
                    -- 先按 spec_no 进行汇总，确保每个 spec_no 只取唯一记录
                    SELECT 
                        spec_no, 
                        MAX(cost_price) AS cost_price,  -- 选取最大成本价（或者可以用 AVG(cost_price)）
                        SUM(stock_num) AS total_stock_count  -- 按 spec_no 聚合库存
                    FROM 
                        WDT_Inventory_management
                    WHERE 
                        warehouse_name IN ({', '.join([f"'{name}'" for name in warehouse_name])})  -- 只取指定仓库
                    GROUP BY 
                        spec_no
                ) i
            ON s.detail_spec_no = i.spec_no
            LEFT JOIN
                WDT_integration.WDT_Products p
            ON s.detail_spec_no = p.spec_no
            WHERE 
                s.trade_type_name IN ({', '.join([f"'{name}'" for name in trade_type_name])}) 
                AND (DATE(s.consign_time) BETWEEN '{start_date}' AND '{end_date}')  
                AND s.status_CN IN ('已完成', '已发货')
            GROUP BY 
                s.detail_spec_no, p.goods_name, i.total_stock_count, i.cost_price
            ORDER BY 
                total_amount DESC;
            '''
    print(sql1)
    db_baidu.execute_query(sql1,)
    sales_detail_table = db_baidu.fetchall()

    return sales_detail_table


def purchasein_detail(db_baidu, purchase_table, start_date, end_date):
    query = f"""
        SELECT DISTINCT DATE(stockin_time) AS stockin_date
        FROM {purchase_table}
        WHERE DATE(stockin_time) BETWEEN '{start_date}' AND '{end_date}' 
        AND status_CN IN ('已完成', '已发货')
        ORDER BY stockin_date;
    """
    print(query)
    # 获取日期
    db_baidu.execute_query(query)
    dates = [row[0].strftime('%Y-%m-%d') for row in db_baidu.fetchall()]

    # 动态生成列
    if not dates:
        columns = ''
    else:
        columns = ", " + ", ".join(
            [f"SUM(CASE WHEN DATE(stockin_time) = '{date}' THEN detail_total_cost ELSE 0 END) AS `{date}_采购金额`, "
             f"SUM(CASE WHEN DATE(stockin_time) = '{date}' THEN detail_goods_count ELSE 0 END) AS `{date}_采购件数`"
             for date in dates]
        )
    print(dates)

    # 拼接完整的 SQL
    final_sql = f"""
        SELECT 
            detail_goods_no AS 商家编码,
            detail_goods_name AS 货品名称
            {columns}
        FROM 
            {purchase_table}
        WHERE 
            DATE(stockin_time) BETWEEN '{start_date}' AND '{end_date}' 
            AND status_CN = '已完成'
        GROUP BY 
            detail_goods_no, detail_goods_name;
        """
    db_baidu.execute_query(final_sql)

    # 获取列名
    column_names = [desc[0] for desc in db_baidu.description()]

    # 获取结果数据
    purchase_detail_table = db_baidu.fetchall()
    # 将结果转为字典格式
    purchase_detail_table = [dict(zip(column_names, row)) for row in purchase_detail_table]

    # 返回最终结果
    return purchase_detail_table


def To_DF(tumple_data, columns):
    df = pd.DataFrame(tumple_data, columns=columns)
    # df = df.set_index()
    return df


def store_classify(store_name):
    mapping_unit = {
        'Crayola绘儿乐文具旗舰店-抖音': '绘儿乐-抖音',
        'Crayola绘儿乐旗舰店-京东': '绘儿乐-京东',
        '绘儿乐官方旗舰店': '绘儿乐-天猫',
        '绘儿乐旗舰店-拼多多': '绘儿乐-拼多多',
        'Crayola绘儿乐-1688': '绘儿乐-阿里巴巴'
    }
    store_name_new = mapping_unit.get(store_name, store_name)
    return store_name_new


def format_number(x):
    if isinstance(x, (int, float)):  # 仅对数值列进行格式化
        if x == '':
            return x
        elif x is None:
            return ''
        elif x == 'nan':
            return ''
        elif pd.isna(x):
            return ''
        elif x == '0':
            return ''
        elif x == 0:
            return ''
        else:
            return f"{x:,.0f}"
    return x  # 非数值列不处理


def summary_all(purchasein_detail_table, columns):
    cols_to_convert = [col for col in purchasein_detail_table.columns if col not in ['商家编码', '货品名称']]
    purchasein_detail_table[cols_to_convert] = purchasein_detail_table[cols_to_convert].apply(pd.to_numeric, errors='coerce')
    col_sum = purchasein_detail_table.drop(columns=['商家编码', '货品名称']).sum()
    # 创建汇总行，并插入到第一行
    summary_row = pd.Series(['汇总', '汇总'] + col_sum.tolist(), index=purchasein_detail_table.columns)
    purchasein_detail_table = pd.concat([pd.DataFrame([summary_row]), purchasein_detail_table], ignore_index=True)
    purchasein_detail_table[columns] = purchasein_detail_table[columns].applymap(format_number)
    return purchasein_detail_table


if __name__ == '__main__':
    # try:
    db_baidu = basic_function.DataBase_baidu()
    stock_out_detail = 'Crayola_WDT_sales_stock_out_detail'
    stock_out_table = 'Crayola_WDT_sales_stock_out'
    inventory_table = 'WDT_Inventory_management'
    purchase_table = 'Crayola_WDT_purchase_stock_in'

    # print(datetime.date.today() - timedelta(7))

    end_date = (datetime.date.today() - timedelta(0))
    start_date_month = (end_date - timedelta(30))
    start_date_week = (end_date - timedelta(7))
    end_date = end_date.strftime("%Y%m%d")
    start_date_month = start_date_month.strftime("%Y%m%d")
    start_date_week = start_date_week.strftime("%Y%m%d")
    print(f'---------------{start_date_week}-{end_date}-----------------')

    store_name = 'Crayola'
    file_root = '/root/scripts/Sales_Summary_Crayola'
    # file_root = 'C:/Users/11837/Desktop/test'
    file_path = file_root + '/' + f'{start_date_week}-{end_date} Sales performance of Crayola .xlsx'
    email_List = ['leo@kungfudata.com','alice@kungfudata.com', 'michelle@kungfudata.com', 'wendy@kungfudata.com', 'pengyue@kungfudata.com']
    # email_List = ['pengyue@kungfudata.com']
    warehouse_name = ['绘儿乐扬州趣云良品仓', '绘儿乐ELEE上海仓1']
    sales_columns = ['销售类型', '出库件数', '销售金额', '成本价', '成本价/销售金额', '利润']
    change_columns = ['销售类型', '出库件数', '销售金额', '成本价', '利润']
    # # 线上线下汇总
    sales_summay_table = sales_summay(db_baidu, stock_out_detail, start_date_week, end_date, warehouse_name)
    sales_summay_table = To_DF(sales_summay_table, sales_columns)
    sales_summay_table[change_columns] = sales_summay_table[change_columns].applymap(format_number)

    sales_summay_online_table = sales_summay_online(db_baidu, stock_out_detail, warehouse_name, ('网店销售', '线下零售', '订单补发'), start_date_week, end_date)
    sales_summay_online_table = To_DF(sales_summay_online_table, sales_columns)
    sales_summay_online_table[change_columns] = sales_summay_online_table[change_columns].applymap(format_number)
    sales_summay_online_table['销售类型'] = sales_summay_online_table['销售类型'].apply(lambda x: store_classify(x))

    sales_summay_offline_table = sales_summay_offline(db_baidu, stock_out_detail, stock_out_table, ('批发业务', ''), start_date_week, end_date)
    sales_summay_offline_table = To_DF(sales_summay_offline_table, sales_columns)
    sales_summay_offline_table[change_columns] = sales_summay_offline_table[change_columns].applymap(format_number)

    # 库存情况
    inventory_summary_table = inventory_summary(db_baidu, warehouse_name, inventory_table, stock_out_detail, start_date_week, end_date)
    inventory_summary_table = To_DF(inventory_summary_table, columns=['分类', '库存件数', '成本金额(RMB)', '出库件数', '销售金额']).applymap(format_number)
    # print(pd.DataFrame(inventory_summary_table, columns=['分类', '库存量(件)', '成本金额(RMB)']))
    #
    # # 初始化ExcelSaver对象
    excel_saver = basic_function.ExcelSaver(file_path)  # TODO 放在服务器上要改
    excel_saver.delete_crayola_files(file_root)
    #
    # 以下是销售详情 线上销售情况、线下销售情况
    sales_online_detail_table = sales_detail(db_baidu, stock_out_detail, ('网店销售', '线下零售'), start_date_week, end_date, warehouse_name)
    sales_online_detail_table = To_DF(sales_online_detail_table, ['商家编码', '货品名称', '出库件数', '支付金额', '总成本(未税)', 'ERP库存件数'])
    sales_online_detail_table = summary_all(pd.DataFrame(sales_online_detail_table), ['出库件数', '支付金额', '总成本(未税)', 'ERP库存件数'])

    sales_offline_detail_table = sales_detail(db_baidu, stock_out_detail, ('批发业务', ''), start_date_week, end_date, warehouse_name)
    sales_offline_detail_table = To_DF(sales_offline_detail_table, ['商家编码', '货品名称', '出库件数', '支付金额', '总成本(未税)', 'ERP库存件数'])
    sales_offline_detail_table = summary_all(pd.DataFrame(sales_offline_detail_table), ['出库件数', '支付金额', '总成本(未税)', 'ERP库存件数'])

    # 采购详情
    purchasein_detail_table = pd.DataFrame(purchasein_detail(db_baidu, purchase_table, start_date_month, end_date))
    purchasein_detail_table = summary_all(pd.DataFrame(purchasein_detail_table), purchasein_detail_table.columns)

    db_baidu.close()
    # 保存数据框到指定位置
    excel_saver.save_text(f'{start_date_week}-{end_date} 销售情况总概', sheet_name="Summary", start_row=1, start_col=1)
    excel_saver.fill_cell_red(sheet_name="Summary", row=1, col=1)
    # 加粗外边框
    excel_saver.add_bold_border(sheet_name="Summary", start_row=1, start_col=1, end_row=1 + sales_summay_table.shape[0] + 1, end_col=6)
    # 贴入数据
    excel_saver.save_dataframe(sales_summay_table, sheet_name="Summary", start_row=2, start_col=1)  # 从第2行第1列开始

    excel_saver.save_text(f'{start_date_week}-{end_date} 线上渠道销售情况', sheet_name="Summary", start_row=9, start_col=1)
    excel_saver.fill_cell_red(sheet_name="Summary", row=9, col=1)
    excel_saver.add_bold_border(sheet_name="Summary", start_row=9, start_col=1, end_row=9 + sales_summay_online_table.shape[0] + 1, end_col=6)
    excel_saver.save_dataframe(sales_summay_online_table, sheet_name="Summary", start_row=10, start_col=1)  # 从第2行第6列开始

    excel_saver.save_text(f'{start_date_week}-{end_date} 线下渠道销售情况', sheet_name="Summary", start_row=22, start_col=1)
    excel_saver.fill_cell_red(sheet_name="Summary", row=22, col=1)
    excel_saver.add_bold_border(sheet_name="Summary", start_row=22, start_col=1, end_row=22 + sales_summay_offline_table.shape[0] + 1, end_col=6)
    excel_saver.save_dataframe(sales_summay_offline_table, sheet_name="Summary", start_row=23, start_col=1)  # 从第2行第11列开始

    excel_saver.save_text(f'{end_date} 库存情况', sheet_name="Summary", start_row=30, start_col=1)
    excel_saver.fill_cell_red(sheet_name="Summary", row=30, col=1)
    excel_saver.add_bold_border(sheet_name="Summary", start_row=30, start_col=1, end_row=30 + inventory_summary_table.shape[0] + 1, end_col=5)
    excel_saver.save_dataframe(inventory_summary_table, sheet_name="Summary", start_row=31, start_col=1)  # 从第9行第1列开始
    #
    #
    excel_saver.save_dataframe(sales_offline_detail_table, sheet_name="线下销售情况", start_row=1, start_col=1)  # 从第1行第1列开始
    excel_saver.save_dataframe(sales_online_detail_table, sheet_name="线上销售情况", start_row=1, start_col=1)  # 从第1行第1列开始
    print(purchasein_detail_table)
    excel_saver.save_dataframe(purchasein_detail_table, sheet_name="采购情况(金额)", start_row=1, start_col=1)  # 从第1行第1列开始

    # # # 保存Excel文件
    print(file_path)
    excel_saver.save()
    emailsender = basic_function.EmailSender()
    print(store_name, file_path, email_List, start_date_week, end_date)
    emailsender.send_report_to_people(store_name, file_path, email_List, start_date_week, end_date)

    # except Exception as e:
    #     # pass
    #     mailgunner = basic_function.MailGunner()
    #     print('报错信息{}'.format(e))
    #     mailgunner.send_WDT_script_error_alert('gfdz', "crayola_purchase_stock_in.py", str(e))
