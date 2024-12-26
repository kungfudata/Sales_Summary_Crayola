def store_classify(store_name):
    mapping_unit = {
        'Crayola绘儿乐文具旗舰店-抖音': '绘儿乐-抖音店',
        'Crayola绘儿乐旗舰店': '绘儿乐-京东店',
        '绘儿乐官方旗舰店': '绘儿乐-天猫店',
        '绘儿乐旗舰店-拼多多': '绘儿乐-拼多多店',
        # 'Crayola绘儿乐-京东自营': '绘儿乐-京东自营'
    }
    store_name_new = mapping_unit.get(store_name)
    return store_name_new

print(store_classify('绘儿乐旗舰店-拼多多'))