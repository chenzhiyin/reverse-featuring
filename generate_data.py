import argparse
import math
import os
import time
import uuid
from torch import long
import wget

import pandas as pd

# Definition of some constants
USER_COLUMNS = ['user_id', 'gender', 'visit_city', 'avg_price',
                'is_supervip', 'ctr_30', 'ord_30', 'total_amt_30']
ITEM_COLUMNS = ['shop_id', 'item_id', 'city_id', 'district_id', 'shop_atoi_id', 'shop_geohash6',
                'shop_geohash12', 'brand_id', 'c_1_id', 'merge_standard_food_id', 'rnk_7', 'rnk_30', 'rnk_90']
BEHAVIOR_CLOUMNS = ['shop_id_list', 'item_id_list', 'c_1_id_list', 'merge_standard_food_id_list', 'brand_id_list',
                    'price_list', 'shop_aoi_id_list', 'shop_geohash6_list', 'timediff_list', 'hours_list',
                    'time_type_list', 'weekdays_list']
REQUEST_CLOUMNS = ['times', 'hours', 'time_type', 'weekdays', 'geohash12']
LABEL_COLUMN = ['clicked']
COLUMNS = LABEL_COLUMN + USER_COLUMNS + \
    ITEM_COLUMNS + BEHAVIOR_CLOUMNS + REQUEST_CLOUMNS
    
current_size = 0
is_terminated = False

log_files_map = {
    'user_log_info_list': "/user/user_service_log",
    'item_log_info_list': "/item/item_service_log",
    'behavior_log_info_list': "/behavior/behavior_service_log",
    'request_log_info_list': "/request/request_service_log"
}

log_files_suffix_map = {
    'user_log_info_list': 0,
    'item_log_info_list': 0,
    'behavior_log_info_list': 0,
    'request_log_info_list': 0
}

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_location',
                        help='Path of initial data',
                        required=False,
                        default='./data')
    
    parser.add_argument('--result_location',
                        help='Path of result data',
                        required=False,
                        default='./data/logs')
    
    parser.add_argument('--chunk_size',
                        help='chunk size to write logs',
                        type=int,
                        default=1000)
    
    parser.add_argument('--total_size',
                        help='total size of data(GB)',
                        type=int,
                        default=100)
    
    parser.add_argument('--rotate_size',
                        help='file size to rotate logs',
                        type=int,
                        default=1024*1024*128)

    return parser


def output_log_files(info):
    global current_size, is_terminated
    
    total_file_size = 0
    for key in log_files_map.keys():
        file = args.result_location + log_files_map[key]
        if os.path.isfile(file):
            total_file_size += os.path.getsize(file)/1024/1024
    
    index = int(math.floor(current_size/1024))
    next = int(math.floor((current_size + total_file_size)/1024))
    is_terminated = (next >= args.total_size)
    
    is_rotate = (next > index)
    
    for key in log_files_map.keys():
        file = args.result_location + log_files_map[key]
        
        f = open(file, 'a', encoding='utf-8')
        f.writelines(info[key])
        f.close()
        info[key] = list()

        if os.path.getsize(file) > args.rotate_size or is_rotate or is_terminated:
            current_size += os.path.getsize(file)/1024/1024
            os.rename(file, "%s_%d_%d" % (file, index, log_files_suffix_map[key]))
            log_files_suffix_map[key] += 1

    if is_rotate:
        for key in log_files_suffix_map.keys():
            log_files_suffix_map[key] = 0
            
    return


def append_log_info(data, key, log_info):
    if key not in data.keys():
        data[key] = list()

    data[key].append(log_info + '\n')


def convert_data_logs(data, log_info, names):
    for column in names:
        value = "" if pd.isna(data[column]) else str(data[column])
        log_info += '|' + value

    return log_info


def convert_user_logs(data, info):
    log_info = f"{info['times']}|INFO|{info['batch_id']}|user_info_service"
    log_info = convert_data_logs(data, log_info, USER_COLUMNS)

    append_log_info(info, 'user_log_info_list', log_info)

    return


def convert_item_logs(data, info):
    log_info = f"{info['times']}|INFO|{info['request_id']}|{info['batch_id']}|item_info_service"
    log_info = convert_data_logs(data, log_info, ITEM_COLUMNS)

    append_log_info(info, 'item_log_info_list', log_info)

    return


def covert_behavior_logs(data, info):
    log_info = f"{info['times']}|INFO|{info['batch_id']}|behavior_info_service"
    log_info = convert_data_logs(data, log_info, BEHAVIOR_CLOUMNS)

    append_log_info(info, 'behavior_log_info_list', log_info)

    return


def convert_request_logs(data, info):
    log_info = f"{info['times']}|INFO|{info['request_id']}|{info['batch_id']}|recommend_service|{data['clicked']}"
    log_info = convert_data_logs(data, log_info, REQUEST_CLOUMNS)

    append_log_info(info, 'request_log_info_list', log_info)

    return


def convert_data(index, data, info):
    # for values in chunk.
    local_time = time.localtime(int(data['times']))
    info['times'] = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    batch_key = str(data['times']) + str(data['geohash12']) + str(data['user_id'])
    request_key = batch_key + str(data['item_id']) + str(index)
    info['request_id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, request_key).hex)

    if 'user_id' not in info.keys() or info['user_id'] != data['user_id']:
        info['user_id'] = str(data['user_id'])
        info['batch_id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, batch_key).hex)
        covert_behavior_logs(data, info)
        convert_user_logs(data, info)

    convert_item_logs(data, info)
    convert_request_logs(data, info)

    return


def generate_log_file(file):
    info = dict()

    reader = pd.read_csv(file, index_col=False, names=COLUMNS,
                         iterator=True, chunksize=args.chunk_size, header=None)
    for chunk in reader:
        for index, row in chunk.iterrows():
            convert_data(index, row, info)
        
        output_log_files(info)
        if is_terminated:
            break
        
        local_time = time.localtime(time.time())
        now = time.strftime("%Y-%m-%d_%H:%M:%S", local_time)
        if (index + 1) / args.chunk_size % 100 == 1:
            print("-----th%d chunk, time:%s, total:%d-----" %
                ((index+1)/args.chunk_size, now, index+1))    

    print("finish file: %s" % file)
    return


def main():
    item_list = ['user', 'item', 'behavior', 'request']
    for item in item_list:
        os.makedirs(args.result_location + '/' + item + '/', exist_ok=True)
        
    for day in range(1, 9):
        if is_terminated:
            break
        
        date = "2022040" + str(day)
        for index in range (10):
            file = args.data_location + '/eleme_data_%s_%d.csv' % (date, index)
            
            if not os.path.isfile(file):
                print("download file: %s" % file)
                url = "http://tfsmoke1.cn-hangzhou.oss.aliyun-inc.com/eleme_data/%s/eleme_data_%s_%d.csv" % (date, date, index)
                wget.download(url, out=args.data_location)
            
            generate_log_file(file)
            print("generate data size: %s M" % str(current_size))
            
            if is_terminated:
                break
            
                
    # files = os.listdir(args.data_location)
    # files.sort()
    
    # for file in files:
    #     if not os.path.isdir(file) and file.endswith(".zip"):
    #         generate_log_file(args.data_location + '/' + file)


if __name__ == '__main__':
    parser = get_arg_parser()
    args = parser.parse_args()
    main()
