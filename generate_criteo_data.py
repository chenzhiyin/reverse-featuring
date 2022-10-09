import argparse
import math
import os
import time
import uuid
from torch import long
import wget

import pandas as pd

# Definition of some constants
CONTINUOUS_COLUMNS = ['I' + str(i) for i in range(1, 14)]  # 1-13 inclusive
CATEGORICAL_COLUMNS = ['C' + str(i) for i in range(1, 27)]  # 1-26 inclusive
LABEL_COLUMN = ['clicked']
COLUMNS = LABEL_COLUMN + CONTINUOUS_COLUMNS + CATEGORICAL_COLUMNS
USER_COLUMNS = ['I' + str(i) for i in range(1, 8)] + ['C' + str(i) for i in range(1, 14)] + LABEL_COLUMN
ITEM_COLUMNS = ['I' + str(i) for i in range(8, 14)] + ['C' + str(i) for i in range(14, 27)] + LABEL_COLUMN
    
current_size = 0
is_terminated = False

log_files_map = {
    'user_log_info_list': "/user/user_service_log",
    'item_log_info_list': "/item/item_service_log"
}

log_files_suffix_map = {
    'user_log_info_list': 0,
    'item_log_info_list': 0
}

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_location',
                        help='Path of initial data',
                        required=False,
                        default='./data/criteo')
    
    parser.add_argument('--result_location',
                        help='Path of result data',
                        required=False,
                        default='./data/criteo/logs')
    
    parser.add_argument('--chunk_size',
                        help='chunk size to write logs',
                        type=int,
                        default=1000)
    
    parser.add_argument('--total_size',
                        help='total size of data(GB)',
                        type=int,
                        default=10)
    
    parser.add_argument('--rotate_size',
                        help='file size to rotate logs',
                        type=int,
                        default=1024*1024*1)

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
    log_info = f"{info['times']}|INFO|{info['request_id']}|user_info_service"
    log_info = convert_data_logs(data, log_info, USER_COLUMNS)

    append_log_info(info, 'user_log_info_list', log_info)

    return


def convert_item_logs(data, info):
    log_info = f"{info['times']}|INFO|{info['request_id']}|item_info_service"
    log_info = convert_data_logs(data, log_info, ITEM_COLUMNS)

    append_log_info(info, 'item_log_info_list', log_info)

    return


def convert_data(index, data, info):
    # for values in chunk.
    local_time = time.localtime()
    info['times'] = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    request_key = str(info['times']) + str(index)
    info['request_id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, request_key).hex)

    convert_item_logs(data, info)
    convert_user_logs(data, info)

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
    item_list = ['user', 'item']
    for item in item_list:
        os.makedirs(args.result_location + '/' + item + '/', exist_ok=True)
               
    # for index in range (23):        
    #     file = args.data_location + '/day_%d.gz' % (index)
        
    #     if not os.path.isfile(file):
    #         print("download file: %s" % file)
    #         url = "http://tfsmoke1.cn-hangzhou.oss.aliyun-inc.com/data/criteo/day_%d.gz" % (index)
    #         wget.download(url, out=args.data_location)
        
    #     generate_log_file(file)
    #     print("generate data size: %s M" % str(current_size))
        
    #     if is_terminated:
    #         break
            
                
    files = os.listdir(args.data_location)
    files.sort()
    
    for file in files:
        if not os.path.isdir(file) and file.endswith(".csv"):
            generate_log_file(args.data_location + '/' + file)


if __name__ == '__main__':
    parser = get_arg_parser()
    args = parser.parse_args()
    main()
