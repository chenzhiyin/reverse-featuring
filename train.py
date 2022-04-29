import argparse
import os
from random import random
import time
import uuid
from sdv.tabular import ctgan, GaussianCopula, TVAE

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


def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_location',
                        help='Full path of train data',
                        required=False,
                        default='./data')

    parser.add_argument('--fraction',
                        help='sample data ratio',
                        type=float,
                        default=0.001)

    parser.add_argument('--chunk_size',
                        help='sample data ratio',
                        type=int,
                        default=100000)

    parser.add_argument('--sample',
                        help='sample dataset to train model',
                        action='store_true')

    return parser


def sample_data(index, data, info):
    if 'user_id' not in info.keys() or info['user_id'] != data['user_id']:
        info['is_selected'] = args.fraction > random()
        info['user_id'] = data['user_id']

    return info['is_selected']


def generate_sample_file(file):
    sample_file = './data/sample/' + \
        str(file).split('/')[-1].split(".")[-2]+"_sample.csv"
    info = dict()
    total = 0

    reader = pd.read_csv(file, index_col=False, names=COLUMNS,
                         iterator=True, chunksize=args.chunk_size, header=None)
    for chunk in reader:
        sample_list = list()
        for index, row in chunk.iterrows():
            if sample_data(index, row, info):
                sample_list.append(index % args.chunk_size)

        if len(sample_list) > 0:
            total += len(sample_list)
            chunk.iloc[sample_list].to_csv(
                sample_file, header=0, index=0, mode='a')

        local_time = time.localtime(time.time())
        now = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        print("-----th%d chunk, time:%s, sample total:%d-----" %
              ((index+1)/args.chunk_size, now, total))

    return


def sample():
    files = os.listdir(args.data_location)
    for file in files:
        if not os.path.isdir(file) and file.endswith(".zip"):
            generate_sample_file(args.data_location + '/' + file)


def train():
    data_path = args.data_location+'/'+'sample'
    files = os.listdir(data_path)
    for file_name in files:
        file = data_path + '/' + file_name
        data = pd.read_csv(file, index_col=False, names=COLUMNS, header=None)
        
        # model = ctgan.CTGAN(
        #     field_names=COLUMNS,
        #     embedding_dim=16, 
        #     generator_dim=(32, 32), 
        #     discriminator_dim=(32, 32), 
        #     discriminator_steps=5, 
        #     batch_size=100, 
        #     epochs=1, 
        #     cuda=False)
        
        model = GaussianCopula(field_names=COLUMNS)
        
        model.fit(data)
        print("finish to fit")
        
        model.save(file_name.split('.')[-2]+'.pkl')
        print("finish to save model")

    return


def main():
    if args.sample:
        sample()

    train()


if __name__ == '__main__':
    parser = get_arg_parser()
    args = parser.parse_args()
    main()
