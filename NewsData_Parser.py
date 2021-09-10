import os
import json
import time
from datetime import datetime
from random import sample

NEWS_DIRECTORY = './dataset/adressa/content_refine'
BASE_DATA = ['20170101', '20170102', '20170103', '20170104', '20170105']
TRAIN_DATA = ['20170106']
TEST_DATA = ['20170107']
NEWS = dict()
NEWS_TRAIN = list()
NEWS_TEST = list()
USER = dict()
USER_MAPPING = list()

def save_news_data():
    index = 1
    cnt = 0
    dir_list = os.listdir(NEWS_DIRECTORY)
    for news in dir_list:

        length = len(dir_list)
        if index % 1 == 0:
            print(f'[save news] {index} / {length}')

        file = open(f'{NEWS_DIRECTORY}/{news}', 'r')
        body = json.loads(file.read())

        try:
            for b in body['fields']:
                if b['field'] == 'body':
                    news_body = ' '.join(b['value']).replace('\n', ' ').replace('Saken oppdateres.', '')
                if b['field'] == 'title':
                    title = b['value']
                if b['field'] == 'url':
                    url = b['value']
        except KeyError:
            continue

        if 'url' not in locals() or 'title' not in locals() or 'news_body' not in locals():
            continue

        if 'ece' not in url and 'html' not in url:
            continue

        NEWS[news] = (index, title, news_body)
        index += 1


def save_user(user_id: str, timestamp: str, news_index: int, news_id: str, active_time: int):
    try:
        user = USER[user_id]
    except KeyError:
        user = []
        USER_MAPPING.append(user_id)

    user.append([timestamp, news_index, news_id, active_time])
    USER[user_id] = user


def parse_time(timestamp: str) -> str:
    return datetime.fromtimestamp(float(timestamp)).strftime('%m/%d/%Y %I:%M:%S %p')


def load_base_data():
    for file_name in BASE_DATA:
        count = 0

        file = open(f'./dataset/adressa/one_week/{file_name}', 'r')
        while True:
            if count % 1000 == 0:
                print(f'[load data] base set {file_name} / {count} loaded.')
            count += 1

            line = file.readline()
            if not line:
                break

            data = json.loads(line)
            try:
                news_id: str = data['id']
            except KeyError:
                continue

            try:
                news_index = NEWS[news_id][0]
            except KeyError:
                continue

            timestamp: str = parse_time(data['time'])

            try:
                active_time: int = int(data['activeTime'])
            except KeyError:
                active_time = 0

            save_user(data['userId'], timestamp, news_index, news_id, active_time)
        file.close()

        print(f'[load data] base set {file_name} done.')


def load_train_data():
    train_data = dict()
    for file_name in TRAIN_DATA:
        file = open(f'./dataset/adressa/one_week/{file_name}', 'r')

        count = 0

        while True:
            if count % 1000 == 0:
                print(f'[load data] train set {file_name} / {count} loaded.')
            count += 1

            line = file.readline()
            if not line:
                break

            data = json.loads(line)
            try:
                news_id: str = data['id']
            except KeyError:
                continue

            try:
                news_index = NEWS[news_id][0]
            except KeyError:
                continue

            timestamp: str = parse_time(data['time'])

            try:
                active_time: int = int(data['activeTime'])
            except KeyError:
                active_time = 0

            user_id = data['userId']

            try:
                temp = train_data[user_id]
            except KeyError:
                temp = []

            temp.append([timestamp, news_index, news_id, active_time])
            train_data[user_id] = temp

        file.close()
    return train_data


def load_test_data():
    test_data = dict()
    for file_name in TEST_DATA:
        file = open(f'./dataset/adressa/one_week/{file_name}', 'r')

        count = 0

        while True:
            if count % 1000 == 0:
                print(f'[load data] test set {file_name} / {count} loaded.')
            count += 1

            line = file.readline()
            if not line:
                break

            data = json.loads(line)
            try:
                news_id: str = data['id']
            except KeyError:
                continue

            try:
                news_index = NEWS[news_id][0]
            except KeyError:
                continue

            timestamp: str = parse_time(data['time'])

            try:
                active_time: int = int(data['activeTime'])
            except KeyError:
                active_time = 0

            user_id = data['userId']

            try:
                temp = test_data[user_id]
            except KeyError:
                temp = []

            temp.append([timestamp, news_index, news_id, active_time])
            test_data[user_id] = temp

        file.close()
    return test_data


def get_unread_news(history: list, data: list) -> list:
    random_news = sample(range(1, len(NEWS) + 1), 150)    # 150

    for info in history:
        if info[1] in random_news:
            random_news.remove(info[1])

    if data[1] in random_news:
        random_news.remove(data[1])

    return sample(random_news, 100)   # 100


def formatting(user_id: int, history: list, data: list, index: int, env: str) -> str:
    timestamp = data[0]
    unread_news: list = get_unread_news(history, data)
    impress_data = f'N{data[1]}-1'

    history_data = ''
    history_active_time = list()
    for info in history:
        history_data += f'N{info[1]} '
        history_active_time.append(info[3])
    history_data = history_data.rstrip()

    for news in unread_news:
        impress_data += f' N{news}-0'

    active_time = ' '.join(map(str, history_active_time))

    if env == 'train':
        for info in history:
            NEWS_TRAIN.append(info[2])
            NEWS_TEST.append(info[2])

        for info in unread_news:
            NEWS_TRAIN.append(info)
            NEWS_TEST.append(info)

        NEWS_TRAIN.append(data[2])
        NEWS_TEST.append(data[2])

    else:
        for info in history:
            NEWS_TEST.append(info[2])

        for info in unread_news:
            NEWS_TEST.append(info)

        NEWS_TEST.append(data[2])

    return f'{index}\tU{USER_MAPPING.index(user_id)}\t{timestamp}\t{history_data}\t{impress_data}\t{active_time}\t{data[3]}\n'


def save_info_train(train_data, test_data):
    train_result = open('./dataset/behaviors.tsv', 'w', encoding='utf-8')

    index = 1
    test_data_keys = list(test_data.keys())
    for train_user_id, train_user_data in train_data.items():
        if train_user_id not in test_data_keys:
            continue

        try:
            history = USER[train_user_id]
        except KeyError:
            continue

        for data in train_user_data:
            train_result.write(formatting(train_user_id, history, data, index, 'train'))
            index += 1

            if index % 1000 == 0:
                print(index)

    train_result.close()


def load_info_test(train_data):
    count = 0

    for data_key, data_value in train_data.items():
        if count % 1000 == 0:
            print(f'load train data to user dict {count}')
        count += 1

        for data in data_value:
            save_user(data_key, data[0], data[1], data[2], data[3])


def save_info_test(train_data, test_data):
    test_result = open('./dataset/behaviors_test.tsv', 'w', encoding='utf-8')

    index = 1
    train_data_keys = list(train_data.keys())
    for test_user_id, test_user_data in test_data.items():
        if test_user_id not in train_data_keys:
            continue

        try:
            history = USER[test_user_id]
        except KeyError:
            continue

        for data in test_user_data:
            test_result.write(formatting(test_user_id, history, data, index, 'test'))
            index += 1
            if index % 1000 == 0:
                print(index)

    test_result.close()


def save_news_train():
    news_train = set(NEWS_TRAIN)
    f = open('./dataset/news.tsv', 'w', encoding='utf-8')

    for key in NEWS.keys():
        if NEWS[key][0] in news_train:
            f.write(f'N{NEWS[key][0]}\t{NEWS[key][1]}\t{NEWS[key][2]}\n')

    f.close()


def save_news_test():
    news_test = set(NEWS_TEST)
    f = open('./dataset/news_test.tsv', 'w', encoding='utf-8')

    for key in NEWS.keys():
        if NEWS[key][0] in news_test:
            f.write(f'N{NEWS[key][0]}\t{NEWS[key][1]}\t{NEWS[key][2]}\n')

    f.close()


def main():
    save_news_data()
    load_base_data()
    train_data = load_train_data()
    test_data = load_test_data()
    save_info_train(train_data, test_data)
    load_info_test(train_data)
    save_info_test(train_data, test_data)
    save_news_train()
    save_news_test()


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(end - start)