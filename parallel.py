import time
import csv

from collections import Counter
from functools import reduce

from multiprocessing import Pool

def read(path):
    with open(path, newline='') as f:
        reader = csv.reader(f, delimiter=';')
        data = list(reader)
    # for row in data:
    #     print(row[30])
    return data

# mapper
def count(rows):
    key_map = Counter()
    for row in rows:
        key = row[9]
        if key in key_map:
            key_map[key] += 1
        else:
            key_map[key] = 1
    return key_map

# reducer
def merge(A, B):
    return A + B

# def check(data_chunks):
#     add = 0
#     for data in data_chunks:
#         key_map = count(data)
#         if key_map['RIO BRANCO']:
#             print(key_map['RIO BRANCO'])
#             add += key_map['RIO BRANCO']
#     print(add)

def split(data, parts):
    return (data[i::parts] for i in range(parts))
    
def run(pool):
    path = './ac.csv'
    
    # Leitura
    data = read(path)
    
    # Divisão
    data_chunks = list(split(data, 8))
    
    # Mapeia e reduz
    mapper = count
    reducer = merge
    
    mapped = pool.map(mapper, data_chunks)
    reduced = reduce(reducer, mapped)
    
    # print(reduced)

if __name__ == '__main__':
    start = time.time()
    with Pool(8) as pool:
        run(pool)
    end = time.time()
    print(f'{end - start}')
    