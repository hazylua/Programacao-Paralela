import time
import csv

from collections import Counter
from functools import reduce

def read(path):
    with open(path, newline='') as f:
        reader = csv.reader(f, delimiter=';')
        data = list(reader)
    # Retorna apenas valores menos o header
    return data[1:]

# mapper
def count(rows):
    key_map = Counter()
    for row in rows:
        key = row[18]
        if key in key_map:
            key_map[key] += 1
        else:
            key_map[key] = 1
    return key_map

# reducer
def merge(A, B):
    return A + B

def split(data, parts):
    return (data[i::parts] for i in range(parts))

def run():
    num_splits = 4
    
    # Leitura
    path = './mg.csv'
    data = read(path)
    
    # Divisão
    data_chunks = list(split(data, num_splits))
    
    # Processamento
    mapper = count
    reducer = merge
    
    temp = Counter()
    for chunk in data_chunks:
        key_map = count(chunk)
        temp = merge(temp, key_map)
        
    reduced = temp
    
    print('\nNúmero de vacinados por muncípio:\n')
    for key in reduced:
        print(f'{key}: {reduced[key]}')
    print(f'\nTotal: {len(data)}')
    
if __name__ == '__main__':
    start = time.time()
    run()
    end = time.time()
    print(f'\nTempo total de execução: {end - start}\n')
    