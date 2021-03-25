import time
import csv

from collections import Counter
from functools import reduce

from multiprocessing import Pool

def read(path):
    with open(path, newline='') as f:
        reader = csv.reader(f, delimiter=';')
        data = list(reader)
    # Retorna apenas valores menos o header
    return data[1:]

# mapper - Cria um dicionário (Counter) com cada cidade e o número de seus vacinados
def count(rows):
    key_map = Counter()
    for row in rows:
        key = row[18]
        if key in key_map:
            key_map[key] += 1
        else:
            key_map[key] = 1
    return key_map

# reducer - Retornar a junção dos dicionários
def merge(A, B):
    return A + B

# divide em partes iguais ou quase iguais
def split(data, parts):
    return (data[i::parts] for i in range(parts))

if __name__ == '__main__':
    start = time.time()
    
    num_splits = 4
    
    # Leitura
    path = './ac.csv'
    data = read(path)
    
    # Divisão
    data_chunks = list(split(data, num_splits))
    
    # Definições
    mapper = count
    reducer = merge
    
    with Pool(num_splits) as pool:
        mapped = pool.map(mapper, data_chunks)
        
    reduced = reduce(reducer, mapped)
    
    print('\nNúmero de vacinados por muncípio:\n')
    for key in reduced:
        print(f'{key}: {reduced[key]}')
    print(f'\nTotal: {len(data)}')
    
    end = time.time()
    print(f'\nTempo total de execução: {end - start}\n')
    