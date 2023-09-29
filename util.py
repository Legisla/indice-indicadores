import requests
import hashlib
import os
import json
import csv
from io import StringIO
from urllib.parse import parse_qs, urlparse
from tqdm import tqdm
from time import sleep
import logging
import re

DEBUG = True if logging.getLogger().level == 10 else False


def _regex_get(d, key):
    """Obtém o valor de um dicionário usando uma correspondência de expressão regular na chave."""
    for regex, value in d.items():
        if re.match(regex, key):
            return value
    return None

def _generate_hash(url, params, cache_dir="cache_api"):
    hash_object = hashlib.md5((url + str(params)).encode())
    hash_hex = hash_object.hexdigest()
    cache_path = os.path.join(cache_dir, f"{hash_hex}")
    return cache_path

def cached_requests(url, params=None, cache_dir='cache_api', filetype='json', cache=True, **kwargs):
    # Cria um hash da URL e parâmetros para usar como nome do arquivo de cache
    cache_path = _generate_hash(url, params, cache_dir)
    # Tenta carregar do cache
    if os.path.exists(cache_path) and cache:
        with open(cache_path, 'r', encoding='utf-8') as f:
            if filetype == 'json':
                data = json.load(f)
                return data
            elif filetype == 'csv':
                csv_reader = csv.DictReader(f, delimiter=';')
                return [row for row in csv_reader]

    # Se não estiver em cache, faz a requisição
    response = requests.get(url, params=params, **kwargs)
    if response.status_code in [403, 502]:
        sleep(5)
        logging.debug("Esta demorando um pouquinho...")

        response = requests.get(url, params=params, **kwargs)
    
    # Salva no cache se a requisição for bem-sucedida
    if response.status_code == 200:
        os.makedirs(cache_dir, exist_ok=True)
        with open(cache_path, 'w', encoding='utf-8') as f:
            if filetype == 'json':
                json.dump(response.json(), f, ensure_ascii=False, indent=4)
                return response.json()
            elif filetype == 'csv':
                f.write(response.text)
                csv_reader = csv.DictReader(StringIO(response.text), delimiter=';')
                return [row for row in csv_reader]
    else:
        logging.debug(f"Erro ao acessar API: {response.status_code}")
        return None


def _acessar_api_portaltransparencia(autor, ano, **kwargs):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.google.com/"
        }

        url = "https://portaldatransparencia.gov.br/emendas/consulta/baixar"
        params = {
            "paginacaoSimples": "true",
            "direcaoOrdenacao": "asc",
            "palavraChave": autor,
            "de": ano,
            "ate": ano,
            "colunasSelecionadas": "codigoEmenda,ano,tipoEmenda,autor,numeroEmenda,localidadeDoGasto,funcao,subfuncao,valorEmpenhado,valorLiquidado,valorPago"
        }
        
        data = cached_requests(url, params=params, filetype='csv', headers=headers, **kwargs)
        return data

def _acessar_bulk_camara(variavel, data, formato='json', **kwargs):
    url = f"https://dadosabertos.camara.leg.br/arquivos/{variavel}/{formato}/{variavel}-{data[0:4]}.{formato}"

    data = cached_requests(url, params={}, filetype=formato, **kwargs)
    return(data)

def _acessar_api_camara(endpoint, params=None, use_params=True, **kwargs):
    url_base = "https://dadosabertos.camara.leg.br/api/v2"
    all_data = []
    
    # Requisição inicial para obter o total de páginas
    url = f"{url_base}/{endpoint}"
    initial_data = cached_requests(url, params=params, filetype='json', headers={"accept": "application/json"}, **kwargs)

    total_pages = 1
    if initial_data:
        for link in initial_data['links']:
            if link['rel'] == 'last':
                query_string = urlparse(link['href']).query
                parsed_query_string = parse_qs(query_string)
                total_pages = int(parsed_query_string['pagina'][0])
                break
    
        if total_pages==1:
            return initial_data['dados']
            
    # Barra de progresso e loop de requisições
    loop = tqdm(range(1, total_pages + 1), desc=f"Fetching endpoint {endpoint}") if DEBUG else range(1, total_pages + 1)
    for current_page in loop:
        url = f"{url_base}/{endpoint}"
        final_params = params if use_params else None
        data = cached_requests(url, params=final_params, filetype='json', headers={"accept": "application/json"}, **kwargs)
        
        if data:
            all_data.extend(data['dados'])
            
            # Verifica o próximo endpoint
            endpoint = None
            for link in data['links']:
                if link['rel'] == 'next':
                    endpoint = link['href'].replace(url_base + '/', '')
                    use_params = False
                    break
                    
    return all_data
