import numpy as np
from statistics import mean
import csv, pickle, os
import shutil, argparse
from datetime import datetime
from processa import Parlamentar
from util import _acessar_api_camara
from tqdm import tqdm
import logging
import gc

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    handlers=[logging.FileHandler('indicadores.log'),
                              logging.StreamHandler()])
DEBUG = True if logging.getLogger().level == 10 else False
TODAY = datetime.today().strftime('%Y-%m-%d')

class Legislatura:
    def __init__(self, name, dataInicio, dataFim):
        self.dataInicio = dataInicio
        self.dataFim = dataFim
        self.lista_de_nomes = self._lista()
        self.parlamentares = []
        self.name = name
        self.data = []

    def _lista(self):
        """Obtém a lista de deputados da API da Câmara. Se a data final não for a data atual, busca os deputados ativos durante o intervalo especificado entre 'dataInicio' e 'dataFim'."""

        #BUG Endpoint /deputados esta retornando vazio ao invés dos deputados em exercício
        endpoint = "deputados/"
               
        params = {
            "dataInicio" : self.dataFim,
            "dataFim" : self.dataFim
            }
        
        data = _acessar_api_camara(endpoint, params, cache=False)
        
        return data

    def _get_parlamentar_data(self, p):
        """Cria uma instância da classe 'Parlamentar' para cada deputado e executa a coleta e processamento de dados relacionados a ele. Cada parlamentar é processado individualmente, e seus dados são armazenados para análise posterior."""

        parlamentar = Parlamentar(p, self.dataInicio, self.dataFim)
        parlamentar.run()
        return parlamentar

    def _to_dict(self):
        for parlamentar in self.parlamentares:
            parlamentar_data = {"Deputado": parlamentar.nome, "external_id" : parlamentar.pid}
            for var, value_data in parlamentar.variaveis.items():
                parlamentar_data[var] = value_data['value']
            self.data.append(parlamentar_data)

    def _save_to_file(self):
        keys = self.data[0].keys()
        with open(f'{self.name}.csv', 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.data)

    def _calculate_indicators(self):
        """Realiza o cálculo dos indicadores para cada parlamentar. Este método percorre cada variável calculada e aplica a função '_calculate_indicator' para obter uma pontuação normalizada baseada na distribuição dos dados."""

        initial_keys = list(self.data[0].keys())  # Copia as chaves para uma lista
        for var_name in initial_keys:
            if 'variavel' in var_name:
                col_data = [d[var_name] for d in self.data]
                col_data = list(map(float, col_data))  # Converting to float
                score = self._calculate_indicator(col_data)
                for i, d in enumerate(self.data):
                    d[var_name + "_score"] = score[i]

    def _calculate_outliers(self, column):
        Q1 = np.percentile(column, 25)
        Q3 = np.percentile(column, 75)
        IQR = Q3 - Q1
        lower_limit = Q1 - 1.5 * IQR
        upper_limit = Q3 + 1.5 * IQR
        return [x for x in column if lower_limit <= x <= upper_limit]

    def _calculate_indicator(self, column):
        """Aplica o método de detecção de outliers e a regra de Sturges para categorizar os dados em classes. Os dados são primeiramente filtrados para remover outliers, e então são divididos em classes usando a fórmula de Sturges, que é baseada na contagem de dados não nulos."""

        filtered_data = self._calculate_outliers(column)
        zero_indices = [i for i, x in enumerate(column) if x == 0]
        count = len([x for x in filtered_data if x > 0])
        num_classes = int(np.ceil(1 + 3.332 * np.log10(count))) if count > 0 else 0

        if num_classes <= 0:
            return [0 if x in filtered_data else 1 for x in column]
        
        min_value = min(filtered_data)
        max_value = max(filtered_data)
        bins = np.linspace(min_value, max_value, num_classes)
        bins = np.insert(bins, 0, -np.inf)
        bins[-1] = np.inf
        classes = np.digitize(column, bins, right=True)
        posicao_classe = classes / num_classes

        return [round(x, 2) if i not in zero_indices else 0 for i, x in enumerate(posicao_classe)]

    def _calculate_score_final(self):
        """Calcula a pontuação final para cada parlamentar. Este método agrega as pontuações de todas as variáveis calculadas para cada parlamentar e calcula a média, resultando em um 'score_final' que representa o desempenho geral do parlamentar."""

        for d in self.data:
            score_columns = [key for key in d.keys() if key.endswith("_score")]
            scores = [d[col] for col in score_columns]
            d["score_final"] = round(mean(scores), 2)

    def _calculate_stars(self):
        """Atribui uma classificação de estrelas aos parlamentares com base em seu 'score_final'. Este método divide o intervalo de pontuação máxima em cinco partes iguais e atribui de 1 a 5 estrelas aos parlamentares, dependendo de onde seu 'score_final' se enquadra neste intervalo."""

        col_data = [d["score_final"] for d in self.data]
        filtered_data = self._calculate_outliers(col_data)
        max_score = max(filtered_data)
        interval = max_score / 5

        for d in self.data:
            score = d["score_final"]
            if score <= interval:
                d["stars"] = 1
            elif score <= 2 * interval:
                d["stars"] = 2
            elif score <= 3 * interval:
                d["stars"] = 3
            elif score <= 4 * interval:
                d["stars"] = 4
            else:
                d["stars"] = 5


    def run(self):
        """Executa o processo completo de geração do índice. Inclui a coleta de dados dos parlamentares, o processamento e cálculo de indicadores, a geração de pontuações finais e a saída dos dados em um arquivo CSV. Também gerencia o cache de dados dos parlamentares para otimizar o processo."""

        pickle_file = f'{self.name}.pickle'
        if os.path.isfile(pickle_file):
             with open(pickle_file, 'rb') as f:
                self.parlamentares = pickle.load(f)
        else:
            self.parlamentares = []
        
        for p in tqdm(self.lista_de_nomes, desc="Carregando deputados em exercício..."):
            if p['id'] not in [p.pid for p in self.parlamentares]:
                _parlamentar = self._get_parlamentar_data(p)
                self.parlamentares.append(_parlamentar)
                with open(pickle_file, 'wb') as f:
                    pickle.dump(self.parlamentares, f)
                gc.collect()

        self._to_dict()
        self._calculate_indicators()
        self._calculate_score_final()
        self._calculate_stars()
        self._save_to_file()


def main():
    """Função principal para executar o script. Permite configurar parâmetros como a necessidade de atualizar o cache, remover dados de cache, definir o nome da legislatura e especificar o intervalo de datas. Inicia o processo de geração do índice com base nos parâmetros fornecidos."""

    parser = argparse.ArgumentParser(description="Script para rodar uma Legislatura.")
    parser.add_argument("--refresh", help="Força o refresh do arquivo .pickle", action="store_true")
    parser.add_argument("--clear-cache", help="Remove o diretório cache_api", action="store_true")
    parser.add_argument("--name", help="Nome da Legislatura", type=str, default="default")
    parser.add_argument("--dataInicio", help="Data de início no formato AAAA-MM-DD", type=str, default="2023-02-01")
    parser.add_argument("--dataFim", help="Data de fim no formato AAAA-MM-DD", type=str, default=TODAY)
    args = parser.parse_args()
    
    name = f"indicador-{args.name}-{TODAY}"
    logging.info(f"Iniciando captura {name} de {args.dataInicio} até {args.dataFim}")
    legislatura = Legislatura(name=name, dataInicio=args.dataInicio, dataFim=args.dataFim)
    
    if args.refresh:
        os.remove(f"{name}.pickle") if os.path.isfile(f"{name}.pickle") else None
    
    if args.clear_cache:
        if os.path.isdir("cache_api"):
            shutil.rmtree("cache_api")
    
    legislatura.run()

if __name__ == "__main__":
    main()