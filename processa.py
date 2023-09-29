import requests
import json
import re
import os
from util import _acessar_api_camara, _acessar_api_portaltransparencia, _regex_get
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    handlers=[logging.FileHandler('example.log'),
                              logging.StreamHandler()])
DEBUG = True if logging.getLogger().level == 10 else False


CONFIG = {
    "itens" : 500,
    "dataInicio" : "2023-02-01",
}

class Parlamentar:
    def __init__(self, pid):
        self.pid = pid
        self.projetos = []
        self.variaveis = {}

    
    def run(self):
        self._busca_deputado()
        self._buscar_projetos()
        self._buscar_eventos()
        self._buscar_votacoes()
        self.processa_variavel_1()
        self.processa_variavel_2()
        self.processa_variavel_3()
        self.processa_variavel_4()
        self.processa_variavel_5()
        self.processa_variavel_6()
        self.processa_variavel_7()
        self.processa_variavel_8()
        self.processa_variavel_9()
        self.processa_variavel_10()
        self.processa_variavel_11()
        self.processa_variavel_12()
        self.processa_variavel_13()
        self.processa_variavel_14()
        self.processa_variavel_15()
        self.processa_variavel_16()


    def _busca_deputado(self):
        # Preparar os parâmetros para a API
        params = {
            "id": self.pid,
            "dataInicio" : CONFIG["dataInicio"]
        }
        
        # Acessar a API para obter a lista de deputados com o nome especificado
        endpoint = "deputados"
        dados = _acessar_api_camara(endpoint, params)
        
        # Se mais de um deputado for encontrado, você pode querer implementar alguma lógica
        # para lidar com isso. Aqui, eu estou assumindo que o primeiro deputado retornado é o correto.
        if dados:
            self.nome = dados[0]['nome']
            self.deputado_raw = dados[0]
            return self.pid
        else:
            logging.debug(f"Deputado com id {self.pid} não encontrado.")
            return None

    
    def _buscar_projetos(self):
        tipos_de_projetos = ['PDL', 'PEC', 'PL', 'PLP', 'PLV', #var 1,2
                             'VTS', #var 4
                             'SBT', #var 5
                             'PRL', #var 6
                             'EMP', 'EMR', #var 8
                             'RIC', 'PFC', #var 9
                             'PLN', #var 12
                             'REQ' #var 15
                             ]
        params = {
            "idDeputadoAutor": self.pid,
            "siglaTipo" : ",".join(tipos_de_projetos),
            "ordem": "ASC",
            "ordenarPor": "id",
            "itens": CONFIG['itens'],
            "dataApresentacaoInicio": CONFIG["dataInicio"]
        }
        self.projetos = _acessar_api_camara("proposicoes", params)
    
    def _buscar_eventos(self):
        orgaos = [180]
        params = {
            "dataInicio": CONFIG["dataInicio"],
            "idOrgao" : orgaos,
            "ordem": "ASC",
            "ordenarPor": "dataHoraInicio",
            "itens": CONFIG['itens']
            
            }
        self.eventos = _acessar_api_camara("eventos", params)
    
    def _buscar_votacoes(self):
        params = {
            "dataInicio": CONFIG["dataInicio"],
            "itens": CONFIG["itens"]
        }

        data = _acessar_api_camara("votacoes", params)

        self.votacoes = data

        
    def _filtrar_projetos_por_tipo(self, tipos_de_projetos):
        return [projeto for projeto in self.projetos if projeto.get('siglaTipo', '') in tipos_de_projetos]
    
    def _filtrar_projetos_protagonistas(self, projetos):
        projetos_protagonistas = []
        for projeto in projetos:
            projeto_id = projeto.get('id', None)
            if projeto_id:
                endpoint = f"proposicoes/{projeto_id}/autores"
                data = _acessar_api_camara(endpoint)
                
                if data:
                    for autor in data:
                        if autor['nome'] == self.nome and autor['ordemAssinatura'] == 1 and autor['proponente'] == 1:
                            projetos_protagonistas.append(projeto)
                            break
        return projetos_protagonistas

    def _filtrar_votacoes_por_orgao(self, orgaos):
        return [votacao for votacao in self.votacoes if votacao.get("proposicaoObjeto") and votacao.get('siglaOrgao', '') in orgaos]
    
    def _verificar_impacto(self, projeto, palavras_chave):
        ementa = projeto.get('ementa', '')
        tema = projeto.get('tema', '')  # Suponha que o campo 'tema' esteja disponível nos dados

        regex = r'\b(?:' + '|'.join(palavras_chave) + r')\b'
        return not (re.search(regex, ementa, re.IGNORECASE) or tema == "Homenagens e Datas Comemorativas")

    def _filtrar_eventos_por_orgao(self, orgaos_de_eventos):
        return [evento for evento in self.eventos if any(orgao.get('sigla', '') in orgaos_de_eventos for orgao in evento.get('orgaos', []))]
    
    def _checa_presenca_deputado(self, evento):
        endpoint = f"eventos/{evento['id']}/deputados"
        data = _acessar_api_camara(endpoint)

        if data:
            for deputado in data:
                if deputado['nome'] == self.nome:
                    return True
        return False

    def _calcular_maioria_partido(self, votos):
        partido = self.deputado_raw['siglaPartido']
        votos_do_partido = [voto['tipoVoto'] for voto in votos if voto['deputado_']['siglaPartido'] == partido]
        if not votos_do_partido:
            return None
        vote_count = {}
        for vote in votos_do_partido:
            if vote not in vote_count:
                vote_count[vote] = 0
            vote_count[vote] += 1
        maioria = max(vote_count, key=vote_count.get)
        return maioria
        

    def _obter_voto_parlamentar(self, votos):
        for voto in votos:
            if voto['deputado_']['id'] == self.pid:
                return voto['tipoVoto']
        return None

    def processa_variavel_1(self):    
        tipos_de_projetos = ['PDL', 'PEC', 'PL', 'PLP', 'PLV']
        self.projetos_legislativos = self._filtrar_projetos_por_tipo(tipos_de_projetos)
        
        contagem = len(self.projetos_legislativos)
        
        self.variaveis["variavel_1"] = {"value": contagem}
        return contagem

    def processa_variavel_2(self):
        self.projetos_protagonistas = self._filtrar_projetos_protagonistas(self.projetos_legislativos)

        contagem = len(self.projetos_protagonistas)
        self.variaveis["variavel_2"] = {"value": contagem}
        return contagem

    def processa_variavel_3(self):
        
        palavras_chave = [
            "hora", "dia", "Dia", "semana", "Semana", "Mês", "ano", "data",
            "festa", "calendario", "calendário", "titulo", "título",
            "prêmio", "medalha", "nome", "galeria", "ponte", "ferrovia",
            "estrada", "aeroporto", "rotatória", "honorário"
        ]
        contagem = 0
        for projeto in self.projetos_protagonistas:
            
            if self._verificar_impacto(projeto, palavras_chave):
                #print(f"Alto Impacto: {projeto.get('ementa')}")
                contagem += 1

        self.variaveis["variavel_3"] = {"value": contagem}
        return contagem

    def processa_variavel_4(self):
            # Filtrar votos em separado
            votos_em_separado = self._filtrar_projetos_por_tipo(['VTS'])
            contagem = len(votos_em_separado)
            self.variaveis["variavel_4"] = {"value": contagem}
            return contagem

    def processa_variavel_5(self):
        # Filtrar substitutivos
        substitutivos = self._filtrar_projetos_por_tipo(['SBT'])
        contagem = len(substitutivos)
        self.variaveis["variavel_5"] = {"value": contagem}
        return contagem

    def processa_variavel_6(self):
        # Filtrar relatorias apresentadas
        relatorias = self._filtrar_projetos_por_tipo(['PRL'])
        contagem = len(relatorias)
        self.variaveis["variavel_6"] = {"value": contagem}
        return contagem

    def processa_variavel_7(self):
        orgaos_de_eventos = ["PLEN"] # VERIFICAR

        eventos_filtrados = self._filtrar_eventos_por_orgao(orgaos_de_eventos)

        contagem = 0
        for evento in eventos_filtrados:
            if self._checa_presenca_deputado(evento):
                contagem += 1
                
        self.variaveis["variavel_7"] = {"value": contagem}
        return contagem

    def processa_variavel_8(self):
        # Filtrar emendas de plenário
        emendas_de_plenario = self._filtrar_projetos_por_tipo(['EMP', 'EMR'])
        
        # Filtrar fora as "Emenda de Plenário à MPV (Ato Conjunto 1/20)"
        emendas_filtradas = [emenda for emenda in emendas_de_plenario if emenda.get('descricaoTipo', '') != "Emenda de Plenário à MPV (Ato Conjunto 1/20)"]
        
        contagem = len(emendas_filtradas)
        
        self.variaveis["variavel_8"] = {"value": contagem}
        return contagem

    def processa_variavel_9(self):
        # Filtrar requerimentos de informação e propostas de fiscalização
        requerimentos_fiscalizacao = self._filtrar_projetos_por_tipo(['RIC', 'PFC'])
        
        contagem = len(requerimentos_fiscalizacao)
        
        self.variaveis["variavel_9"] = {"value": contagem}
        return contagem
    
    def processa_variavel_10(self):
        # Obter os dados em CSV do Portal da Transparência
        csv_data = _acessar_api_portaltransparencia(self.nome, 2023)

        # Soma da coluna Valor_pago como inteiros
        total_valor_pago = 0
        if not csv_data:
            logging.debug(csv_data)
        for row in csv_data:
            valor_pago_str = row.get('Valor pago', '0').replace('.', '').replace(',', '.')
            valor_pago_int = float(valor_pago_str)
            total_valor_pago += valor_pago_int

        self.variaveis["variavel_10"] = {"value": total_valor_pago}
        return total_valor_pago

    def processa_variavel_11(self):
        #TODO Checar diferença da Variavel 11
        # Filtrar emendas de plenário 
        emendas_de_plenario = self._filtrar_projetos_por_tipo(['EMP'])
        
        # Filtrar fora as "Emenda de Plenário à MPV (Ato Conjunto 1/20)"
        emendas_filtradas = [emenda for emenda in emendas_de_plenario if emenda.get('descricaoTipo', '') != "Emenda de Plenário à MPV (Ato Conjunto 1/20)"]
        
        contagem = len(emendas_filtradas)
        
        self.variaveis["variavel_11"] = {"value": contagem}
        return contagem

    def processa_variavel_12(self):
        #Emenda LOA

        emendas_loa = self._filtrar_projetos_por_tipo(["PLN"])

        contagem = len(emendas_loa)
        self.variaveis["variavel_12"] = {"value" : contagem}
        return contagem
    
    def processa_variavel_13(self):
        # Filtrar os projetos de lei de interesse
        tipos_de_projetos = ['PDL', 'PEC', 'PL', 'PLP', 'PLV']
        projetos_filtrados = self._filtrar_projetos_por_tipo(tipos_de_projetos)
        
        # Contar projetos com tramitação especial
        regimes_especiais = ['Especial', 'Especial (Art. 202 c/c 191, I, RICD)']
        contagem = 0
        for projeto in projetos_filtrados:
            if projeto.get('ultimo_status_regime', '') in regimes_especiais:
                contagem += 1
        
        self.variaveis["variavel_13"] = {"value": contagem}
        return contagem

    def processa_variavel_14(self):
        logging.debug(f"Buscando dep {self.nome}")

        # Preparar os parâmetros para a API
        params = {
            "dataInicio": CONFIG["dataInicio"],
            "itens": CONFIG["itens"],
            "ordem": "ASC",
            "ordenarPor": "dataInicio"
        }
        
        # Acessar a API para obter a lista de órgãos
        endpoint = f"deputados/{self.pid}/orgaos"
        data = _acessar_api_camara(endpoint, params)
        self.orgaos = data
        
        # Filtrar os órgãos para excluir os cargos obrigatórios
        orgaos_obrigatorios = [
            "CONGRESSO NACIONAL",
            "Plenário",
            "Plenário Comissão Geral",
            "CÂMARA DOS DEPUTADOS",
            f"Bancada de {self.deputado_raw['siglaUf']}"
        ]

        orgaos_filtrados = [
            cargo for cargo in data
            if cargo['nomeOrgao'] not in orgaos_obrigatorios and cargo['dataFim'] == None
        ]
        # Calcular a pontuação total com base no órgão e no título
        orgaos_pontuacao = {
            r"(Mesa Diretora|Secretaria)": {
                "Presidente": 1,
                "1º Vice-Presidente": 0.8,
                "2º Vice-Presidente": 0.6,
                "3º Vice-Presidente": 0.6,
                "1º Secretário": 1,
                "2º Secretário": 0.8,
                "3º Secretário": 0.6,
                "4º Secretário": 0.5,
                "1º Suplente de Secretário": 0.4,
                "2º Suplente de Secretário": 0.3,
                "3º Suplente de Secretário": 0.2,
                "4º Suplente de Secretário": 0.1,
                r"Secretári[ao]": 1,
            },
            r"Ouvidoria Parlamentar": {
                "Ouvidor-Geral": 1,
            },
            r"Coordenadoria": {
                "Coordenador-Geral": 1,
                "1º Coordenador Adjunto": 1,
                "2º Coordenador Adjunto": 0.8,
                "3º Coordenador Adjunto": 0.6,
            },
            r"Procurador[ia].*": {
                r"Procurador[oa]": 1,
                "1º Procurador Adjunto": 1,
                "2º Procurador Adjunto": 0.8,
                "3º Procurador Adjunto": 0.6,
            },
            r"Corregedoria Parlamentar": {
                "Corregedor": 1,
            },
            r"(Comissão|Grupo de Trabalho|Subcomissão)": {
                "Presidente": 1,
                "1º Vice-Presidente": 0.8,
                "2º Vice-Presidente": 0.6,
                "3º Vice-Presidente": 0.6,
                "Titular": 1,
                "Relator": 1,
                "Suplente" : 0.8,
                r"Coordenador" : 1,
            },
        }

        total_pontuacao = 0
        pontuacoes_por_orgao = {}
        

        for orgao in orgaos_filtrados:
            nome_orgao = orgao['nomeOrgao']
            titulo_cargo = orgao['titulo']

            pontuacoes_cargo = _regex_get(orgaos_pontuacao, nome_orgao)
            if pontuacoes_cargo:
                pontuacao = _regex_get(pontuacoes_cargo, titulo_cargo)
                
                # Se a pontuação foi encontrada, atualize a maior pontuação para esse órgão
                if pontuacao is not None:
                    if nome_orgao in pontuacoes_por_orgao:
                        pontuacoes_por_orgao[nome_orgao] = max(pontuacoes_por_orgao[nome_orgao], pontuacao)
                    else:
                        pontuacoes_por_orgao[nome_orgao] = pontuacao

        # Soma todas as maiores pontuações de cada órgão
        
        logging.debug(pontuacoes_por_orgao)
        total_pontuacao = sum(pontuacoes_por_orgao.values())
        
        self.variaveis["variavel_14"] = {"value": round(total_pontuacao, 2)}
        return total_pontuacao

    def processa_variavel_15(self):
        # Filtrar projetos que são requerimentos
        requerimentos = self._filtrar_projetos_por_tipo(['REQ'])
        
        # Filtrar aqueles que mencionam "Audiência Pública" na ementa
        requerimentos_audiencia_publica = [req for req in requerimentos if 'audiência pública' in req.get('ementa', '').lower()]
        
        contagem = len(requerimentos_audiencia_publica)
        
        self.variaveis["variavel_15"] = {"value": contagem}
        return contagem


    def processa_variavel_16(self):
        votos_desalinhados = 0
        total_votos = 0
        
        orgaos = ['PLEN'] #Checar se essa lógica esta certa.
        votacoes_filtradas = self._filtrar_votacoes_por_orgao(orgaos)
        
        loop = tqdm(votacoes_filtradas, desc="Buscando votos...") if DEBUG else votacoes_filtradas
        for votacao in loop:
            votacao_id = votacao['id']
            votos_detalhes = _acessar_api_camara(f"votacoes/{votacao_id}/votos")
            
            maioria_partido = self._calcular_maioria_partido(votos_detalhes)
            
            voto_parlamentar = self._obter_voto_parlamentar(votos_detalhes)
            
            if voto_parlamentar is None:  # Caso em que o parlamentar não votou ele esta *desalinhado* com o partido
                continue
            
            if voto_parlamentar == "Artigo 17": #Caso em que o parlamentar é da Mesa e não votou por conta do Artigo 17 considera-se que ele esta alinhado com o partido
                voto_parlamentar = maioria_partido

            if voto_parlamentar != maioria_partido:
                votos_desalinhados += 1
            total_votos += 1

            logging.debug(f"Proposicao: {votacao['proposicaoObjeto']}")
            logging.debug(f"Parlamentar: {self.nome}")
            logging.debug(f"Maioria Partido: {maioria_partido}")
            logging.debug(f"Voto parlamentar: {voto_parlamentar}")
        
        
            
        #TODO: Checar cálculo do Desvio e casos de None
        if total_votos > 0:
            contagem =  votos_desalinhados/total_votos
        else:
            contagem = 0
            logging.info(self.nome)
        logging.debug(f"Votos Desalinhados: {votos_desalinhados}\nVotos Totais: {total_votos}\nContagem: {contagem}")

        self.variaveis["variavel_16"] = {"value": contagem}
        logging.debug(self.variaveis["variavel_16"])
        return contagem

import pandas as pd
import numpy as np

class Legislatura:
    def __init__(self, name='default'):
        self.lista_de_nomes = self._lista()
        self.parlamentares = []
        self.name = name

    def _lista(self):
        endpoint = "deputados/"
        params = {}

        data = _acessar_api_camara(endpoint, params)

        deputados = [d['id'] for d in data]
        return deputados

    def _get_parlamentar(self, nome):
        parlamentar = Parlamentar(nome)
        parlamentar.run()
        self.parlamentares.append(parlamentar)

    def _to_df(self):
        data = []
        for parlamentar in self.parlamentares:
            # Initialize a dictionary with the name of the Parlamentar
            parlamentar_data = {"Deputado": parlamentar.nome,
                                "external_id" : parlamentar.pid}
            # Add each variable and its value to the dictionary
            for var, value_data in parlamentar.variaveis.items():
                parlamentar_data[var] = value_data['value']  # Extract just the value
            # Append the dictionary to the list
            data.append(parlamentar_data)
        # Convert the list of dictionaries to a DataFrame
        self.dataframe = pd.DataFrame(data)
        self.dataframe.to_pickle(f'{self.name}.pickle')

        return self.dataframe


    def _calculate_indicators(self):
        reverse_sturges = ["variavel_16"]

        for col in self.dataframe.columns:  # Skipping the "Deputado" column
            if 'variavel' in col:
                self.dataframe[col] = pd.to_numeric(self.dataframe[col], errors='coerce')
                self.dataframe[col + "_score"] = self._calculate_indicator(self.dataframe[col], reverse=col in reverse_sturges)

    def _calculate_outliers(self, column):
         # Calculando o 1º e 3º quartis (25% e 75% dos dados)
        Q1 = column.quantile(0.25)
        Q3 = column.quantile(0.75)
        # Calculando a amplitude interquartil (diferença entre o 3º e o 1º quartis)
        IQR = Q3 - Q1

        # Definindo os limites para detectar outliers (valores extremos)
        lower_limit = Q1 - 1.5 * IQR
        upper_limit = Q3 + 1.5 * IQR

        # Filtrando os dados para remover outliers
        filtered_data = column[(column >= lower_limit) & (column <= upper_limit)]

        return filtered_data
    
    def _calculate_indicator(self, column, reverse=False):
        # Filtrando os outliers usando a função _calculate_outliers
        filtered_data = self._calculate_outliers(column)
        
        #Armazena os 0s
        zero_indices = column == 0

        # Calculando a amplitude (diferença entre o valor máximo e mínimo)
        amplitude = filtered_data.max() - filtered_data.min()

        # Contando o número de valores positivos
        count = filtered_data[(filtered_data > 0)].count()

        # Aplicando a Regra de Sturges para determinar o número de classes
        num_classes = int(np.ceil(1 + 3.332 * np.log10(count))) if count > 0 else 0
          # Se num_classes for 0 retornamos uma série de zeros (todos os valores estão na mesma classe) e aplicamos 1 para os outliers
        if num_classes <= 0:
            scores = pd.Series([0.0] * len(column))
            scores[~column.isin(filtered_data)] = 1
        else:
            # Criando os bins
            # Menor valor positivo sem outliers
            min_value = filtered_data[(filtered_data > 0)].min()
            # Maior valor sem outliers
            max_value = filtered_data.max()

            bins = np.linspace(min_value, max_value, num_classes + 1)
            bins = np.insert(bins, 0, -np.inf)
            bins[-1] = np.inf

            if reverse:
                bins = bins[::-1]
            # Calculando as classes para todos os valores na coluna original
            classes = np.digitize(column, bins, right=True) - 1
            
            # Calculando a "posição" da classe como um valor float entre 0 e 1
            posicao_classe = (classes) / (num_classes)            
            
            # Arredondando a "posição" da classe para duas casas decimais
            scores = np.round(posicao_classe, 2)
            if not reverse:
                scores[zero_indices] = 0
            if reverse:
                scores[scores==0] = 0.1
        return scores


    def _calculate_score_final(self):
        # Calcula o score final de todas colunas com final _score
        score_columns = [col for col in self.dataframe.columns if col.endswith("_score")]
        
        self.dataframe["score_final"] = round(self.dataframe[score_columns].mean(axis=1),2)

    def _calculate_stars(self, column):
        
        filtered_data = self._calculate_outliers(column)

        max_score = filtered_data.max()
        if max_score == 0:
            return pd.Series([0] * len(column), index=column.index)
        interval = max_score / 5
        stars = pd.cut(
            column,
            bins=[0, interval, 2 * interval + 0.01, 3 * interval + 0.01, 4 * interval + 0.01, np.inf],
            labels=[1, 2, 3, 4, 5],
            right=True,
            include_lowest=True
        )
        self.dataframe["stars"] = stars
        return stars

    def _to_csv(self, output_path):
        self.dataframe.to_csv(output_path, index=False)
        logging.info(f"Arquivo salvo em {output_path}")

    def run(self, output_path):
        if os.path.isfile(f'{self.name}.pickle'):
            self.dataframe = pd.read_pickle(f'{self.name}.pickle')
        else:
            for pid in tqdm(self.lista_de_nomes, desc="Carregando deputados em exercício..."):
                self._get_parlamentar(pid)
            self._to_df()
        self._calculate_indicators()
        self._calculate_score_final()   
        self._calculate_stars(self.dataframe["score_final"])
        self._to_csv(output_path)


legislatura = Legislatura(name="teste")
legislatura.run("data/resultado.csv")