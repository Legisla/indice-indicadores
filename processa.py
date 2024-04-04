import requests
import json
import re
import os
from datetime import datetime
from util import _acessar_api_camara, _acessar_api_portaltransparencia, _acessar_bulk_camara, _regex_get
from tqdm import tqdm
import logging
import gc
from unidecode import unidecode


DEBUG = True if logging.getLogger().level == 10 else False

CONFIG = {
    "itens" : 500,
    "dataInicio" : "2023-02-01",
    "dataFim" : datetime.now().strftime('%Y-%m-%d')
}

class Parlamentar:
    def __init__(self, parlamentar, dataInicio = CONFIG['dataInicio'], dataFim=CONFIG['dataFim']):
        self.pid = parlamentar.get('id')
        self.nome = parlamentar.get('nome')
        self.dados = {}
        self.parlamentar_raw = parlamentar
        self.dataInicio = dataInicio
        self.dataFim = dataFim
        self.projetos = []
        self.variaveis = {}
        self.historico = []
        self.tempo = {}


    def run(self):
        self.projetos = self._buscar_projetos()
        self.dados = self._buscar_dados()
        self.eventos = self._buscar_eventos()
        self.votacoes = self._buscar_votacoes()
        self.historico = self._buscar_historico()
        self.tempo = self._calcula_tempo()
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
        self._cleanup()
        
    def _cleanup(self):
        del(self.projetos)
        del(self.votacoes)
        del(self.eventos)
        del(self.parlamentar_raw)
        gc.collect()

    def _buscar_dados(self): 
        id= self.pid
        endpoint = f"deputados/{id}"
        dados = _acessar_api_camara(endpoint)
        return(dados)

    def _buscar_projetos(self, extra_params={}):
        tipos_de_projetos = ['PDL', 'PEC', 'PL', 'PLP', 'PLV', #var 1,2
                             'VTS', #var 4
                             'SBT', #var 5
                             'PRL', #var 6
                             'EMP', 'EMR', #var 8
                             'RIC', 'PFC', #var 9
                             'PLN', #var 12
                             'REQ', #var 15
                             'EMC' #var 11
                             ]
        params = {
            "idDeputadoAutor": self.pid,
            "siglaTipo" : ",".join(tipos_de_projetos),
            "ordem": "ASC",
            "ordenarPor": "id",
            "itens": CONFIG['itens'],
            "dataApresentacaoInicio": self.dataInicio,
            "dataApresentacaoFim" : self.dataFim
        }    

        params.update(extra_params)
        
        return(_acessar_api_camara("proposicoes", params))
    
    def _buscar_eventos(self, extra_params={}):
        orgaos = [180]
        params = {
            "dataInicio": self.dataInicio,
            "dataFim": self.dataFim,
            "idOrgao" : orgaos,
            "ordem": "ASC",
            "ordenarPor": "dataHoraInicio",
            "itens": CONFIG['itens']
        }
        
        params.update(extra_params)
        return(_acessar_api_camara("eventos", params))
    
    def _buscar_votacoes(self):
        params = {
            "dataInicio": self.dataInicio,
            "dataFim" : self.dataFim,
            "itens": CONFIG["itens"]
        }

        data = _acessar_bulk_camara("votacoesVotos", self._lista_anos())
        
        votacoes = {}

        for voto in data:
            if self._checa_data(voto['dataHoraVoto']):
                if votacoes.get(voto['idVotacao']):
                    votacoes[voto['idVotacao']].append(voto)
                else:
                    votacoes[voto['idVotacao']] = [voto]
        return(votacoes)

    def _buscar_historico(self):
        params = {
            "dataInicio": self.dataInicio,
            "dataFim" : self.dataFim
        }    
        id= self.pid
        endpoint = f"deputados/{id}/historico"
        historico = _acessar_api_camara(endpoint, params)
        return(historico)
    
    def _calcula_tempo(self):
        data_anterior = None
        situacao_anterior = None
        tempo = {}
        
        for index, h in enumerate(self.historico):
            data_atual = datetime.strptime(h['dataHora'], '%Y-%m-%dT%H:%M')
            situacao_atual = h['situacao']
            
            if situacao_anterior is None:
                data_anterior = data_atual
                situacao_anterior = situacao_atual
                tempo[situacao_anterior] = 0
            else:
                diferenca = data_atual - data_anterior
                tempo[situacao_anterior] = tempo.get(situacao_anterior, 0) + int(diferenca.days)
                situacao_anterior = situacao_atual
                data_anterior = data_atual

            if index == len(self.historico) - 1:
                diferenca = datetime.now() - data_atual
                tempo[situacao_atual] = tempo.get(situacao_atual, 0) + int(diferenca.days)
        
        return tempo

    def _checa_projeto(self, pid):
        return(pid in [projeto['id'] for projeto in self.projetos])
    
    def _checa_data(self, datacomt):
        start_date_dt = datetime.strptime(self.dataInicio, "%Y-%m-%d")
        end_date_dt = datetime.strptime(self.dataFim, "%Y-%m-%d")

        vote_date_str = datacomt.split('T')[0]  # Pega só a parte da data, ignorando a hora
        vote_date_dt = datetime.strptime(vote_date_str, "%Y-%m-%d")

        return start_date_dt <= vote_date_dt <= end_date_dt
    
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
    
    def _filtrar_eventos_por_orgao(self, orgaos_de_eventos):
        return [evento for evento in self.eventos if any(orgao.get('sigla', '') in orgaos_de_eventos for orgao in evento.get('orgaos', []))]
    
    def _checa_presenca_deputado(self, evento):
        endpoint = f"eventos/{evento['id']}/deputados"
        data = _acessar_api_camara(endpoint)

        if data:
            for deputado in data:
                if deputado['id'] == self.pid:
                    return True
        return False

    def _calcular_maioria_partido(self, votos):
        partido = self.parlamentar_raw['siglaPartido']
        votos_do_partido = [voto['voto'] for voto in votos if voto['deputado_']['siglaPartido'] == partido]
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
            if voto['deputado_']['id'] == str(self.pid):
                return voto['voto']
        return None

    def _lista_anos(self):
        start_year = int(self.dataInicio.split('-')[0])
        years = [start_year]
        if self.dataFim:
            end_year = int(self.dataFim.split('-')[0])
            years = list(range(start_year, end_year + 1))
        return years

    def processa_variavel_1(self):
        """Calcula a quantidade de projetos legislativos (PDL, PEC, PL, PLP, PLV) apresentados pelo parlamentar, incluindo tanto autoria própria quanto coautoria."""

        tipos_de_projetos = ['PDL', 'PEC', 'PL', 'PLP', 'PLV']
        self.projetos_legislativos = self._filtrar_projetos_por_tipo(tipos_de_projetos)
        
        contagem = len(self.projetos_legislativos)
        
        self.variaveis["variavel_1"] = {"value": contagem}
        return contagem

    def processa_variavel_2(self):
        """Determina o número de projetos nos quais o parlamentar é o autor principal. Este cálculo envolve filtrar projetos legislativos e identificar aqueles onde o parlamentar é o primeiro autor, indicando protagonismo nas matérias legislativas. A função `_filtrar_projetos_protagonistas` é usada para isolar esses projetos."""

        self.projetos_protagonistas = self._filtrar_projetos_protagonistas(self.projetos_legislativos)

        contagem = len(self.projetos_protagonistas)
        self.variaveis["variavel_2"] = {"value": contagem}
        return contagem

    def processa_variavel_3(self):
        """Avalia a relevância das autorias do parlamentar, categorizando os projetos irrelevantes quando tem o tema Homenagens e Datas Comemorativas e removendo-os da contagem."""

        contagem = 0
        projetos_irrelevantes = [projeto['id'] for projeto in self._buscar_projetos({'codTema' : 72})]

        for projeto in self.projetos_legislativos:
            if projeto['id'] not in projetos_irrelevantes:
                contagem += 1

        self.variaveis["variavel_3"] = {"value": contagem}
        return contagem

    def processa_variavel_4(self):
        """Conta o número de votos em separado apresentados pelo parlamentar, refletindo sua participação ativa e perspectivas alternativas em votações."""
        
        # Filtrar votos em separado
        votos_em_separado = self._filtrar_projetos_por_tipo(['VTS'])
        contagem = len(votos_em_separado)
        self.variaveis["variavel_4"] = {"value": contagem}
        return contagem

    def processa_variavel_5(self):
        """Mede a quantidade de substitutivos apresentados pelo parlamentar, demonstrando sua capacidade de propor soluções alternativas em debates legislativos."""

        # Filtrar substitutivos
        substitutivos = self._filtrar_projetos_por_tipo(['SBT'])
        contagem = len(substitutivos)
        self.variaveis["variavel_5"] = {"value": contagem}
        return contagem

    def processa_variavel_6(self):
        """Quantifica as relatorias apresentadas pelo parlamentar, indicando seu envolvimento e influência na análise e destino de matérias legislativas."""

        # Filtrar relatorias apresentadas
        relatorias = self._filtrar_projetos_por_tipo(['PRL'])
        contagem = len(relatorias)
        self.variaveis["variavel_6"] = {"value": contagem}
        return contagem

    def processa_variavel_7(self):
        """Calcula a frequência de presença do parlamentar em sessões deliberativas no Plenário. Envolve a filtragem de eventos pelo tipo 'Sessão Deliberativa' e a verificação da presença do parlamentar usando a função `_checa_presenca_deputado`."""


        eventos_filtrados = [evento for evento in self.eventos if evento.get('descricaoTipo', '') == "Sessão Deliberativa" and evento.get('localCamara',{}).get('nome', '') == "Plenário da Câmara dos Deputados" ]
        contagem = 0
        for evento in eventos_filtrados:
            if self._checa_presenca_deputado(evento):
                contagem += 1
                
        self.variaveis["variavel_7"] = {"value": contagem}
        return contagem

    def processa_variavel_8(self):
        """Conta as emendas de plenário e relator (tipos EMP e EMR) propostas pelo parlamentar, excluindo as 'Emendas de Plenário à MPV (Ato Conjunto 1/20)', para avaliar sua contribuição em sessões plenárias."""

        # Filtrar emendas de plenário
        emendas_de_plenario = self._filtrar_projetos_por_tipo(['EMP', 'EMR'])
        
        # Filtrar fora as "Emenda de Plenário à MPV (Ato Conjunto 1/20) codTipo = 873

        emendas_filtradas = [emenda for emenda in emendas_de_plenario if emenda.get('codTipo') != 873]
        
        contagem = len(emendas_filtradas)
        
        self.variaveis["variavel_8"] = {"value": contagem}
        return contagem

    def processa_variavel_9(self):
        """Determina o número total de solicitações de informações e propostas de fiscalização e controle protocoladas."""

        # Filtrar requerimentos de informação e propostas de fiscalização
        requerimentos_fiscalizacao = self._filtrar_projetos_por_tipo(['RIC', 'PFC'])
        
        contagem = len(requerimentos_fiscalizacao)
        
        self.variaveis["variavel_9"] = {"value": contagem}
        return contagem
    
    def processa_variavel_10(self):
        """Calcula o total de emendas parlamentares empenhadas que foram pagas até o momento da execução do script."""

        # Obter os dados em CSV do Portal da Transparência
        csv_data = _acessar_api_portaltransparencia(self.nome, self._lista_anos())

        # Soma da coluna Valor_pago como inteiros
        total_valor_pago = 0
        if csv_data:
            for row in csv_data:
                valor_pago_str = row.get('Valor pago', '0').replace('.', '').replace(',', '.')
                valor_pago_int = float(valor_pago_str)
                total_valor_pago += valor_pago_int

        self.variaveis["variavel_10"] = {"value": round(total_valor_pago,2)}
        return total_valor_pago

    def processa_variavel_11(self):
        """Avalia o número de emendas de plenário específicas para Medidas Provisórias."""
        # Filtrar emendas de plenário 
        emendas_de_plenario = self._filtrar_projetos_por_tipo(['EMP'])
        
        # Filtrar só as "Emenda de Plenário à MPV (Ato Conjunto 1/20)"
        emendas_de_plenario_filtradas = [emenda for emenda in emendas_de_plenario if emenda.get('codTipo') == 873]
        
        # Filtrar emendas de comissão
        emendas_de_comissao = self._filtrar_projetos_por_tipo(['EMC'])
        
        
        frase_filtro = "Dá nova redação à MPV 1162/2023"
        # Convertendo para minúsculas e removendo a acentuação
        frase_sem_acentos = unidecode(frase_filtro.lower())
        # Filtrar só as "Emenda de comissão" que contenham o texto "dá nova redação à MPV" 
        emendas_de_comissao_filtradas = [emenda for emenda in emendas_de_comissao if unidecode(emenda.get('ementa').lower()) == frase_sem_acentos]

        # Juntando as duas listas
        emendas_filtradas = emendas_de_comissao_filtradas + emendas_de_plenario_filtradas

        contagem = len(emendas_filtradas)
        
        self.variaveis["variavel_11"] = {"value": contagem}
        return contagem

    def processa_variavel_12(self):
        """Quantifica as emendas propostas pelo parlamentar em projetos de lei orçamentária (PLN)."""

        #Emenda LOA

        emendas_loa = self._filtrar_projetos_por_tipo(["PLN"])

        contagem = len(emendas_loa)
        self.variaveis["variavel_12"] = {"value" : contagem}
        return contagem
    
    def processa_variavel_13(self):
        """Calcula a quantidade de projetos de lei de interesse do parlamentar com regime de tramitação especial. Devido à limitação da API, que não permite filtrar proposições diretamente por seu status de tramitação, este indicador requer uma abordagem diferente. Primeiramente, utiliza-se a função `_acessar_bulk_camara` para coletar um conjunto abrangente de proposições. Em seguida, cada proposição é analisada individualmente pela função `_checa_projeto` para determinar se pertence ao parlamentar. Finalmente, os projetos são filtrados com base nos regimes especiais de tramitação."""


        # Filtrar os projetos de lei de interesse
        tipos_de_projetos = ['PDL', 'PEC', 'PL', 'PLP', 'PLV']
        projetos = _acessar_bulk_camara("proposicoes", self._lista_anos())
        # Contar projetos com tramitação especial
        regimes_especiais = ['Especial', 'Especial (Art. 202 c/c 191, I, RICD)']
        contagem = 0
        for projeto in projetos:
            if self._checa_data(projeto.get('dataApresentacao')) and self._checa_projeto(projeto.get('id')):
                if projeto.get('siglaTipo') in tipos_de_projetos and projeto.get('ultimoStatus',{}).get('regime') in regimes_especiais:
                    contagem += 1
        
        self.variaveis["variavel_13"] = {"value": contagem}
        return contagem

    def processa_variavel_14(self):
        """Calcula uma pontuação baseada nos cargos ocupados pelo parlamentar durante a legislatura. O método envolve coletar dados de órgãos via API, filtrar cargos obrigatórios e calcular pontuações com base no título do cargo ocupado."""


        logging.debug(f"Buscando dep {self.nome}")

        # Preparar os parâmetros para a API
        params = {
            "dataInicio": self.dataInicio,
            "dataFim" : self.dataFim,
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
            f"Bancada de {self.parlamentar_raw['siglaUf']}"
        ]

        orgaos_filtrados = [
            cargo for cargo in data
            if cargo['nomeOrgao'] not in orgaos_obrigatorios and cargo['dataFim'] == None
        ]
        # Calcular a pontuação total com base no órgão e no título
        pontuacoes_cargos = {
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
            "Ouvidor-Geral": 1,
            "Coordenador-Geral": 1,
            "Secretário de Relações Internacionais": 1,
            "Secretário de Comunicação Social": 1,
            "Secretário de Transparência": 1,
            "Sec de Part Inter e Mídias Digitais": 1,
            "Secretário da Juventude": 1,
            "Procurador": 1,
            "1º Procurador Adjunto": 1,
            "2º Procurador Adjunto": 0.8,
            "3º Procurador Adjunto": 0.6,
            "Procuradora": 1,
            "Corregedor": 1,
            "Titular": 1,
            "Suplente": 0.8,
            "1º Coordenador Adjunto": 1,
            "2º Coordenador Adjunto": 0.8,
            "3º Coordenador Adjunto": 0.6,
            "Relator": 1
        }


        total_pontuacao = 0

        total_pontuacao = 0

        for orgao in orgaos_filtrados:
            titulo_cargo = orgao['titulo']
            
            # Busque a pontuação diretamente do dicionário
            pontuacao = pontuacoes_cargos.get(titulo_cargo, None)
            
            # Se a pontuação foi encontrada, some-a ao total
            if pontuacao is not None:
                total_pontuacao += pontuacao

        # Arredonde a pontuação total para duas casas decimais
        total_pontuacao = round(total_pontuacao, 2)

        # Armazene a pontuação total
        self.variaveis["variavel_14"] = {"value": total_pontuacao}

        # Retorne a pontuação total
        return total_pontuacao


    def processa_variavel_15(self):
        """Determina o número de requerimentos de Audiência Pública protocolados pelo parlamentar."""

        # Filtrar projetos que são requerimentos e códTipo 294 ("Requisição de Audiência Pública")
        requerimentos = self._filtrar_projetos_por_tipo(['REQ'])
        # Filtrando os requerimentos
        requerimentos_audiencia_publica = [req for req in requerimentos if req.get("codTipo") == 294]
        contagem = len(requerimentos_audiencia_publica)
        
        self.variaveis["variavel_15"] = {"value": contagem}
        return contagem


    def processa_variavel_16(self):
        """Avalia a taxa de alinhamento do parlamentar com a maioria do partido nas votações. O processo inclui o cálculo da maioria do partido em votações específicas e a comparação com o voto do parlamentar. Utiliza as funções `_calcular_maioria_partido` e `_obter_voto_parlamentar` para determinar o alinhamento partidário nas decisões de voto."""


        votos_desalinhados = 0
        total_votos = 0
        
        loop = tqdm(self.votacoes, desc="Buscando votos...") if DEBUG else self.votacoes
        for votoId in self.votacoes:
            votos_detalhes = self.votacoes[votoId]
            import pprint
            maioria_partido = self._calcular_maioria_partido(votos_detalhes)
            
            voto_parlamentar = self._obter_voto_parlamentar(votos_detalhes)
            
            if voto_parlamentar is None:  # Caso em que o parlamentar não votou ele esta *desalinhado* com o partido
                continue
            
            if voto_parlamentar == "Artigo 17": #Caso em que o parlamentar é da Mesa e não votou por conta do Artigo 17 considera-se que ele esta alinhado com o partido
                voto_parlamentar = maioria_partido

            if voto_parlamentar != maioria_partido:
                votos_desalinhados += 1
            total_votos += 1

            logging.debug(f"Proposicao: {votos_detalhes[0]['uriVotacao']}")
            logging.debug(f"Parlamentar: {self.nome}")
            logging.debug(f"Maioria Partido: {maioria_partido}")
            logging.debug(f"Voto parlamentar: {voto_parlamentar}")
        
        
            
        #TODO: Checar cálculo do Desvio e casos de None
        if total_votos > 0:
            contagem =  1 - (votos_desalinhados/total_votos)
        else:
            contagem = 0
            logging.info(self.nome)
        logging.debug(f"Votos Desalinhados: {votos_desalinhados}\nVotos Totais: {total_votos}\nContagem: {contagem}")

        self.variaveis["variavel_16"] = {"value": round(contagem,2)}
        logging.debug(self.variaveis["variavel_16"])
        return contagem