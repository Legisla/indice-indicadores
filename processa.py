import requests
import json
import re
import os
from util import _acessar_api_camara, _acessar_api_portaltransparencia
from tqdm import tqdm

CONFIG = {
    "itens" : 500,
    "dataInicio" : "2023-01-01"
}

class Parlamentar:
    def __init__(self, nome):
        self.nome = nome
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
            "nome": self.nome,
        }
        
        # Acessar a API para obter a lista de deputados com o nome especificado
        endpoint = "deputados"
        dados = _acessar_api_camara(endpoint, params)
        
        # Se mais de um deputado for encontrado, você pode querer implementar alguma lógica
        # para lidar com isso. Aqui, eu estou assumindo que o primeiro deputado retornado é o correto.
        if dados:
            self.id_deputado = dados[0]['id']
            self.deputado_raw = dados[0]
            return self.id_deputado
        else:
            print(f"Deputado com nome {self.nome} não encontrado.")
            return None

    
    def _buscar_projetos(self):
        params = {
            "autor": self.nome,
            "ordem": "ASC",
            "ordenarPor": "id",
            "itens": CONFIG['itens'],
            "dataApresentacaoInicio": CONFIG["dataInicio"]
                  }
        self.projetos = _acessar_api_camara("proposicoes", params)
    
    def _buscar_eventos(self):
        params = {
            "dataInicio": CONFIG["dataInicio"],
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
            if voto['deputado_']['nome'] == self.nome:
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

        for row in csv_data:
            valor_pago_str = row.get('Valor pago', '0').replace('.', '').replace(',', '')
            valor_pago_int = int(valor_pago_str)
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
        # Preparar os parâmetros para a API
        params = {
            "dataInicio": CONFIG["dataInicio"],
            "itens": CONFIG["itens"],
            "ordem": "ASC",
            "ordenarPor": "dataInicio"
        }
        
        # Acessar a API para obter a lista de órgãos
        endpoint = f"deputados/{self.id_deputado}/orgaos"
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
            orgao for orgao in data
            if orgao['nomeOrgao'] not in orgaos_obrigatorios
        ]
        
        # Contar o número total de cargos ocupados após a filtragem
        contagem = len(orgaos_filtrados)
        
        self.variaveis["variavel_14"] = {"value": contagem}
        return contagem

    def processa_variavel_15(self):
        # Filtrar projetos que são requerimentos
        requerimentos = self._filtrar_projetos_por_tipo(['REQ'])
        
        # Filtrar aqueles que mencionam "Audiência Pública" na ementa
        requerimentos_audiencia_publica = [req for req in requerimentos if 'audiência pública' in req.get('ementa', '').lower()]
        
        contagem = len(requerimentos_audiencia_publica)
        
        self.variaveis["variavel_15"] = {"value": contagem}
        return contagem


    def processa_variavel_16(self):
        votos_alinhados = 0
        total_votos = 0
        
        orgaos = ['PLEN'] #Checar se essa lógica esta certa.
        votacoes_filtradas = self._filtrar_votacoes_por_orgao(orgaos)
        for votacao in tqdm(votacoes_filtradas, desc="Buscando votos..."):
            votacao_id = votacao['id']
            votos_detalhes = _acessar_api_camara(f"votacoes/{votacao_id}/votos")
            
            maioria_partido = self._calcular_maioria_partido(votos_detalhes)
            
            if maioria_partido is None:  # Caso em que o partido não participou da votação
                continue
            
            voto_parlamentar = self._obter_voto_parlamentar(votos_detalhes)
            
            if voto_parlamentar is None:  # Caso em que o parlamentar não votou
                continue

            if voto_parlamentar == maioria_partido:
                votos_alinhados += 1
            total_votos += 1

        #TODO: Checar cálculo do Desvio e casos de None
        contagem = total_votos - votos_alinhados

        self.variaveis["variavel_16"] = {"value": contagem}

        return contagem

nomes_de_parlamentares = ["Adriana Ventura", "Afonso Hamm"]#, "Outro Nome", "Mais Um Nome"]

# Criar objetos Parlamentar para cada nome e processar suas variáveis
parlamentares = []
for nome in nomes_de_parlamentares:
    parlamentar = Parlamentar(nome)
    parlamentar.run()
    parlamentares.append(parlamentar)
    print(f"Nome do Parlamentar: {parlamentar.nome}")
    for var, data in parlamentar.variaveis.items():
        print(f"{var}: {data['value']}")
    print("-" * 20)  # Separator for readability