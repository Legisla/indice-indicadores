Índice Legisla Brasil - Importador de Dados
===========================================

Sobre o Projeto
---------------

O Índice Legisla Brasil é uma ferramenta de análise parlamentar que avalia os deputados federais brasileiros com base em uma metodologia de 17 indicadores distribuídos em 4 eixos principais. Este repositório contém o código-fonte utilizado para a coleta, processamento e análise desses dados.

Funcionamento do Código
-----------------------

### Arquivo Principal

-   **script.py**: Este é o arquivo de execução principal. Ele coordena o fluxo de trabalho do programa, incluindo a coleta de dados, processamento e geração de indicadores.

### Caching de Dados

-   **Cache API**: Para otimizar as requisições e reduzir o volume de chamadas de API, o sistema utiliza um mecanismo de cache. Ele armazena as respostas das requisições em `cache_api`, utilizando um hash da URL solicitada para recuperação eficiente.

### Processamento de Indicadores

-   **Arquivo processa.py**: Cada indicador do Índice Legisla Brasil é processado por funções específicas localizadas neste arquivo. Essas funções são responsáveis por calcular os valores individuais dos indicadores.

### Classe Legislatura

-   **Análise e Totalização**: A classe `Legislatura` em `script.py` desempenha um papel crucial na análise dos dados. Ela totaliza os indicadores e aplica a regra de Sturges para a análise estatística.

### Saída de Dados

-   **Arquivo CSV**: O resultado do processamento é um arquivo CSV, que contém todos os indicadores calculados e está pronto para ser importado em sistemas que utilizam a estrutura Laravel.

Uso e Execução
--------------

Para executar o script, utilize:

bash

`python script.py`

Este comando iniciará o processo de coleta e análise de dados, resultando em um arquivo CSV. Este arquivo pode ser importado em um sistema Laravel usando o comando:

bash

`php artisan import:congressperson-indicators --csv_file [CSV_PATH] --initiator [CONSOLE/CRON]`

Configuração e Instalação
-------------------------

Instruções para configurar o ambiente de desenvolvimento e instalar as dependências do projeto.

bash

`git clone [url-do-repositorio]
cd [nome-do-diretorio]
pip install -r requirements.txt`

Estrutura do Projeto
--------------------

-   `/src`: Código fonte principal.
-   `/data`: Dados coletados e gerados.
-   `/cache_api`: Armazena requisições aos servidores da Câmara e Portal da Transparência

Contribuindo
------------

Contribuições são bem-vindas e podem ser feitas diretamente no repositório. Encorajamos os desenvolvedores a contribuir com melhorias no código, novas funcionalidades ou correções de bugs.

Licença
-------

Este projeto é distribuído sob a Licença MIT.

Contato
-------

Para mais informações, suporte ou dúvidas, entre em contato através do email: <falecom@legislabrasil.org>