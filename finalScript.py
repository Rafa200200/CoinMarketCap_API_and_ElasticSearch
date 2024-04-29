import requests
from requests.exceptions import RequestException
import json
import time
from datetime import datetime, timedelta
import logging

# Configurar o logger
logging.basicConfig(filename='app.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# Configuração inicial
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': '7e4ad96d-e64a-4a03-8c54-8d0040f42b39'
}
params = {
    'start': '1',
    'limit': '2',
    'convert': 'EUR'
}
url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

# Função para adicionar dados ao JSON
def adicionar_dados_ao_json(novos_dados, nome_do_ficheiro):
    try:
        # Ler o ficheiro JSON existente
        with open(nome_do_ficheiro, 'r') as file:
            dados_existentes = json.load(file)
    except FileNotFoundError:
        # Se o ficheiro não existir, criar um dicionário vazio
        dados_existentes = {}

    # Adicionar a marca de tempo aos novos dados
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for symbol, data in novos_dados.items():
        # Se a chave não existir, criar uma lista para os registros
        if symbol not in dados_existentes:
            dados_existentes[symbol] = []
        # Verificar se o valor associado à chave é uma lista
        elif not isinstance(dados_existentes[symbol], list):
            # Se não for uma lista, criar uma lista com o valor existente
            dados_existentes[symbol] = [dados_existentes[symbol]]
        # Adicionar os novos dados com a marca de tempo à lista do ativo
        dados_existentes[symbol].append({'timestamp': timestamp, 'valor': data['price'], 'volume_24h': data['volume_24h'], 'volume_change_24h': data['volume_change_24h'], 'market_cap': data['market_cap']})

    # Escrever os dados atualizados no ficheiro
    with open(nome_do_ficheiro, 'w') as file:
        json.dump(dados_existentes, file, indent=4)

    # Log do processo de adição de dados
    logging.info(f"Dados adicionados ao arquivo {nome_do_ficheiro}: {novos_dados}")

# Definir o tempo total de execução (5 dias)
tempo_total = 5 * 24 * 3600  # 5 dias em segundos

# Loop principal
tempo_inicio = time.time()
while time.time() - tempo_inicio < tempo_total:
    try:
        # Calcular o tempo restante até a próxima hora
        tempo_atual = datetime.now()
        proxima_hora = tempo_atual + timedelta(hours=1)
        proximo_inicio = proxima_hora.replace(minute=0, second=0, microsecond=0)
        tempo_restante = (proximo_inicio - tempo_atual).total_seconds()

        # Fazer o pedido à API
        r = requests.get(url, params=params, headers=headers)
        r.raise_for_status()  # Lança uma exceção se o status da resposta não for 200 OK
        coins = r.json()['data']

        # Preparar os dados para adicionar
        dados_para_adicionar = {}
        for coin in coins:
            symbol = coin['symbol']
            quote = coin.get('quote', {}).get('EUR', {})  # Ajuste para acessar 'quote' de forma segura
            dados_para_adicionar[symbol] = {
                'price': quote.get('price', 0),
                'volume_24h': quote.get('volume_24h', 0),
                'volume_change_24h': quote.get('volume_change_24h', 0),
                'market_cap': quote.get('market_cap', 0)
            }

        # Adicionar os dados ao ficheiro JSON
        adicionar_dados_ao_json(dados_para_adicionar, 'valores_coins.json')

        # Aguardar até a próxima hora
        time.sleep(tempo_restante)

    except RequestException as e:
        # Log de erros
        logging.error(f"Erro durante a solicitação: {e.__class__.__name__} - {e}")

        # Aguardar um tempo antes de tentar novamente
        time.sleep(60)  # Aguarda 1 minuto antes de tentar novamente

    except Exception as e:
        # Log de erros inesperados
        logging.error(f"Erro inesperado: {e.__class__.__name__} - {e}")

logging.info("A execução do script foi concluída.")

