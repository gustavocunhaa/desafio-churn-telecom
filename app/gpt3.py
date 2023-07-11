# SCRIPT PARA GERAR CLASSIFICAÇÕES DE COMENTÁRIOS USANDO O PROMPT PADRÃO SALVO NA PASTA 'data'
# O Script faz a integração com a API, realiza o prompt para cada comentário e salva um csv com os dados retornados

import os
import dotenv
import requests as rt
import json
import pandas as pd
from datetime import datetime as dt

# Definindo as pastas do diretório

root = os.getcwd()
dataFolder = os.path.join(root, 'data')
env = dotenv.find_dotenv()

dataPath = os.path.join(dataFolder, 'churn_com_texto.csv')
dados = pd.read_csv(dataPath)
print("Dados com os comentários importados")

# Lendo a chave de uso da API e o prompt a ser utlizado

api_key_gpt = dotenv.get_key(env, 'API_KEY_GPT')
prompt = open(os.path.join(dataFolder, 'prompt_gpt.txt'), mode='r', encoding='utf-8').read()
headers = {"Authorization": f"Bearer {api_key_gpt}", "Content-Type":"application/json"}
link = "https://api.openai.com/v1/chat/completions"
id_model = "gpt-3.5-turbo"
print("Conexão com a API preparada")

# Percorrendo para cada Comentário da base de dados original, gerando uma avaliação via GPT3
lista_avaliacoes_gpt = []

for i in range(0, dados.shape[0]):
    mensagem_prompt = prompt + '"' + str(dados['Comentários'][i]) + '"'

    body_message = json.dumps(
        {
            "model": id_model,
            "messages": [{"role": "user", "content": mensagem_prompt}]
        }
    )

    request_gpt = rt.post(link, headers=headers, data=body_message)
    avaliacao = json.loads(request_gpt.content)['choices'][0]['message']['content']
    lista_avaliacoes_gpt.append(avaliacao)
    print("Integração realizada. Avaliação dada pelo GPT3: ", avaliacao)

# Salvando a lista das avaliações geradas em CSV
print("Preparando para salvar o CSV com os dados")
data_hora = str(dt.now())
df_avaliacoes_gpt = pd.DataFrame({"Avaliações GPT": lista_avaliacoes_gpt})
nome_csv = f'avaliacoes_gpt_{data_hora}.csv'

df_avaliacoes_gpt.to_csv(os.path.join(dataFolder, nome_csv), sep=';')
print(f"CSV salvo -> {nome_csv}")