# PIPELINE DE TRANSFORMAÇÃO DOS DADOS
import os
import pandas as pd
import numpy as np
from datetime import date as dt

print("Iniciando o pipeline")
# Definindo as pastas do diretório
root = os.getcwd()
dataFolder = os.path.join(root, 'data')

dataPath = os.path.join(dataFolder, 'churn_com_texto.csv')
dados = pd.read_csv(dataPath)
print("Dados brutos importados")

# Criando a coluna de última data de contrato ativo
# Para aqueles que possuem a Data de término de contrato, sera essa data, para os que tiverem valores vazios será a data atual
dados['Última data de contrato ativo'] = dados['Data de Término do Contrato'].fillna(dt.today())

dados['Houve término de contrato'] = dados['Data de Término do Contrato'].notnull()
dados['Data de Início do Contrato'] = pd.to_datetime(dados['Data de Início do Contrato'])
dados['Data de Término do Contrato'] = pd.to_datetime(dados['Data de Término do Contrato'])
dados['Última data de contrato ativo'] = pd.to_datetime(dados['Última data de contrato ativo'])
dados['Tempo ativo de contrato - dias'] = (dados['Última data de contrato ativo'] - dados['Data de Início do Contrato'])
print("Colunas de datas tratadas")

# Corrigindo a variaçãoes incorretas da localização
dados['Localização'] = dados['Localização'].replace('Rio de Janeiro/RJ/Rio Grande do Sul', 'Rio de Janeiro/RJ/Rio de Janeiro')
dados['Localização'] = dados['Localização'].replace('Brasília/DF', 'Brasília/DF/Distrito Federal')

# Criando as variáveis Cidade, UF e Estado
lista_cidades = []
lista_uf = []
lista_estados = []


for i in range(0, dados.shape[0]):
    lista_cidades.append(dados['Localização'].str.split('/', n=3)[i][0])
    lista_uf.append(dados['Localização'].str.split('/', n=3)[i][1])
    lista_estados.append(dados['Localização'].str.split('/', n=3)[i][2])

dados['Cidade'] = lista_cidades
dados['UF'] = lista_uf
dados['Estado'] = lista_estados
print("Localização tratada e colunas de Cidade, UF e Estado criadas")

# Corrigindo as colunas "Voluma de dados", "Número de reclamações" e "Comentário"
dados['Comentários'][69:] = dados['Número de Reclamações'][69:]
dados['Número de Reclamações - Split'] = dados['Número de Reclamações'][:69]
dados['Classificação Comentário'] = dados['Volume de Dados'][69:]
dados['Volume de Dados - Split'] = dados['Volume de Dados'][:69]
print("Correção dos dados contidos nas colunas: Volume de dados, Número de reclamações e Comentários")

# Retira os dados preenchidos com '-' e coloca uma linha vazia
dados['Num Reclamações'] = dados['Número de Reclamações - Split'].replace("-", np.nan).astype('float')
# Retira os dados preenchidos com '-' e coloca uma linha vazia, depois retira o valor GB deixando apenas o valor numérico
dados['Volume de Dados - Split'] = dados['Volume de Dados - Split'].replace("-", np.nan)
dados['Volume de Dados - GB'] = dados['Volume de Dados - Split'].str.removesuffix(' GB').astype('float')
print("Persiste Número de reclamações e volume de dados como variáveis numéricas e não textuais")

# Filtrando apenas as de interesse
dados_tratados = dados.drop(columns=['ID', 'Localização', 'Volume de Dados', 'Número de Reclamações', 'Número de Reclamações - Split', 'Volume de Dados - Split'])
print("Dataframe intermediário tratado construído")

# Lidando com a duração média das chamadas e volume de dados
dados_tratados['Duração Média das Chamadas'] = dados_tratados['Duração Média das Chamadas'].fillna(0)
dados_tratados['Volume de Dados - GB'] = dados_tratados['Volume de Dados - GB'].fillna(0)

# Lidando com a coluna de número de reclamações
dados_tratados.loc[dados_tratados['Classificação Comentário'].notnull() & dados_tratados['Num Reclamações'].isnull(), 'Num Reclamações'] = 1
dados_tratados['Num Reclamações'] = dados_tratados['Num Reclamações'].fillna(0)

# Trazendo as classificações dos comentários criados pelo GPT3
pathCsvGpt = os.path.join(dataFolder, 'avaliacoes_gpt.csv')
lista_avaliacoes_gpt = pd.read_csv(pathCsvGpt, sep=';')
dados_tratados['Avaliações - GPT'] = lista_avaliacoes_gpt

padrao_avaliacoes_comentarios = {
    'Classificação: Extremamente Negativa': 'Negativa',
    'Extremamente Negativa.': 'Negativa',
    'Extremamente Negativa': 'Negativa',
    'Negativa.': 'Negativa',
    'Extremamente Positiva.': 'Positiva',
    'Extremamente Positiva': 'Positiva'
}

dados_tratados['Avaliações - GPT'] = dados_tratados['Avaliações - GPT'].replace(padrao_avaliacoes_comentarios)
dados_tratados = dados_tratados.drop(columns=['Classificação Comentário'])
print("Valores nulos corrigidos")

print("Salvando os dados tratados em CSV")
dados_tratados.to_csv(os.path.join(dataFolder, 'dados_tratados.csv'), sep=';', index=False)
print("Processo finalizado")