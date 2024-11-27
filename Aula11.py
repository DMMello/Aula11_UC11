import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Variáveis de Conexão com o banco de dados MySQL
host = 'localhost'
user = 'root'
password = 'root'
database = 'db_aula11'

# Estabelecendo a conexão com o banco de dados
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

# Usando uma consulta SQL para ler todos os registros da tabela 'basedp'
df_basedp = pd.read_sql('SELECT * FROM basedp', engine)
# print(df_basedp.head())  # Exibe as primeiras linhas do dataframe

print(30 * '-')

# Usando uma consulta SQL para ler todos os registros da tabela 'roubo_celular'
df_base_roubo = pd.read_sql('SELECT * FROM roubo_celular', engine)
# print(df_base_roubo.head())  # Exibe as primeiras linhas do dataframe

# Realizando a junção (merge) entre os dois dataframes com base na coluna 'cod_ocorrencia'
df_base_total = pd.merge(df_basedp, df_base_roubo, on='cod_ocorrencia', how='inner')

# Filtrando os dados para o ano de 2022 a 2023
df_base_total = df_base_total[(df_base_total['ano'] >= 2022) & (df_base_total['ano'] <= 2023)]

# Agrupando os dados por 'aisp' e somando os valores da coluna 'roubo_celular'
df_base_total = df_base_total.groupby('aisp').agg({
    'roubo_celular': 'sum'  # Somando os roubos de celular por 'aisp'
}).reset_index()

# Exibe o resultado da agregação
# print(df_base_total.head())

try:
    # Convertendo a coluna 'roubo_celular' para um array numpy para realizar as análises
    array_roubo = np.array(df_base_total['roubo_celular'])

    # Calculando a média e a mediana
    media = np.mean(array_roubo)
    mediana = np.median(array_roubo)

    # Calculando a distância entre a média e a mediana (como proporção da mediana)
    distancia = abs((media - mediana) / mediana)

    # Calculando os quartis (Q1, Q2, Q3) usando o método Weibull
    q1 = np.quantile(array_roubo, 0.25, method='weibull')
    q2 = np.quantile(array_roubo, 0.50, method='weibull')
    q3 = np.quantile(array_roubo, 0.75, method='weibull')

    # Exibindo as medidas de tendência central
    print('\nMEDIDAS: ')
    print(30 * '-')
    print('Média: ', media)
    print('Mediana: ', mediana)
    print('Distância entre média e mediana: ', distancia)

    # Exibindo as medidas de posição (quartis)
    print('\nMEDIDAS DE POSIÇÃO: ')
    print(30 * '-')
    print('Q1 (25%): ', q1)
    print('Q2 (50%): ', q2)
    print('Q3 (75%): ', q3)

    # Calculando o IQR (Intervalo Interquartil)
    iqr = q3 - q1

    # Definindo os limites superior e inferior
    limite_inferior = q1 - 1.5 * iqr
    limite_superior = q3 + 1.5 * iqr

    # Identificando outliers inferiores
    outliers_inferiores = df_base_total[df_base_total['roubo_celular'] < limite_inferior]

    # Identificando outliers superiores
    outliers_superiores = df_base_total[df_base_total['roubo_celular'] > limite_superior]

except Exception as e:
    print(f'Erro ao calcular as métricas: {e}')
    exit()
