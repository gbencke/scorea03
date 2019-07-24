import pandas as pd
import pickle
import joblib
import psycopg2
import time
from datetime import datetime

def combina_bandeira_loja(x):
    lista_cc = ['ITAUCARD 2.0 CANAIS DIRETOS','OPERACOES CREDITO CREDICARD',
                'TAM ITAUCARD 2.0','Cartao Cobranded Netshoes 2.0',
                'PL PURO ITAUCARD LOJISTA','TITULARES CARTOES DE CREDITO',
                'CARTÃO IPIRANGA 2.0']
    if x[0][0]=="F":
        return "FC"
    elif x[0] != "CC":
        return x[0]
    elif x[1] in lista_cc:
        return "CC_"+x[1]
    else:
        return "CC_OUTROS"

def ajusta_cpf(x):        
    x = str(x)
    qtd_zeros = '0'*(11-len(x))
    return qtd_zeros+str(x)

def converte_publico(x3):
    if x3=="Eleg?¡vel Exce?º?úo" or x3=="Elegível Exceção" or x3=="Elegivel Excecao" or x3=="ElegÃ­vel ExceÃ§Ã£o" or x3=="Eleg?Â¡vel Exce?Âº?Ãºo":
        return "elegivel_excecao"
    elif x3=="Alto Atrito":
        return "alto_atrito"
    elif x3=="0" or x3=="883000140672":
        return "nao_definido"
    else:
        return x3

consulta = """ SELECT DISTINCT 
            CA.texto12 as loja            
           ,CLI.cpf_cnpj as cpf_cnpj
           ,case when ca.numero2 is null then 383 else ca.numero2 end as scorecontratante           
           ,case when CA.numero7 is null then 278 else CA.numero7 end as atrasocongelado
           ,case when CA.valor1 is null then 2052 else CA.valor1 end as valorcartacampanha
           ,CA.TEXTO31 as status_boletagem           
           ,CA.valor12 as desconto
           ,CA.texto8 as bandeira
           ,CA.TEXTO25 as publico
           ,case when CA.numero14 is null then 3 else CA.numero14 end as matriz
            FROM CONTRATOS C
            INNER JOIN CA_CONTRATOS CA ON C.cod_contrato = CA.cod_pai
            INNER JOIN CLIENTES CLI ON C.cod_cliente = CLI.cod_cliente
            INNER JOIN CONVENIOS CON ON CLI.cod_convenio = CON.cod_convenio
            WHERE
             -- C.status_pagamento IN (1,2) AND
            C.devolver >= ('2019-05-29')
            AND CON.nome = 'ITAU CARTOES'
            AND CA.TEXTO5 = 'A03' """

consulta_bom_bureau = """ select distinct b.cpf_cnpj from desenv_itau_cartao.bureau_normalizado as b where b.status = 'bom' """
dadosConexao = "host='192.168.255.22' dbname='mcob_bd03' user='desenvolvimento' password='m@ndr4k3'"
dadosConexao_dw = "host='192.168.249.149' dbname='dw_poa' user='dw_loader' password='OD0FSwunQbtf'"

# Importando modelos e escaladores
xgboost_a03 = joblib.load(r'models/xgboosting_a03.pkl') 
escalador_vlrcartacampanha = joblib.load(r'models/scaler_vlrcartacampanha_a03.pkl') 
escalador_vlrdesconto = joblib.load(r'models/scaler_vlrdesconto_a03.pkl')
escalador_scorecontratante = joblib.load(r"models/scaler_scorecontratante_a03.pkl")
escalador_atrasocongelado = joblib.load(r"models/scaler_atrasocongelado_a03.pkl")

# Buscando cpfs que possuem telefone bom no bureau
conn_dw = psycopg2.connect(dadosConexao_dw)
cursor_dw = conn_dw.cursor()
cursor_dw.execute(consulta_bom_bureau)
cpfs_bom_bureau = set()
for i in cursor_dw.fetchall():
    cpfs_bom_bureau.add(i[0])

# Importando o dataset para prever
conn = psycopg2.connect(dadosConexao)
cursor = conn.cursor()
cursor.execute(consulta)
df = pd.DataFrame(cursor.fetchall())
df.columns = [desc[0] for desc in cursor.description]


df['tem_tel_no_bureau'] = df['cpf_cnpj'].apply(lambda x: 1 if x in cpfs_bom_bureau else 0)
print(df['tem_tel_no_bureau'].value_counts())

# Aplicando tratamentos
df.matriz = df.matriz.astype('object')
df.atrasocongelado = df.atrasocongelado.astype('int64')
df.cpf_cnpj = df.cpf_cnpj.astype('int64')
df.scorecontratante = df.scorecontratante.astype('int64')
df.valorcartacampanha = df.valorcartacampanha.astype('float')

# Escaladores
df['valorcartacampanha_s'] = escalador_vlrcartacampanha.fit_transform(df[['valorcartacampanha']].values)
df['desconto'] = escalador_vlrdesconto.fit_transform(df[['desconto']].values)
df['scorecontratante'] = escalador_scorecontratante.fit_transform(df[['scorecontratante']].values)
df['atrasocongelado'] = escalador_atrasocongelado.fit_transform(df[['atrasocongelado']].values)

# Mapeamento de colunas, loja e publico
df['bandeira_loja'] = df[['bandeira','loja']].apply(combina_bandeira_loja, axis=1)
df['publico'] = df['publico'].apply(converte_publico)

df = df.drop(['bandeira', 'loja'],axis=1)
df = pd.get_dummies(df)

colunas_esperadas_para_treino =  ['scorecontratante', 'atrasocongelado', 'valorcartacampanha_s', 'desconto',
       'status_boletagem_0', 'status_boletagem_BOLETAR_A_PARTIR_',
       'status_boletagem_BOLETAR_A_VONTADE', 'publico_alto_atrito',
       'publico_elegivel excecao', 'publico_nao_definido', 'matriz_1',
       'matriz_2', 'matriz_3', 'matriz_4', 'tem_tel_no_bureau_0',
       'tem_tel_no_bureau_1', 'bandeira_loja_CC_Cartao Cobranded Netshoes 2.0',
       'bandeira_loja_CC_ITAUCARD 2.0 CANAIS DIRETOS',
       'bandeira_loja_CC_OPERACOES CREDITO CREDICARD',
       'bandeira_loja_CC_OUTROS', 'bandeira_loja_CC_PL PURO ITAUCARD LOJISTA',
       'bandeira_loja_CC_TAM ITAUCARD 2.0',
       'bandeira_loja_CC_TITULARES CARTOES DE CREDITO', 'bandeira_loja_CR',
       'bandeira_loja_FC', 'bandeira_loja_HC', 'bandeira_loja_LC',
       'bandeira_loja_MA']

colunas_atuais = df.columns
for col in colunas_esperadas_para_treino:
    if col not in colunas_atuais:
        df[col] = 0

X_test = df[colunas_esperadas_para_treino].values

#Prevendo
pred = xgboost_a03.predict_proba(X_test)
df['prob prevista'] = pred[:,1]

# Ajustando cpf
df['cpf_cnpj'] = df['cpf_cnpj'].astype('object')
df['cpf_cnpj'] = df['cpf_cnpj'].apply(ajusta_cpf)

# Ajustando prob
df['prob prevista'] = df['prob prevista'].round(2)
df['prob prevista'] = df['prob prevista'].astype('object')

# Ajustando valor cartacampanha
df['valorcartacampanha'] = df['valorcartacampanha'].astype('object')

# Conectando e inserindo no dw
conn_dw = psycopg2.connect(dadosConexao_dw)
cursor_dw = conn_dw.cursor()
cursor_dw.execute('TRUNCATE TABLE desenv_itau_cartao.score_a03')

cpf_prob = list(zip(df.cpf_cnpj.values,df['prob prevista'].values,df['valorcartacampanha'].values))

start_time = time.time()

for i in cpf_prob:
    cursor_dw.execute("INSERT INTO desenv_itau_cartao.score_a03 values ('"+i[0]+"','"+str(i[1])+"','"+str(i[2])+"')")
    conn_dw.commit()

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print(current_time + " - Insert execution time: " + str((time.time() - start_time)) + ' ms')