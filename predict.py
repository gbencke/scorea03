import pandas as pd
import pickle
import joblib

def trata_loja(x):
    lojas = ['CARTÃO EXTRA 2.0',
             'CARTÃO MARISA 2.0',
             'CARTAO PL EMBANDEIRADO MARISA',
             'CARTAO PL FIC CB S/P',
             'CARTAO PL FIC EXTRA BAND',
             'CARTAO PL FIC EXTRA S/P',
             'CARTÃO PONTO FRIO 2.0',
             'CARTÃO PRIV LBL FIC ASSAI',
             'CARTÃO WALMART 2.0',
             'CREDICARD CLASSICOS',
             'HIPERCARD',
             'ITAUCARD 2.0 CANAIS DIRETOS',
             'MAGAZINE LUIZA/LUIZACRED FLEX',
             'OPERACOES CREDITO CREDICARD',
             'OUTROS',
             'TAM ITAUCARD 2.0']
    if x in lojas:
        return x
    else:
        return 'OUTROS'

def converte_publico(x):
    if x=="Eleg?¡vel Exce?º?úo" or x=="Elegível Exceção" or x=="Elegivel Excecao" or x=="ElegÃ­vel ExceÃ§Ã£o" or x=="Eleg?Â¡vel Exce?Âº?Ãºo":
        return "elegivel_excecao"
    elif x=="Alto Atrito":
        return "alto_atrito"
    elif x=="0" or x=="883000140672":
        return "nao_definido"
    else:
        return x    

consulta = """ SELECT DISTINCT 
            '' as renda
           ,CLI.cpf_cnpj
           ,ca.numero2 as scorecontratante
           ,CA.data2 as dataentrada
           ,C.devolver as validadecampanha
           ,CA.numero7 as atrasocongelado
           ,CA.valor1 as valorcartacampanha
           ,CA.valor10 as vlcusters
           ,CA.TEXTO31 as status_boletagem
           ,CA.TEXTO37 as data_status_boletagem
           ,CA.valor12 as desconto
           ,CA.texto8 as bandeira
           ,CA.TEXTO25 as publico
           ,CA.numero14 as matriz
           ,'' as Pagamentos
           ,'' as Acionamentos
           ,'' as Valor_Pago
            FROM CONTRATOS C
            INNER JOIN DADOS_PESSOAIS AS D ON C.cod_cliente = D.cod_cliente
            INNER JOIN CA_CONTRATOS CA ON C.cod_contrato = CA.cod_pai
            INNER JOIN CLIENTES CLI ON C.cod_cliente = CLI.cod_cliente
            INNER JOIN CONVENIOS CON ON CLI.cod_convenio = CON.cod_convenio
            INNER JOIN TIPOS_STATUS TS ON C.cod_ts = TS.cod_ts
            WHERE C.status_pagamento IN (1,2)
            AND C.devolver >= CURRENT_DATE+1
            AND CON.nome = 'ITAU CARTOES'
            AND CA.TEXTO5 = 'A03' """

string_conexao = "192.168.255.22,desenvolvimento,m@ndr4k3,mcob_bd03"
    
# Importando modelos e escaladores
xgboost_a03 = joblib.load(r'models\xgboosting_a03.pkl') 
escalador_vlrcartacampanha = joblib.load(r'models\scaler_vlrcartacampanha_a03.pkl') 
escalador_vlrccluster = joblib.load(r'models\scaler_vlrcluster_a03.pkl')
escalador_vlrdesconto = joblib.load(r'models\scaler_vlrdesconto_a03.pkl')

# Importando o dataset para prever (por enquanto csv, depois conectar em banco)
df = pd.read_csv(r"data\prever_novo.csv", encoding="latin1", delimiter=";", decimal = ",")
## CUIDAR NULOS!! ###
print(df.isna().sum())

# Aplicando tratamentos

# Escaladores
df['valorcartacampanha_s'] = escalador_vlrcartacampanha.fit_transform(df[['valorcartacampanha']].values)
df['vlclusters_s'] = escalador_vlrccluster.fit_transform(df[['vlclusters']].values)
df['desconto_s'] = escalador_vlrdesconto.fit_transform(df[['desconto']].values)

# Mapeamento de colunas, loja e publico
df['LOJA'] = df['LOJA'].apply(trata_loja)
df['publico'] = df['publico'].apply(converte_publico)

# Selecionando colunas para predicao
colunas_treino = ['LOJA',
                  'renda',
                  'scorecontratante',
                  'atrasocongelado',
                  'valorcartacampanha_s',
                  'vlclusters_s',
                  'status_boletagem',
                  'desconto_s',
                  'bandeira',
                  'publico',
                  'matriz'
                 ]

dummies = pd.get_dummies(df[colunas_treino])
dropado = df.drop(colunas_treino,axis=1)
df = pd.concat([dummies,dropado], axis=1)


colunas_esperadas_para_treino =  ['renda', 'scorecontratante', 'atrasocongelado', 'valorcartacampanha_s',
       'vlclusters_s', 'desconto_s', 'matriz',
       'LOJA_CARTAO PL EMBANDEIRADO MARISA', 'LOJA_CARTAO PL FIC CB S/P',
       'LOJA_CARTAO PL FIC EXTRA BAND', 'LOJA_CARTAO PL FIC EXTRA S/P',
       'LOJA_CREDICARD CLASSICOS', 'LOJA_HIPERCARD',
       'LOJA_ITAUCARD 2.0 CANAIS DIRETOS',
       'LOJA_MAGAZINE LUIZA/LUIZACRED FLEX',
       'LOJA_OPERACOES CREDITO CREDICARD', 'LOJA_OUTROS',
       'LOJA_TAM ITAUCARD 2.0', 'status_boletagem_0',
       'status_boletagem_BOLETAR_A_PARTIR_',
       'status_boletagem_BOLETAR_A_VONTADE', 'bandeira_CC', 'bandeira_CR',
       'bandeira_FA', 'bandeira_FC', 'bandeira_FT', 'bandeira_HC',
       'bandeira_LC', 'bandeira_MA', 'publico_alto_atrito',
       'publico_elegivel_excecao', 'publico_nao_definido']

colunas_atuais = dummies.columns
for col in colunas_esperadas_para_treino:
    if col not in colunas_atuais:
        df[col] = 0        

X_test = df[dummies.columns].values

#Prevendo
pred = xgboost_a03.predict_proba(X_test)

# Exportando os dados (por enquanto printando os 5 primeiros)
df['prob prevista'] = pred[:,1]
print(df[['cpf_cnpj','prob prevista']].head())