import pandas as pd
import pickle
import joblib
import psycopg2
import time

def converte_loja(x):
    lojas = {'1788': 'VIVO ITAUCARD',
'17088': 'ITAUCARD 2.0 CLÁSSICO VAREJO',
'17091': 'TAM ITAUCARD 2.0',
'17092': 'TIM ITAUCARD 2.0',
'17093': 'VIVO ITAUCARD 2.0',
'17094': 'FIAT ITAUCARD 2.0',
'17095': 'FORD ITAUCARD 2.0',
'17096': 'VOLKSWAGEN ITAUCARD 2.0',
'17097': 'MITSUBISHI ITAUCARD 2.0',
'17098': 'LIVRARIA CULTURA ITAUCARD 2.0',
'17099': 'CARTÃO WALMART 2.0',
'17100': 'CARTÃO MARISA 2.0',
'17101': 'CARTÃO IPIRANGA 2.0',
'17102': 'Cartão Itaucard Lojistas 2.0',
'17103': 'CARTÃO EXTRA 2.0',
'17104': 'CARTÃO PÃO DE ACUCAR 2.0',
'17105': 'CARTÃO PONTO FRIO 2.0',
'17120': 'CREDICARD CLASSICOS',
'17121': 'CREDICARD UNIVERSITARIO',
'17122': 'AVON CREDICARD',
'17123': 'CASHBACK CREDICARD',
'17124': 'EXCLUSIVE CREDICARD',
'17125': 'DSUPER CREDICARD',
'17126': 'AYRTON SENNA ITAUCARD',
'17127': 'BRASTEMP ITAUCARD',
'17128': 'EMOCOES CREDICARD',
'17129': 'MUFFATO CREDICARD',
'17130': 'SUL AMERICA ITAUCARD',
'17131': 'WALMART 2.0 HIPER NACIONAL',
'18006': 'ITAUCARD PROGRAMADO VISA',
'18007': 'ITAUCARD PROGRAMADO MASTERCARD',
'18008': 'NOVO UNICARD',
'18009': 'BLACK INFINITY ',
'18010': 'FORD ITAUCARD ',
'18011': 'FIAT ITAUCARD ',
'18012': 'CARTãO FIAT ITAUCARD',
'18013': 'CARTãO MITSUBISHI ITAUCARD',
'18014': 'LIVRARIA CULTURA',
'18016': 'HIPERCARD',
'18021': 'CARTÃO VIVA! UNIMED ITAUCARD',
'18023': 'CARTÃO UNITED ITAUCARD',
'18024': 'CARTÃO AUDI ITAUCARD',
'18028': 'ITAUCARD HIPER 2.0',
'18030': 'ITAUCARD 2.0 CANAIS DIRETOS',
'29001': 'CARTAO PRIV LBL FIC COMPRE BEM',
'29002': 'CARTAO PRIV LBL FIC EXTRA',
'29003': 'CARTAO PRIV LBL FIC PAOACUCAR',
'29004': 'CARTAO PRIV FIC LBL SENDAS',
'29005': 'CARTAO COBRAND FIC COMPREBEM ',
'29006': 'CARTAO COBRAND FIC EXTRA',
'29007': 'CARTAO COBRAND FIC PAO ACUCAR',
'29008': 'CARTAO COBRAND FIC SENDA',
'29009': 'CARTAO PRIV LBL PACR ABN REAL',
'29010': 'CL-CART PRIV LBL PACR ABN REAL',
'29011': 'CAR COBRAND FIC P ACUC CREDICA',
'29012': 'CARTÃO PRIV LBL FIC ASSAI',
'29015': 'CARTAO PL FIC COMPREBEM BAND',
'29016': 'CARTAO PL FIC EXTRA BAND',
'29017': 'CARTAO PL FIC PAO ACUCAR BAND',
'29018': 'CARTAO PL FIC SENDAS BAND',
'29021': 'CARTAO PL FIC CB S/P',
'29022': 'CARTAO PL FIC EXTRA S/P',
'29023': 'CARTAO PL FIC PAO ACUCAR S/P',
'29024': 'CARTAO PL FIC SENDAS S/P',
'29025': 'FLEX ITAUCARD LOJISTA',
'29026': 'PL PURO ITAUCARD LOJISTA',
'29027': 'MAGAZINE LUIZA/LUIZACRED FLEX',
'29135': 'CREDICOMP FIC COBRANDED PFGP',
'29137': 'COMPJUR FIC CARTAO',
'29163': 'FIC - EP CARTAO REFIN',
'29187': 'FIC - COMPOSICAO DE DIVIDA',
'29224': 'FIC - EP CARTAO',
'29231': 'REVOLVING EP CARTAO FIC',
'29235': 'CRED. CAMPANHA EP CARTAO FIC',
'29275': 'CDC FIC AQUISIÇÃO/TOMBAMENTO',
'29283': 'RENEGOCIACAO CDC FIC TOMB/AQUI',
'29381': 'CREDITO DIRETO CONSUMIDOR FIC',
'29382': 'RENEGOCIACAO CDC FIC',
'29541': 'FIC EP CHEQUE ESCOB',
'29542': 'FIC - CREDICOMP EP CHEQUE',
'29588': 'RENEGOC CARTAO FIC EM ATRASO',
'29701': 'CDC ELETR CORRENTISTA ORBITAL',
'29702': 'CDC ELETR NAO CORRENT ORBITAL',
'29801': 'CARTAO PRIVATE LABEL - FAI',
'29802': 'CARTAO COBRANDED FAI',
'29803': 'CARTãO PRIVATE LABEL SHOPTIME',
'29811': 'FAI PL EMBANDEIRADO AMERICANAS',
'29812': 'CARTAO PL EMBANDEIRADO MARISA',
'29821': 'FAI PL AMERICANAS SEM PARCELAD',
'29828': 'CDC FIC RISC GPA',
'29840': 'FIC - EP CHEQUE',
'29856': 'FIC-CAMPANHA EPCHEQUE CREDCOMP',
'29926': 'FIC EP CARTAO REFIN',
'29928': 'FIC EP CARTAO CREDICOMP',
'29972': 'FIC - CREDICOMP 30 EP CHEQUE',
'29992': 'COMPJUR FIC',
'30048': 'RENEGOC CARTAO ITAUCARD EM DIA',
'30082': 'CREDICOMP ITAUCARD PF GP',
'30139': 'CREDICOMP UNICO CARTAO ITAUCAR',
'30539': 'RENEGOC CARTAO ITAUCARD ATRASO',
'30618': 'COMPJUR ITAUCARD PFGP',
'30837': 'GIROPOS GAR.REAL FINAL',
'42028': 'EP GLOBEX PF',
'42029': 'CDC GLOBEX PF',
'42030': 'CARTãO EMBANDEIRADO GLOBEX PF',
'42033': 'CDC BENS MAQ E EQUIP',
'42036': 'CDC SFC PF CARNÊ',
'42037': 'CDC SFC PF CHEQUE',
'42038': 'EP SFC PF CHEQUE',
'42040': 'CREDICOMP ACC MIG UBB',
'42041': 'CREDICOMP GLOBEX CDC MIG UBB',
'42042': 'CDC PF BACKCRED MIG UBB',
'42043': 'EP PF BACKCRED MIG UBB',
'42045': 'CREDICOMP GLOBEX EP MIG UBB',
'42065': 'CREDICOMP UNICO COBRANDED FAI',
'42066': 'CREDICOMP UNICO EP - FAI',
'42067': 'CREDICOMP UNICO EP - FIC',
'42068': 'CREDICOMP UNICO EP - FIC',
'42069': 'CREDICOMP UNICO COBRANDED FIC',
'42070': 'CREDICOMP UNICO EP - FIT',
'42071': 'CREDICOMP UNICO - HIPERCARD',
'42072': 'CREDICOMP UNICO CDC - LOJISTA',
'42080': 'CREDICOMP UNICO PL - FIC',
'42081': 'COMPJUR PL FIC',
'42085': 'CREDICOMP UNICO PL - FAI',
'42086': 'COMPJUR PL FAI',
'42087': 'CREDICOMP UNI PL PURO  LOJISTA',
'42089': 'COMPJUR PL PURO LOJISTA',
'42090': 'CREDICOMP UNICO FLEX  LOJISTA',
'42091': 'COMPJUR FLEX LOJISTA',
'42092': 'CREDICOMP UNICO PL MARISA',
'42093': 'COMPJUR PL MARISA',
'42094': 'CREDICOMP UNICO  LUIZACRED',
'42095': 'COMPJUR LUIZACRED',
'42097': 'REFIN UNICO PL ATRASO - FIC',
'42115': 'REFIN UNICO ATRASO - HIPERCARD',
'42116': 'REFIN UNICO EM DIA - HIPERCARD',
'42117': 'COMPJUR ITAUCARD ACC',
'42147': 'CREDICOMP UNICO IPIRANGA',
'42148': 'COMPJUR IPIRANGA',
'42150': 'COMP UNI CARTAO CANAIS DIRETOS',
'42208': 'CREDCOMP CARTãO PONTO FRIO2.0',
'42209': 'COMPJUR CARTãO PONTO FRIO 2.0',
'42210': 'CREDCOMP WALMART HIPER/SAMS2.0',
'42211': 'COMPJUR WALMART HIPER/SAMS2.0',
'42212': 'CREDCOMP CARTãO IPIRANGA 2.0',
'42213': 'COMPJUR CARTãO IPIRANGA 2.0',
'42214': 'CREDICOMP  CARTAO MARISA 2.0',
'42215': 'COMPJUR CARTAO MARISA 2.0',
'42216': 'CREDICOMP ITAUC 2.0  PARCERIAS',
'42217': 'COMPJUR ITAUC 2.0  PARCERIAS',
'42223': 'CREDICOMP EXTRA/ PAO ACUCAR 2.0',
'42224': 'COMPJUR EXTRA/ PAO ACUCAR 2.0',
'42225': 'CREDICOMP A. SENNA ITAUCARD',
'42226': 'COMPJUR A. SENNA ITAUCARD',
'42227': 'CREDICOMP BRASTEMP ITAUCARD',
'42228': 'COMPJUR BRASTEMP ITAUCARD',
'42229': 'CREDICOMP EMOCOES CREDICARD',
'42230': 'COMPJUR EMOCOES CREDICARD',
'42231': 'CREDICOMP SULAMERICA ITAUCARD',
'42232': 'COMPJUR SUL AMERICA ITAUCARD',
'42233': 'CREDICOMP CREDICARD CLASSICOS',
'42234': 'COMPJUR CREDICARD CLASSICOS',
'42235': 'CREDICOMP CREDICARD UNIVERSIT.',
'42236': 'COMPJUR CREDICARD UNIVERSIT.',
'42237': 'CREDICOMP AVON CREDICARD',
'42238': 'COMPJUR AVON CREDICARD',
'42239': 'CREDICOMP CASHBACK CREDICARD',
'42240': 'COMPJUR CASHBACK CREDICARD',
'42241': 'CREDICOMP EXCLUSIVE CREDICARD',
'42242': 'COMPJUR EXCLUSIVE CREDICARD',
'42243': 'CREDICOMP DSUPER CREDICARD',
'42244': 'COMPJUR DSUPER CREDICARD',
'42292': 'CDC SEM GARANTIA (CREDICARD)',
'42293': 'EMPRESTIMO PESSOAL (CREDICARD)',
'42294': 'EP RENEGOCIACAO (CREDICARD)',
'42298': 'CONSIGNADO TOMBADO EP',
'42305': 'CREDICOMP EP-CDC CREDICARD',
'42306': 'COMPJUR EP-CDC CREDICARD',
'42307': 'CREDICOMP EP-RENEGCP CREDICARD',
'42308': 'COMPJUR EP-RENEG CP CREDICARD',
'42309': 'CREDICOMP EP -  CREDICARD',
'42310': 'COMPJUR EP - CREDICARD',
'42322': 'CREDICOMP INTER. CREDICARD',
'42323': 'COMPJUR INTER.  CREDICARD',
'42340': 'Credicomp TAM',
'42346': 'Credicomp TudoAzul',
'42352': 'Credicomp TIM',
'42358': 'Credicomp Fiat',
'42364': 'Credicomp TAM 2.0',
'42370': 'Credicomp TudoAzul 2.0',
'42372': 'COMPJUR HIPERCARD',
'42377': 'Credicomp TIM 2.0',
'42383': 'Credicomp Fiat 2.0',
'42398': 'Credicomp Netshoes 1.0',
'42399': 'Credicomp Netshoes 2.0',
'42498': 'CREDICOMP HIPERCARD',
'42499': 'COMPJUR HIPERCARD',
'48548': 'Cartao Cobranded Netshoes 1.0',
'48549': 'Cartao Cobranded Netshoes 2.0',
'52008': 'ITAUCARD NET MASTERCARD',
'52056': 'ITAUCARD EX GM',
'52064': 'UNITED EX BKB',
'52522': 'CARTãO TAM ITAUCARD',
'52523': 'EMBANDEIRADO MARISA',
'55041': 'TAII - REFIN',
'55042': 'TAII - CREDICOMP',
'55043': 'FIT - REVOLVING EP CARTAO',
'55044': 'FIT - EP CARTAO',
'55101': 'FIT - CREDICOMP BOLETO',
'55136': 'CREDICOMP FAI COBRANDED PFGP',
'55138': 'COMPJUR FAI CARTAO',
'55152': 'FAI - REFIN',
'55159': 'FAI-CREDCOMP-CARTAO',
'55219': 'FAI CAMPANHA EPCHEQUE CREDCOMP',
'55220': 'FAI - EP CARTAO',
'55222': 'FAI-REVOLVING EP CARTAO',
'55270': 'RENEGOC CARTAO FAI EM DIA',
'55374': 'RENEGOC CARTAO FAI EM ATRASO',
'55527': 'EP CHEQUE-CREDICOMP ESCRITORIO',
'55528': 'FIT - CREDICOMP ESCRITORIO',
'55539': 'FINANC CDC CH CREDICOMP ESCOB',
'55540': 'FINANC CDC CHEQUE CREDICOMP',
'55603': 'COMPJUR FIT',
'55800': 'FAI - EP CHEQUE',
'55821': 'FAI - EP CHEQUE CREDICOMP 90',
'55822': 'FIT - CDC CHEQUE',
'55827': 'CDC CHEQUE SHOPTIME',
'55830': 'EP CHEQUE FIT',
'55837': 'FIT EP CHEQUE CREDICOMP ESCOB',
'55838': 'FIT EP CHEQUE CREDICOMP 90',
'55905': 'FIT EP CARTAO REFIN',
'55906': 'FIT EP CARTAO CREDICOMP',
'55929': 'FAI EP CARTAO REFIN',
'55930': 'FAI EP CARTAO CREDICOMP',
'55944': 'COMPJUR FAI',
'55971': 'FAI CREDICOMP30 EP CHEQUE',
'55988': 'FIT EP CHEQUE CREDICOMP 30',
'55991': 'CRED. CAMPANHA EP CARTAO FAI',
'90452': 'SOE-CESSAO ITAUCARD',
'90453': 'SOE-CESSÃO CREDICARCARD',
'90719': 'CESSAO CARTãO UBB',
'90741': 'CESSãO FINANCEIRAS UBB',
'98040': 'TITULARES CARTOES DE CREDITO',
'98102': 'Black Uniclass/ NCC',
'98105': 'TudoAzul Convencional',
'98118': 'Fiat Convencional',
'98160': 'TAM Itaucard Convencional',
'98161': 'TIM Convencional',
'98163': 'TAM Itaucard Black',
'99056': 'OPERACOES CREDITO CREDICARD',
}
    try:
        loja_convertido = lojas[x]
    except:
        loja_convertido = "OUTROS"
    return loja_convertido


def trata_loja(x2):
    lojas_agrup = ['CARTÃO EXTRA 2.0',
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
    if x2 in lojas_agrup:
        return x2
    else:
        return 'OUTROS'

def ajusta_cpf(x):        
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
            CA.texto12 as loja,
            '' as renda
           ,CLI.cpf_cnpj as cpf_cnpj
           ,case when ca.numero2 is null then 383 else ca.numero2 end as scorecontratante
           ,CA.data2 as dataentrada
           ,C.devolver as validadecampanha
           ,case when CA.numero7 is null then 278 else CA.numero7 end as atrasocongelado
           ,case when CA.valor1 is null then 2052 else CA.valor1 end as valorcartacampanha
           ,case when CA.valor10 is null then 3197 else CA.valor10 end as vlclusters
           ,CA.TEXTO31 as status_boletagem
           ,CA.TEXTO37 as data_status_boletagem
           ,CA.valor12 as desconto
           ,CA.texto8 as bandeira
           ,CA.TEXTO25 as publico
           ,case when CA.numero14 is null then 3 else CA.numero14 end as matriz
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

dadosConexao = "host='192.168.255.22' dbname='mcob_bd03' user='desenvolvimento' password='m@ndr4k3'"

# Importando modelos e escaladores
xgboost_a03 = joblib.load(r'models\xgboosting_a03.pkl') 
escalador_vlrcartacampanha = joblib.load(r'models\scaler_vlrcartacampanha_a03.pkl') 
escalador_vlrccluster = joblib.load(r'models\scaler_vlrcluster_a03.pkl')
escalador_vlrdesconto = joblib.load(r'models\scaler_vlrdesconto_a03.pkl')

# Importando o dataset para prever (por enquanto csv, depois conectar em banco)
#df = pd.read_csv(r"data\prever_novo.csv", encoding="latin1", delimiter=";", decimal = ",")
conn = psycopg2.connect(dadosConexao)
cursor = conn.cursor()
cursor.execute(consulta)
df = pd.DataFrame(cursor.fetchall())
df.columns = [desc[0] for desc in cursor.description]

# Aplicando tratamentos

# Escaladores
df['valorcartacampanha_s'] = escalador_vlrcartacampanha.fit_transform(df[['valorcartacampanha']].values)
df['vlclusters_s'] = escalador_vlrccluster.fit_transform(df[['vlclusters']].values)
df['desconto_s'] = escalador_vlrdesconto.fit_transform(df[['desconto']].values)

# Mapeamento de colunas, loja e publico
df['loja'] = df['loja'].apply(converte_loja)
df['loja'] = df['loja'].apply(trata_loja)
df['publico'] = df['publico'].apply(converte_publico)
df.atrasocongelado = df.atrasocongelado.astype('int64')
df.matriz = df.matriz.astype('int64')
df.scorecontratante = df.scorecontratante.astype('int64')

# Selecionando colunas para predicao
colunas_treino = ['loja',
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
       'loja_CARTAO PL EMBANDEIRADO MARISA', 'loja_CARTAO PL FIC CB S/P',
       'loja_CARTAO PL FIC EXTRA BAND', 'loja_CARTAO PL FIC EXTRA S/P',
       'loja_CREDICARD CLASSICOS', 'loja_HIPERCARD',
       'loja_ITAUCARD 2.0 CANAIS DIRETOS',
       'loja_MAGAZINE LUIZA/LUIZACRED FLEX',
       'loja_OPERACOES CREDITO CREDICARD', 'loja_OUTROS',
       'loja_TAM ITAUCARD 2.0', 'status_boletagem_0',
       'status_boletagem_BOLETAR_A_PARTIR_',
       'status_boletagem_BOLETAR_A_VONTADE', 'bandeira_CC', 'bandeira_CR',
       'bandeira_FA', 'bandeira_FC', 'bandeira_FT', 'bandeira_HC',
       'bandeira_LC', 'bandeira_MA', 'publico_alto_atrito',
       'publico_elegivel_excecao', 'publico_nao_definido']

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

# Conectando e inserindo no dw
dadosConexao_dw = "host='192.168.249.149' dbname='dw_poa' user='dw_loader' password='OD0FSwunQbtf'"
conn_dw = psycopg2.connect(dadosConexao_dw)
cursor_dw = conn_dw.cursor()
cursor_dw.execute('TRUNCATE TABLE desenv_itau_cartao.score_a03')

cpf_prob = list(zip(df.cpf_cnpj.values,df['prob prevista'].values))

start_time = time.time()

for i in cpf_prob:
    cursor_dw.execute("INSERT INTO desenv_itau_cartao.score_a03 values ('"+i[0]+"','"+str(i[1])+"')")
    conn_dw.commit()

print("Insert execution time: " + str((time.time() - start_time)) + ' ms')