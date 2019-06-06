import pandas as pd
import pickle
import joblib

def converte_publico(x):
    if x=="Eleg?¡vel Exce?º?úo" or x=="Elegível Exceção" or x=="Elegivel Excecao":
        return "elegivel_excecao"
    elif x=="Alto Atrito":
        return "alto_atrito"
    else:
        return x


xgboost_a03 = joblib.load(r'models\xgboosting_a03.pkl') 
escalador_vlrcartacampanha = joblib.load(r'models\scaler_vlrcartacampanha_a03.pkl') 
escalador_vlrccluster = joblib.load(r'models\scaler_vlrcluster_a03.pkl')
escalador_vlrdesconto = joblib.load(r'models\scaler_vlrdesconto_a03.pkl')

df = pd.read_csv("data\prever.csv", encoding="latin1", delimiter=";", decimal = ",")
## CUIDAR NULOS!! ###
print(df.isna().sum())

df[['valorcartacampanha']] = escalador_vlrcartacampanha.fit_transform(df[['valorcartacampanha']].values)
df[['vlclusters']] = escalador_vlrccluster.fit_transform(df[['vlclusters']].values)
df[['desconto']] = escalador_vlrdesconto.fit_transform(df[['desconto']].values)

df.pop('data_status_boletagem')
df.pop('LOJA')
df.pop('validadecampanha')
df.pop('dataentrada')

df['publico'] = df['publico'].apply(converte_publico)

df_dummies = pd.get_dummies(df)
y_test = df_dummies.pop('Pagamentos').values # Quando for a base real retirar essa linha
X_test = df_dummies.values

pred = xgboost_a03.predict_proba(X_test)
df_dummies['prob prevista'] = pred[:,1]
df_dummies.to_csv(r'data\xgboost_predicted.csv', decimal=',', sep=";")