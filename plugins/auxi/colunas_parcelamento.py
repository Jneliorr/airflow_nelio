import pandas as pd
import numpy as np
from datetime import datetime


coluna_dici_0 = [
'Tipo_de_Registro', #01	01	01	Numérico	Valor fixo “0”
'Sistema', # Sistema	03	08	06	Alfa	Valor fixo “PARCSN”
'Data_Inicial', # 10	17	08	YYYYMMDD	Os dados extraídos são os que sofreram alterações a partir dessa data
'Hora_Inicial', # 19	22	04	HHMM	Hora inicial associada à data inicial
'Data_Final', # 24	31	08	YYYYMMDD	Os dados extraídos são os que sofreram alterações  até essa data (data do início da extração)
'Hora_Final' # 33	36	04	HHMM	Hora final associada à data final
]

coluna_dici_1 = [
"Tipo_de_Registro",     #01	01	01	Numérico	Valor fixo “1”
"CNPJ",     #03	16	14	Numérico	Número do CNPJ que solicitou o parcelamento.
"Parcelamento",     #18	20	03	Numérico	Número do parcelamento.
"Data_do_Pedido",   #22	29	08	YYYYMMDD	Data do pedido do parcelamento.
"Situação",     #Situação do parcelamento, 31	33	03	Numérico	Situação do parcelamento.
"Data da Situação"      #	35	42	08	YYYYMMDD	Data da Situação do parcelamento.  #31	33	03	Numérico	Situação do parcelamento.
]


coluna_dici_2 = [
"Tipo de Registro",	#01	01	01	Numérico	Valor fixo “2”
"CNPJ",	#03	16	14	Numérico	Número do CNPJ que solicitou o parcelamento
"Parcelamento",	#18	20	03	Numérico	Número do parcelamento.
"Data da consolidação",	#22	29	08	YYYYMMDD	Data da consolidação do parcelamento.
"Valor total consolidado",	#31	47	17	Numérico(15,2)	"Valor total consolidado do parcelamento",
"Qtd Parcelas",	#49	51	03	Numérico	Quantidade de parcelas do parcelamento.
"Valor da parcela"	#53	69	17	Numérico (15,2)	
]

coluna_dici_3 = [
"Tipo de Registro",	#01	01	01	Numérico	Valor fixo “3”
"CNPJ",     #03	16	14	Numérico	Número do CNPJ que solicitou o parcelamento
"Parcelamento",     #	18	20	03	Numérico	Número do parcelamento.
"PA",	#22	29	08	YYYYMMDD	Período de apuração do débito.
"Data_de_Vencimento",	#31	38	08	YYYYMMDD	Data de vencimento do débito.
"Processo",	#40	56	17	Numérico	Número do processo do débito.
"Valor_Original",	#58	74	17	Numérico (15,2)	Valor original do débito.
"Valor_atualizado"	#76	92	17	Numérico (15,2)	Valor atualizado do débito.
]

coluna_dici_4 = [
"Tipo de Registro",     #01	01	01	Numérico	Valor fixo “4”
"CNPJ",     #03	16	14	Numérico	Número do CNPJ que solicitou o parcelamento.
"Parcelamento",     #18	20	03	Numérico	Número do parcelamento.
"Vencimento_da_Parcela",        #22	29	08	YYYYMMDD	 Data de vencimento da parcela.
"Número_do_DAS",        #31	47	17	Numérico	Número do DAS utilizado para o pagamento da Parcela.
]

coluna_dici_9 = [
"Tipo_de_Registro",	#01	01	01	Numérico	Valor fixo “9”
"Sistema",	#03	08	06	Alfa	Valor fixo“PARCSN”
"Qtd_Reg",	#10	18	09	Numérico	Total de registros gravados no arquivo.
]





# def aplicar_conversoes(df, tabela):
#     if tabela == "header_0":
#         df["Tipo_de_Registro"] = df["Tipo_de_Registro"].astype(str)
#         df["Sistema"] = df["Sistema"].astype(str)
#         df["Data_Inicial"] = pd.to_datetime(df["Data_Inicial"], format="%Y%m%d", errors="coerce").dt.date
#         df["Data_Final"] = pd.to_datetime(df["Data_Final"], format="%Y%m%d", errors="coerce").dt.date
#         for col in ["Hora_Inicial", "Hora_Final"]:
#             s = df[col].astype(str).str.strip()
#             s = s.str.zfill(4)
#             df[col] = s.str.slice(0,2) + ":" + s.str.slice(2,4) + ":00"
#     elif tabela == "pedidos_parcelamento_1":
#         df["Data_do_Pedido"] = pd.to_datetime(df["Data_do_Pedido"], format="%Y%m%d", errors="coerce").dt.date
#         df["Data da Situação"] = pd.to_datetime(df["Data da Situação"], format="%Y%m%d", errors="coerce").dt.date
#     elif tabela == "info_consolidacacao_2":
#         df["Data da consolidação"] = pd.to_datetime(df["Data da consolidação"], format="%Y%m%d", errors="coerce").dt.date
#         df["Valor da parcela"] = df["Valor da parcela"].str.replace(',', '.').replace('', np.nan).astype(float)
#     elif tabela == "relacao_debitos_3":
#         df["PA"] = pd.to_datetime(df["PA"], format="%Y%m%d", errors="coerce").dt.date
#         df["Data_de_Vencimento"] = pd.to_datetime(df["Data_de_Vencimento"], format="%Y%m%d", errors="coerce").dt.date
#     elif tabela == "parcelas_4":
#         df["Vencimento_da_Parcela"] = pd.to_datetime(df["Vencimento_da_Parcela"], format="%Y%m%d", errors="coerce").dt.date
#     return df



# def aplicar_conversoes(df, tabela):
#     if tabela == "header_0":
#         for colun in df.columns:
#             if colun.lower().startswith(("data","Vencimento_")):
#                 # df[colun] =pd.to_datetime(df[colun], format="%Y%m%d", errors="raise").dt.date
#                 df[colun] = pd.to_datetime(df[colun].str.replace(r'\D', '', regex=True))
#                 df[colun] = pd.to_datetime(df[colun], format="%Y%m%d", errors="coerce").dt.date
#             elif colun.lower().startswith("hora"):
#                 # df[colun] = df[colun].astype(str).str.strip()
#                 # df[colun] = df[colun].str.zfill(4)
#                 # 1) limpa / 2) garante 4 dígitos 3) converte e 4) formata em HH:MM
#                 df[colun] = (
#                     df[colun]
#                     .astype(str)
#                     .str.strip()
#                     .str.zfill(4)                                           # e.g. " 830" → "0830"
#                     .pipe(lambda s: pd.to_datetime(s, format="%H%M", errors="coerce"))  # Timestamp “1900-01-01 HH:MM”
#                     .dt.strftime("%H:%M")                                  # “08:30”
#                 )
#             elif colun.lower().startswith("Valor"):
#                 df[colun] = df[colun] = df[colun].str.replace(',', '.').replace('', np.nan).astype(float)
#             else:
#                 df[colun]=df[colun].astype(str)
#     elif tabela == "pedidos_parcelamento_1":
#             for colun in df.columns:
#                 if colun.lower().startswith(("data","Vencimento_")):
#                     # df[colun] =pd.to_datetime(df[colun], format="%Y%m%d", errors="raise").dt.date
#                     df[colun] = pd.to_datetime(df[colun].str.replace(r'\D', '', regex=True))
#                     df[colun] = pd.to_datetime(df[colun], format="%Y%m%d", errors="coerce").dt.date
#                 elif colun.lower().startswith("hora"):
#                     df[colun] = df[colun].astype(str).str.strip()
#                     df[colun] = df[colun].str.zfill(4)
#                 elif colun.lower().startswith("Valor"):
#                     df[colun] = df[colun] = df[colun].str.replace(',', '.').replace('', np.nan).astype(float)
#                 else:
#                     df[colun]=df[colun].astype(str)
#     elif tabela == "info_consolidacacao_2":
#             for colun in df.columns:
#                 if colun.lower().startswith(("data","Vencimento_")):
#                     # df[colun] =pd.to_datetime(df[colun], format="%Y%m%d", errors="raise").dt.date
#                     df[colun] = pd.to_datetime(df[colun].str.replace(r'\D', '', regex=True))
#                     df[colun] = pd.to_datetime(df[colun], format="%Y%m%d", errors="coerce").dt.date
#                 elif colun.lower().startswith("hora"):
#                     df[colun] = df[colun].astype(str).str.strip()
#                     df[colun] = df[colun].str.zfill(4)
#                 elif colun.lower().startswith("Valor"):
#                     df[colun] = df[colun] = df[colun].str.replace(',', '.').replace('', np.nan).astype(float)
#                 else:
#                     df[colun]=df[colun].astype(str)
#     elif tabela == "relacao_debitos_3":
#             for colun in df.columns:
#                 if colun.lower().startswith(("data","Vencimento_")):
#                     # df[colun] =pd.to_datetime(df[colun], format="%Y%m%d", errors="raise").dt.date
#                     df[colun] = pd.to_datetime(df[colun].str.replace(r'\D', '', regex=True))
#                     df[colun] = pd.to_datetime(df[colun], format="%Y%m%d", errors="coerce").dt.date
#                 elif colun.lower().startswith("hora"):
#                     df[colun] = df[colun].astype(str).str.strip()
#                     df[colun] = df[colun].str.zfill(4)
#                 elif colun.lower().startswith("Valor"):
#                     df[colun] = df[colun] = df[colun].str.replace(',', '.').replace('', np.nan).astype(float)
#                 else:
#                     df[colun]=df[colun].astype(str)
#     elif tabela == "parcelas_4":
#             for colun in df.columns:
#                 if colun.lower().startswith(("data","Vencimento_")):
#                     # df[colun] =pd.to_datetime(df[colun], format="%Y%m%d", errors="raise").dt.date
#                     df[colun] = pd.to_datetime(df[colun].str.replace(r'\D', '', regex=True))
#                     df[colun] = pd.to_datetime(df[colun], format="%Y%m%d", errors="coerce").dt.date
#                 elif colun.lower().startswith("hora"):
#                     df[colun] = df[colun].astype(str).str.strip()
#                     df[colun] = df[colun].str.zfill(4)
#                 elif colun.lower().startswith("Valor"):
#                     df[colun] = df[colun] = df[colun].str.replace(',', '.').replace('', np.nan).astype(float)
#                 else:
#                     df[colun]=df[colun].astype(str)
#     elif tabela == "registro_9":
#             for colun in df.columns:
#                 if colun.lower().startswith(("data","Vencimento_")):
#                     # df[colun] =pd.to_datetime(df[colun], format="%Y%m%d", errors="raise").dt.date
#                     df[colun] = pd.to_datetime(df[colun].str.replace(r'\D', '', regex=True))
#                     df[colun] = pd.to_datetime(df[colun], format="%Y%m%d", errors="coerce").dt.date
#                 elif colun.lower().startswith("hora"):
#                     df[colun] = df[colun].astype(str).str.strip()
#                     df[colun] = df[colun].str.zfill(4)
#                 elif colun.lower().startswith("Valor"):
#                     df[colun] = df[colun] = df[colun].str.replace(',', '.').replace('', np.nan).astype(float)
#                 else:
#                     df[colun]=df[colun].astype(str)
#     return df
                    
def aplicar_conversoes(df: pd.DataFrame, tabela: str) -> pd.DataFrame:
    """
    Converte colunas de acordo com prefixos:
    - Data*: de string YYYYMMDD → datetime.date
    - Vencimento_* ou PA: idem Data
    - Hora*: de string H HMM ou HHMM → datetime.time
    - Valor*: de string com vírgula → float
    - Demais: cast para str
    """
    for col in df.columns:
        col_lower = col.lower()

        # Data e vencimento (YYYYMMDD → date)
        if col_lower.startswith("data") or col_lower.startswith("vencimento") or col_lower == "pa":
            # Retira quaisquer não dígitos e faz parse
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r'\D', '', regex=True)
                .pipe(pd.to_datetime, format="%Y%m%d", errors="coerce")
                .dt.date
            )

        # Hora (HHMM → time)
        elif col_lower.startswith("hora"):
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.zfill(4)  # garante 4 dígitos, ex: "830" → "0830"
                .apply(lambda s: 
                    datetime.strptime(s, "%H%M").time()
                    if pd.notna(s) and len(s) == 4 else None
                )
            )

        # Valores numéricos com casa decimal (ex: "1.234,56" ou "1234,56")
        elif "valor" in col_lower:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r'\.', "", regex=True)   # opcional: remove milhares
                .str.replace(',', '.', regex=False)   # vírgula → ponto
                .replace('', np.nan)
                .astype(float)
            )

        # Tudo mais fica string
        else:
            df[col] = df[col].astype(str)

    return df