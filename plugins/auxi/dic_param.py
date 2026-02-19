from airflow.models.param import Param
import datetime

params_dict = {
    "retries": 3,
    
    "Usuario": Param(
        default="José Nélio",
        type="string",
        enum=["José Nélio", "Vinicius B. Soares", "Igor Kalon"],
    ),
    "BUCKET_NAME": Param(
        default="nelio_teste",
        type="string",
        enum=["dataita", "k8s-dataita", "nelio_teste"],
    ),
    "PA": Param(
    default="2025-05",
    type="string",
    enum=["2025-05", "2025-04", "2025-03"],
    description="YYYY-MM",
    ),
    "PREFIX_RAW": Param(
        default="raw/simples_nacional/parcelamento/waiting",
        type="string",
        description="Caminho do bucket",
    ),
    "RAW_ARCHIVE": Param(
        default="raw/simples_nacional/parcelamento/archive",
        type="string",
        description="Caminho do bucket",
    ),
    "PREFIX_STAGED": Param(
        default="staged/simples_nacional/parcelamento/waiting",
        type="string",
        description="Caminho do bucket",
    ),
    "PREFIX_ARCHIVE": Param(
        default="staged/simples_nacional/parcelamento/archive",  
        type="string",
        description="Caminho do bucket",
    ),
    "dataset": Param(
        default="teste_parc_2",
        type="string",
        description="Dataset destino no BigQuery",
    ),
    "DELIMITER_A":Param(
        default=".zip",
        type="string",
        enum=[".parquet", ".csv", ".zip", ".txt", ".json"],
        description="Padrão é zip"
    ),
    "DELIMITER_B":Param(
        default=".parquet",
        type="string",
        enum=[".parquet", ".csv", ".zip", ".txt", ".json"],
        description="Padrão é parquet"
    )


    # "modo_escrita_bq": Param(
    # "WRITE_APPEND",
    #     type="string",
    #     title="Modo de Escrita no BigQuery",
    #     description="Como os dados devem ser escritos na tabela de destino: WRITE_APPEND para adicionar dados ou WRITE_TRUNCATE para substituir os dados.",
    #     enum=["WRITE_APPEND", "WRITE_TRUNCATE"],
    # ),
}


