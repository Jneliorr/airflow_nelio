
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import logging


class MoveGCSFiles(BaseOperator):

    template_fields = ("bucket_source", "bucket_dest", "prefix", "destino", "delimiter")

    @apply_defaults
    def __init__(self, bucket_source, bucket_dest, prefix, destino, delimiter = '.paquet' ,**kwargs):
        super().__init__(**kwargs)
        self.bucket_source = bucket_source
        self.bucket_dest = bucket_dest
        self.prefix = prefix.rstrip("/") + "/"
        self.destino = destino.rstrip("/") + "/"
        self.delimiter = delimiter

    def execute(self, context): 
                    
            hook = GCSHook(gcp_conn_id="google_cloud_default")
            logging.info(f"Listando arquivos em: {self.prefix}")

            arquivos = hook.list(bucket_name=self.bucket_source, prefix=self.prefix)
            arquivos = [arquivo for arquivo in arquivos if arquivo.endswith(self.delimiter)]
            logging.info(f"Arquivos encontrados: {arquivos}")

            for prefix_arquivo in arquivos:

                nome_arquivo = prefix_arquivo.split('/')[-1]
                destino_final = self.destino + nome_arquivo            
                logging.info(f"Processando: {nome_arquivo}")

                hook.copy(
                    source_bucket=self.bucket_source,
                    source_object=prefix_arquivo,
                    destination_bucket=self.bucket_dest,
                    destination_object=destino_final
                )

                logging.info(f"Movido para: {destino_final}")
                logging.info("Deletando arquivos residuais...")

                logging.info(f"Deletando: {prefix_arquivo}")
                hook.delete(bucket_name=self.bucket_source, object_name=prefix_arquivo)      
            
            #logging.info(f"Criando pasta vazia: {origem_prefix}")
            #hook.upload(
            #    bucket_name=bucket,
            #    object_name=origem_prefix,
            #    data=b"not_drop.svg"
            #)

            #confere_pasta = hook.list(bucket_name=bucket, prefix=origem_prefix)
            #logging.info(f"Arquivos residuais: {confere_pasta}")
            #logging.info("Processo concluído. Todos os passos foram executados.")