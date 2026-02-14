import json
import logging
from pyspark.sql.functions import col, date_sub, current_date

# 🔥 Configuração básica de log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def process_table(spark, config_path):

    try:
        logger.info("Iniciando processamento")

        with open(config_path, "r") as f:
            config = json.load(f)

        nometabela = config["nometabela"]
        db_origem = config["db_origem"]
        db_destino = config["db_destino"]
        carga = config["carga"]

        origem = f"{db_origem}.{nometabela}"
        destino = f"{db_destino}.{nometabela}"

        logger.info(f"Lendo tabela origem: {origem}")

        df = spark.table(origem)

        # 🔥 Incremental
        if carga == "incremental":
            coluna_data = config["coluna_data"]
            dias_atras = config.get("dias_atras", 1)

            logger.info(f"Aplicando filtro incremental D-{dias_atras}")

            df = df.filter(
                col(coluna_data) >= date_sub(current_date(), dias_atras)
            )

        # 🔥 Adiciona partição
        df = df.withColumn("data_particao", current_date())

        qtd = df.count()

        logger.info(f"Quantidade de registros a gravar: {qtd}")

        df.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("data_particao") \
            .saveAsTable(destino)

        logger.info(f"Gravação concluída com sucesso em {destino}")

    except Exception as e:
        logger.error(f"Erro no processamento: {str(e)}")
        raise
