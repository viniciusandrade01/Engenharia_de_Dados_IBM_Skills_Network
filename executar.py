import os
import zipfile
from lib.utils import Utils, Logger

# Criação de instâncias das classes "Utils" e "Logger"
utl = Utils()
logger = Logger()

# Coleta e cria diretório desejado
diretorio = utl.directory()

# Faz o download do arquivo ZIP
zip_data = utl.request('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip')

# Abre o arquivo ZIP
with zipfile.ZipFile(zip_data, 'r') as zip_ref:
    # Extrai todo o conteúdo para o diretório informado
    zip_ref.extractall(os.path.join(os.path.dirname(os.path.abspath(__file__)), diretorio))

# Rodando Processo ETL
logger.info("Processo ETL inicializado.")

logger.info("Fase de Extração inicializada.")
extracted_data = utl.extract(diretorio)
logger.info("Fase de Extração finalizada.")

logger.info("Fase de Transformação inicializada.")
transformed_data = utl.transform(extracted_data)
logger.info("Fase de Transformação finalizada.")

logger.info("Fase de Carregamento inicializada.")
utl.load("Dados_transformados.csv", transformed_data)
logger.info("Fase de Carregamento finalizada.")

logger.info("Processo ETL finalizado.")