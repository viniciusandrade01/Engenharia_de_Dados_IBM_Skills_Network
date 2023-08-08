import os
import requests
from io import BytesIO
import zipfile
from lib.utils import Utils

utl = Utils()

# Faz o download do arquivo ZIP
response = requests.get('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip')
zip_data = BytesIO(response.content)

# Abre o arquivo ZIP
with zipfile.ZipFile(zip_data, 'r') as zip_ref:
    # Extrai todo o conteúdo para o diretório do script
    zip_ref.extractall(os.path.dirname(os.path.abspath(__file__)) + "\\DadosBase")
    
tmpfile    = "temp.tmp"
logfile    = "logfile.txt"
targetfile = "transformed_data.csv"

# Rodando Processo ETL
utl.log("ETL Job Started")

utl.log("Extract phase Started")
extracted_data = utl.extract()
utl.log("Extract phase Ended")

utl.log("Transform phase Started")
transformed_data = utl.transform(extracted_data)
utl.log("Transform phase Ended")

utl.log("Load phase Started")
utl.load(targetfile, transformed_data)
utl.log("Load phase Ended")

utl.log("ETL Job Ended")
