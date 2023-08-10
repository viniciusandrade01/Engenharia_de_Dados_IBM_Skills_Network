import glob
from io import BytesIO
import logging
import os 
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

import requests

class Utils:
    # Extrair do CSV
    def extract_from_csv(self, file_to_process):
        dataframe = pd.read_csv(file_to_process)
        return dataframe

    # Extrair do JSON
    def extract_from_json(self, file_to_process):
        dataframe = pd.read_json(file_to_process,lines=True)
        return dataframe
    
    # Extrair do XML
    def extract_from_xml(self, file_to_process):
        dataframe = pd.DataFrame(columns=["name", "height", "weight"])
        tree = ET.parse(file_to_process)
        root = tree.getroot()
        for person in root:
            name = person.find("name").text
            height = float(person.find("height").text)
            weight = float(person.find("weight").text)
            dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name.title(), "height":height, "weight":weight}])], ignore_index=True)
        return dataframe
    
    def extract(self, diretorio: str):
        extracted_data = pd.DataFrame(columns=['name','height','weight']) 
        origem = os.path.join(os.getcwd(), diretorio)
    
        # Processando todos os arquivos csv
        for csvfile in [os.path.basename(arquivo) for arquivo in glob.glob(os.path.join(origem, "*.csv"))]:
            csvfile = f"{origem}\\{csvfile}"
            extracted_data = pd.concat([extracted_data, self.extract_from_csv(csvfile)], ignore_index=True)
            
        # Processando todos os arquivos json
        for jsonfile in [os.path.basename(arquivo) for arquivo in glob.glob(os.path.join(origem, "*.json"))]:
            jsonfile = f"{origem}\\{jsonfile}"
            extracted_data = pd.concat([extracted_data, self.extract_from_json(jsonfile)], ignore_index=True)
        
        # Processando todos os arquivos xml
        for xmlfile in [os.path.basename(arquivo) for arquivo in glob.glob(os.path.join(origem, "*.xml"))]:
            xmlfile = f"{origem}\\{xmlfile}"
            extracted_data = pd.concat([extracted_data, self.extract_from_xml(xmlfile)], ignore_index=True)

        return extracted_data
    
    def transform(self, data):
        data['height'] = round(data.height * 0.0254,2)
        data['weight'] = round(data.weight * 0.45359237,2)
        data['name'] = data.name.str.title()
        return data
    
    def load(self, targetfile, data_to_load):
        data_to_load.to_csv(targetfile, sep="\t")
    
    def directory(self):
        diretorio = input("Digite o nome do diretório para armazenar o csv gerado (exemplo: Coleta): ")
        diretorio = diretorio or "Coleta"
        
        # Verificar se o diretório existe, e se não existir, criá-lo
        if not os.path.exists(diretorio):
            os.makedirs(diretorio)
            
        return diretorio
    
    def request(self, link: str):
        try:
            log = Logger()
            response = requests.get(link)

            # Verificar se a resposta foi bem-sucedida (código de status 200)
            if response.status_code == 200:
                return BytesIO(response.content)
            else:
                log.error(f"Erro ao acessar o link. Código de status: {response.status_code}")

        except requests.exceptions.RequestException as e:
            log.error(f"Erro de conexão: {e}")
        except Exception as e:
            log.error(f"Erro inesperado: {e}")

class Logger:
    def __init__(self):
        if not logging.getLogger(__name__).hasHandlers():
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
        
        file_handler = logging.FileHandler(f"Log_{datetime.now().strftime('%Y_%m_%d')}.log")
        #file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
    
    def debug(self, message):
        self.logger.debug(message)
    
    def info(self, message):
        self.logger.info(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def critical(self, message):
        self.logger.critical(message)
