import glob 
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

class Utils:
    def __init__(self):
        self.ok = 'ok'
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
            dataframe = dataframe.append({"name":name, "height":height, "weight":weight}, ignore_index=True)
        return dataframe
    
    def extract(self):
        extracted_data = pd.DataFrame(columns=['name','height','weight']) # create an empty data frame to hold extracted data
    
        #process all csv files
        for csvfile in glob.glob("*.csv"):
            extracted_data = extracted_data.append(self.extract_from_csv(csvfile), ignore_index=True)
            
        #process all json files
        for jsonfile in glob.glob("*.json"):
            extracted_data = extracted_data.append(self.extract_from_json(jsonfile), ignore_index=True)
        
        #process all xml files
        for xmlfile in glob.glob("*.xml"):
            extracted_data = extracted_data.append(self.extract_from_xml(xmlfile), ignore_index=True)
            
        return extracted_data
    
    def transform(self, data):
        data['height'] = round(data.height * 0.0254,2)
        data['weight'] = round(data.weight * 0.45359237,2)
        return data
    
    def load(self, targetfile,data_to_load):
        data_to_load.to_csv(targetfile)  
        
    def log(self, message):
        timestamp_format = '%Y-%h-%d-%H:%M:%S'
        now = datetime.now()
        timestamp = now.strftime(timestamp_format)
        with open("logfile.txt","a") as f:
            f.write(timestamp + ',' + message + '\n')