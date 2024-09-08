import os
import zipfile
from abc import ABC, abstractmethod
import pandas as pd

# defining the abstract class for data ingestion
class Ingestor(ABC):
    @abstractmethod
    def ingest(self, file_path: str) -> pd.DataFrame:
        # abs method ingest to get data from a file
        pass
    
# implementing a concrete class for zip ingestion
class ZipIngestor(Ingestor):
    def ingest(self, file_path: str) -> pd.DataFrame:
        # ensure file is a .zip
        if not file_path.endswith('.zip'):
            raise ValueError("This isn't a zip file!")
        
        # extract the zip file 
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall("extracted_data")
            
        # find extracted csv file(assuming there is csv in zip)
        extracted_files = os.listdir("extracted_data")
        csv_files = [f for f in extracted_files if f.endswith('.csv')]
        
        if len(csv_files) == 0:
            raise ValueError("No csv file found in the zip!")
        if len(csv_files) > 1:
            raise ValueError("Multiple csv's found , please specify one!")
        
        # read csv into a dataframe
        csv_file_path = os.path.join('extracted_data', csv_files[0])
        df = pd.read_csv(csv_file_path)
        
        return df
    
    # Implenting a Factory to create data Ingestors
    class IngestorFactory:
        @staticmethod
        def get_ingestor(file_extension: str) -> Ingestor:
            if file_extension == '.zip':
                return ZipIngestor()
            else:
                raise ValueError("NO ingestor found for : {file_extension} file")
            
            
            
    # ex use
    if __name__ == "__main__":
        
       file_path = "https://github.com/Shr11/House-Price-Prediction-Website/tree/main/data.zip"
       
       # determine file extension
       file_extension = os.path.splitext(file_path)[1]
       
       # get the apt data ingestor
       data_ingestor = IngestorFactory.get_ingestor(file_extension)
       
       # ingest data into df
       df = data_ingestor.ingest(file_path)
       
       # now df has data from extracted csv file
       print(df.head())
       # pass