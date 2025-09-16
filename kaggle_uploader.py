# %%

import datetime
import os
import dotenv
import shutil
import json

dotenv.load_dotenv()

KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

class KaggleUploader:
    
    def __init__(self, name, kaggle_client, kaggle_config):
        self.name = name
        self.kaggle_client = kaggle_client
        self.kaggle_config = kaggle_config


    def prepare(self):
        if not os.path.exists(f"data/{self.name}/kaggle"):
            os.makedirs(f"data/{self.name}/kaggle")

            with open(f"data/{self.name}/kaggle/dataset-metadata.json", "w") as f:
                json.dump(self.kaggle_config, f, indent=4)
        
        for file in os.listdir(f"data/{self.name}"):
            if file.endswith(".csv") or file.endswith(".db"):
                shutil.copy(f"data/{self.name}/{file}", f"data/{self.name}/kaggle/{file}")


    def upload(self):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.kaggle_client.dataset_create_version(folder=f"data/{self.name}/kaggle",
                              version_notes=f"{now} - New version",
                              convert_to_csv=False,
                              quiet=False,
                              )


    def run(self):
        self.prepare()
        self.upload()


def execute():
    dotenv.load_dotenv('.env')

    with open('config.json', 'r') as f:
        configs = json.load(f)

    from kaggle.api.kaggle_api_extended import KaggleApi
    client = KaggleApi()
    client.authenticate()
    
    for config in configs.values():
        kaggle_points = KaggleUploader(config['name'], client, config['kaggle'])
        kaggle_points.run()

def main():
    execute()


if __name__ == "__main__":
    main()