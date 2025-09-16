# %%

import argparse
import json
import os

import sqlalchemy
import pandas as pd
import dotenv

class Table:
    def __init__(self, name:str, query:str=None, if_exists:str='replace'):
        self.name = name
        self.query = query
        self.if_exists = if_exists

        if self.query:
            self.query = self.load_query()

    def load_query(self):
        with open(self.query) as f:
            sql = f.read()

        return sql
    
    def __str__(self):
        return f"Table(name={self.name}, query={self.query}, if_exists={self.if_exists})"


class DataLoader:
    def __init__(self,
                 name:str,
                 source_engine:sqlalchemy.engine.base.Engine,
                 target_engine:sqlalchemy.engine.base.Engine,
                 tables:list[Table],
                 csv:bool=True,
                 parquet:bool=True
                 ):
        
        self.name = name
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.tables = tables
        self.csv = csv
        self.parquet = parquet


    def load_table_source(self, table):
        return pd.read_sql_table(table, self.source_engine)
    

    def load_query_source(self, query):
        return pd.read_sql_query(query, self.source_engine)


    def load_data(self, table:Table):
        if table.query:
            return self.load_query_source(table.query)
        else:
            return self.load_table_source(table.name)


    def save_to_target(self, data, table:Table):
        data.to_sql(table.name, self.target_engine, if_exists=table.if_exists, index=False)
        print(f"Data saved to {table.name} in target database.")


    def save_csv(self, data, table:Table):
        filename = f"data/{self.name}/{table.name}.csv".lower()
        print(filename)
        data.to_csv(filename, index=False, sep=";")
        print(f"Data saved to {filename}.")


    def save_parquet(self, data, table:Table):
        filename = f"data/{self.name}/{table.name}.parquet".lower()
        print(filename)
        data.to_parquet(filename, index=False)
        print(f"Data saved to {filename}.")


    def load_and_save_data(self, table:Table):
        data = self.load_data(table)
        self.save_to_target(data, table)

        if self.csv:
            self.save_csv(data, table)
        if self.parquet:
            self.save_parquet(data, table)


    def run(self):
        print(f"Starting data loading process for {self.name}...")
        for table in self.tables:
            self.load_and_save_data(table)
        print("Data loading process completed.")


def setup_engine(uri:str, engine_type:str='mysql'):
    
    if engine_type == 'sqlite':
        return sqlalchemy.create_engine(f'sqlite:///{uri}')
    
    elif engine_type == 'mysql':
        return sqlalchemy.create_engine(f'mysql+pymysql://{uri}')
    
    else:
        print(engine_type)
    

def load_migration(migration, csv:bool, parquet:bool):

    source_uri = os.getenv(migration['source']['name'])
    source_type = migration['source']['engine']

    target_uri = os.getenv(migration['target']['name'])
    target_type = migration['target']['engine']

    source_engine = setup_engine(source_uri, source_type)
    target_engine = setup_engine(target_uri, target_type)

    tables = [Table(**table) for table in migration['tables']]

    data_loader = DataLoader(
        name=migration['name'],
        source_engine=source_engine,
        target_engine=target_engine,
        tables=tables,
        csv=csv,
        parquet=parquet
    )

    data_loader.run()


def execute(csv, parquet):
    dotenv.load_dotenv('.env')

    with open('config.json', 'r') as f:
        migrations = json.load(f)

    for m in migrations.values():
        os.makedirs(f"data/{m['name']}", exist_ok=True)
        load_migration(m, csv=csv, parquet=parquet)


def main():
    parser = argparse.ArgumentParser(description="Data Loader Script")
    parser.add_argument("--csv", "-c", action="store_true", help="Save data as CSV files")
    parser.add_argument("--parquet", "-p", action="store_true", help="Save data as Parquet files")
    args = parser.parse_args()
    execute(csv=args.csv, parquet=args.parquet)


if __name__ == "__main__":
   main()
