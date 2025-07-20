import luigi
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tasks.data_ingestion import ingest_breweries_data
from tasks.bronze_to_silver import bronze_to_silver
from tasks.silver_to_gold import silver_to_gold

BRONZE_DIR = "data_lake/bronze"
SILVER_DIR = "data_lake/silver"
GOLD_DIR = "data_lake/gold"

class FetchRawData(luigi.Task):
    def output(self):
        return luigi.LocalTarget(os.path.join(BRONZE_DIR, "breweries.json"))

    def run(self):
        ingest_breweries_data(BRONZE_DIR)

class TransformToSilver(luigi.Task):
    def requires(self):
        return FetchRawData()

    def output(self):
        return [luigi.LocalTarget(os.path.join(SILVER_DIR, f)) for f in os.listdir(SILVER_DIR) if f.endswith(".parquet")]

    def run(self):
        bronze_to_silver(BRONZE_DIR, SILVER_DIR)

class AggregateToGold(luigi.Task):
    def requires(self):
        return TransformToSilver()

    def output(self):
        return luigi.LocalTarget(os.path.join(GOLD_DIR, "aggregated.parquet"))

    def run(self):
        silver_to_gold(SILVER_DIR, GOLD_DIR)

if __name__ == "__main__":
    luigi.run()