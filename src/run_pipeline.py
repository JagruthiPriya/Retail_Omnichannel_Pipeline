
import subprocess
import sys
from pathlib import Path

from utils.pipeline_logger import get_logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]

logger = get_logger("RetailPipeline")


PIPELINE_STAGES = [
    ("API ingestion", "src/ingestion/ingest_api.py"),
    ("CSV ingestion", "src/ingestion/ingest_csv.py"),
    ("PostgreSQL ingestion", "src/ingestion/ingest_postgres.py"),
    ("Bronze layer", "src/ingestion/build_bronze.py"),
    ("Silver layer", "src/transformations/build_silver.py"),
    ("Silver verification", "src/quality/verify_silver.py"),
    ("Gold layer", "src/transformations/build_gold.py"),
    ("Gold quality validation", "src/quality/validate_gold.py"),
    ("SQL reporting", "src/transformations/query_gold.py"),
]


def run_pipeline():

    logger.info("========== RETAIL PIPELINE STARTED ==========")

    for stage_name, script_path in PIPELINE_STAGES:

        logger.info("Starting stage: %s", stage_name)

        full_script_path = PROJECT_ROOT / script_path

        try:
            subprocess.run(
                [sys.executable, str(full_script_path)],
                cwd=PROJECT_ROOT,
                check=True
            )

            logger.info("Completed stage: %s", stage_name)

        except subprocess.CalledProcessError as error:

            logger.error(
                "Stage failed: %s | Exit code: %s",
                stage_name,
                error.returncode
            )

            logger.error("Pipeline stopped. Later stages will not run.")

            raise SystemExit(error.returncode)

    logger.info("========== RETAIL PIPELINE COMPLETED SUCCESSFULLY ==========")


if __name__ == "__main__":
    run_pipeline()
