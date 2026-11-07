
from utils.pipeline_logger import get_logger

logger = get_logger("RetailPipeline")

logger.info("Retail pipeline started")
logger.info("Bronze layer completed")
logger.warning("Unknown product detected")
logger.error("This is a sample error message")

print("Logger test completed")
