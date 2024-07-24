import logging
import logging.config
import yaml

# Function to generate logger from config file
def get_logger() -> logging.Logger:
    with open('cdapython/config/logger.yml') as log_config_file:
        log_config = yaml.safe_load(log_config_file)
    logging.config.dictConfig(log_config)
    logger = logging.getLogger("simple")
    return logger

