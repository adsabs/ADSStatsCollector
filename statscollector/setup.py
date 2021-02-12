import os
import requests
from adsputils import setup_logging, load_config

# Global configuration
proj_home = os.path.realpath(os.path.dirname(os.path.dirname(__file__)))
config = load_config(proj_home=proj_home)
logger = setup_logging('run.py', proj_home=proj_home,
                       level=config.get('LOGGING_LEVEL', 'INFO'),
                       attach_stdout=config.get('LOG_STDOUT', False))
