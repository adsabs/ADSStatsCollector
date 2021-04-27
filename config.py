LOGGING_LEVEL = 'INFO'
LOG_STDOUT = True
SERVICE = 'stats_collector'
ENVIRONMENT = 'ADSStatsCollector'

PROMETHEUS_PUSHGATEWAY_PROVIDER = "ADSStatsCollector"
PROMETHEUS_PUSHGATEWAY_INSTANCE = "stats_collector"
PROMETHEUS_PUSHGATEWAY_URL = "http://localhost:9091"

SOLR_URL = 'http://localhost:9983/solr/collection1/'

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "<secret>"
POSTGRES_MASTER_PIPELINE_DB = "master_pipeline"

# http://localhost:9000/system/authentication/users/tokens/admin
GRAYLOG_TOKEN = '<secret>'
GRAYLOG_URL = "http://localhost:9000/"
GRAYLOG_MYADS_QUERY = 'namespace_name:back-prod AND container_name:myads_pipeline AND message:"Email sent to *"'
GRAYLOG_CONTAINER_QUERY = 'namespace_name:back-prod AND container_name:{}'
GRAYLOG_CONTAINER_NAMES = (
    "fulltext_pipeline",
    "citation_capture_pipeline",
    "import_pipeline",
    "orcid_pipeline",
    "master_pipeline",
    "augment_pipeline",
    "data_pipeline",
    "myads_pipeline",
    "montysolr",
    "postgresql",
    "rabbitmq",
    "article_of_the_day",
    "doc_matching_pipeline",
    "neo4j"
)

CLASSIC_CANONICAL_FILE = "/bibcodes.list.can"

GOOGLE_DRIVE_KEEP_LAST_N_FOLDERS = 7
# Please, follow the instructions in https://developers.google.com/drive/api/v3/quickstart/python to download the file 'credentials.json'
GOOGLE_DRIVE_CREDENTIALS_FILENAME = "credentials.json"
GOOGLE_DRIVE_TOKEN_FILENAME = "token.json"
# https://drive.google.com/drive/u/1/folders/ID
GOOGLE_DRIVE_FOLDER_ID = "ID"

SOURCES_DIR = "/sources"
PDF_SOURCES_DIR = "/seri"
STDDEV_CUTOFF = 1.5
COUNT_ERR = 5
