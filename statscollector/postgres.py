import psycopg2
from .setup import config, logger

INTERVAL = '1 HOURS'

BIBCODES = """
SELECT  bibcode FROM records WHERE bib_data IS NOT NULL;
"""

COUNTS = """
SELECT  count(*) AS total,
        count(bib_data) AS bib_data,
        count(nonbib_data) AS nonbib_data,
        count(metrics) AS metrics,
        count(orcid_claims) AS orcid_claims,
        count(augments) AS augments,
        count(fulltext) AS fulltext
FROM records;
"""

UPDATED = """
WITH total AS (
        SELECT count(*) AS total FROM records
        WHERE updated BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    ),
    bib_data AS (
        SELECT count(*) AS bib_data FROM records
        WHERE bib_data_updated BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    ),
    nonbib_data AS (
        SELECT count(*) AS nonbib_data FROM records
        WHERE nonbib_data_updated BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    ),
    metrics AS (
        SELECT count(*) AS metrics FROM records
        WHERE metrics_updated BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    ),
    orcid_claims AS (
        SELECT count(*) AS orcid_claims FROM records
        WHERE orcid_claims_updated BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    ),
    augments AS (
        SELECT count(*) AS augments FROM records
        WHERE augments_updated BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    ),
    fulltext AS (
        SELECT count(*) AS fulltext FROM records
        WHERE fulltext_updated BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    )
SELECT * FROM total, bib_data, nonbib_data, metrics, orcid_claims, augments, fulltext;
""".format(INTERVAL)

PROCESSED = """
WITH total AS (
        SELECT count(*) AS total FROM records
        WHERE processed BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    ),
    solr AS (
        SELECT count(*) AS bib_data FROM records
        WHERE solr_processed BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    ),
    metrics AS (
        SELECT count(*) AS nonbib_data FROM records
        WHERE metrics_processed BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    ),
    datalinks AS (
        SELECT count(*) AS metrics FROM records
        WHERE datalinks_processed BETWEEN NOW() - INTERVAL '{0}' AND NOW()
    )
SELECT * FROM total, solr, metrics, datalinks;
""".format(INTERVAL)

CREATED = "SELECT count(*) AS count FROM records WHERE created BETWEEN NOW() - INTERVAL '{0}' AND NOW();".format(INTERVAL)

def stats():
    host = config.get('POSTGRES_HOST')
    port = config.get('POSTGRES_PORT')
    user = config.get('POSTGRES_USER')
    password = config.get('POSTGRES_PASSWORD')
    master_pipeline_db = config.get('POSTGRES_MASTER_PIPELINE_DB')

    results = {}
    master_connection = None
    try:
        master_connection = psycopg2.connect(host=host,
                                             port=port,
                                             database=master_pipeline_db,
                                             user=user,
                                             password=password)
        with master_connection.cursor() as master_cursor:
            master_cursor.execute(CREATED)
            created, = master_cursor.fetchone()
            results['created'] = created
            master_cursor.execute(UPDATED)
            total, bib_data, nonbib_data, metrics, orcid_claims, augments, fulltext = master_cursor.fetchone()
            results['updated'] = {
                'total': total,
                'bib_data': bib_data,
                'nonbib_data': nonbib_data,
                'metrics': metrics,
                'orcid_claims': orcid_claims,
                'augments': augments,
                'fulltext': fulltext
            }
            master_cursor.execute(PROCESSED)
            total, solr, metrics, datalinks = master_cursor.fetchone()
            results['processed'] = {
                'total': total,
                'solr': solr,
                'metrics': metrics,
                'datalinks': datalinks
            }
            master_cursor.execute(COUNTS)
            total, bib_data, nonbib_data, metrics, orcid_claims, augments, fulltext = master_cursor.fetchone()
            results['registered'] = {
                'total': total,
                'bib_data': bib_data,
                'nonbib_data': nonbib_data,
                'metrics': metrics,
                'orcid_claims': orcid_claims,
                'augments': augments,
                'fulltext': fulltext
            }
    except:
        logger.exception("Failed retrieving stats from postgres")
        return {}
    finally:
        if master_connection is not None:
            master_connection.close()
    return results

def bibcodes():
    host = config.get('POSTGRES_HOST')
    port = config.get('POSTGRES_PORT')
    user = config.get('POSTGRES_USER')
    password = config.get('POSTGRES_PASSWORD')
    master_pipeline_db = config.get('POSTGRES_MASTER_PIPELINE_DB')

    bibcodes = []
    master_connection = None
    try:
        master_connection = psycopg2.connect(host=host,
                                             port=port,
                                             database=master_pipeline_db,
                                             user=user,
                                             password=password)
        with master_connection.cursor() as master_cursor:
            chunk_size = 100000
            master_cursor.itersize = chunk_size
            master_cursor.execute(BIBCODES)
            bibcodes = [bibcode for (bibcode,) in master_cursor]
    except:
        logger.exception("Failed retrieving bibcodes from postgres")
        return []
    finally:
        if master_connection is not None:
            master_connection.close()
    return bibcodes
