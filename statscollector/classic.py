from datetime import datetime
from .setup import config, logger

def bibcodes():
    try:
        with open(config.get('CLASSIC_CANONICAL_FILE'), "r") as f:
            bibcodes = [line.strip() for line in f]
    except:
        logger.exception("Unable to retreive bibcodes from classic")
        return []
    else:
        return bibcodes

def compare(classic_bibcodes, db_bibcodes, solr_bibcodes):
    """Compare bibcode lists against classic"""
    results = {}
    batch = {}
    now = datetime.utcnow()
    prefix = "{}{}{}_{}{}".format(now.year, now.month, now.day, now.hour, now.minute)

    if len(classic_bibcodes) > 0:
        classic_bibcodes = set(classic_bibcodes)

        if len(db_bibcodes) > 0:
            db_bibcodes = set(db_bibcodes)
            extra_in_db = db_bibcodes.difference(classic_bibcodes)
            extra_in_db = [e for e in extra_in_db if "zndo" not in e] # Filter out non-classic Zenodo records
            missing_in_db = classic_bibcodes.difference(db_bibcodes)
            results['extra_in_db'] = len(extra_in_db)
            results['missing_in_db'] = len(missing_in_db)
        else:
            extra_in_db = set()
            missing_in_db = set()

        if len(solr_bibcodes) > 0:
            solr_bibcodes = set(solr_bibcodes)
            extra_in_solr = solr_bibcodes.difference(classic_bibcodes)
            extra_in_solr = [e for e in extra_in_solr if "zndo" not in e] # Filter out non-classic Zenodo records
            missing_in_solr = classic_bibcodes.difference(solr_bibcodes)
            results['extra_in_solr'] = len(extra_in_solr)
            results['missing_in_solr'] = len(missing_in_solr)
        else:
            extra_in_solr = set()
            missing_in_solr = set()

        batch.update({
            "{}_extra_in_db".format(prefix): extra_in_db,
            "{}_missing_in_db".format(prefix): missing_in_db,
            "{}_extra_in_solr".format(prefix): extra_in_solr,
            "{}_missing_in_solr".format(prefix): missing_in_solr,
        })

    return results, batch
