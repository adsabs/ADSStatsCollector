import os
import requests
from urllib.parse import urljoin
from .setup import config, logger

def _build_url(job, provider, instance):
    """Build URL"""
    base_url = config.get('PROMETHEUS_PUSHGATEWAY_URL')
    endpoint = 'metrics/job/{j}/provider/{p}'.format(j=job, p=provider)
    if instance:
        endpoint += '/instance/{i}'.format(i=instance)
    url = urljoin(base_url, endpoint)
    return url

def _build_data(payload_key, payload_type, payload_description, payload_label, payload_value):
    """Build data payload"""
    data = '# TYPE {payload_key} {payload_type}\n'.format(payload_key=payload_key, payload_type=payload_type)
    if payload_description:
        data += '# HELP {payload_key} {payload_description}\n'.format(payload_key=payload_key, payload_description=payload_description)
    if payload_label:
        payload_label = "{{label=\"{l}\"}}".format(l=payload_label)
    else:
        payload_label = ""
    data += '{payload_key}{payload_label} {payload_value}\n'.format(payload_key=payload_key, payload_label=payload_label, payload_value=payload_value)
    return data

def _push(job, payload_key, payload_value, provider="ADSStatsCollector", instance=config.get('PROMETHEUS_PUSHGATEWAY_INSTANCE'), payload_type="untyped", payload_description=None, payload_label=None, simulate=False):
    url = _build_url(job, provider, instance)
    data = _build_data(payload_key, payload_type, payload_description, payload_label, payload_value)
    if not simulate:
        r = requests.post(url, data=data, timeout=30)
        #r = requests.delete(url, data=None, timeout=30)
        r.raise_for_status()
    else:
        logger.info("Push key '%s', job '%s', instance '%s', provider '%s' and value '%s'", payload_key, job, provider, instance, payload_value)

def push(payload_key, results, prefix=None, simulate=False):
    if prefix is None:
        prefix = []
    for k, v in results.items():
        if isinstance(v, dict):
            push(payload_key, v, prefix=prefix+[k], simulate=simulate)
        else:
            job = "_".join(prefix+[k])
            payload_value = v
            try:
                _push(job, payload_key, payload_value, simulate=simulate)
            except:
                logger.exception("Unable to push key '%s', job '%s' and value '%s'", payload_key, job, payload_value)
