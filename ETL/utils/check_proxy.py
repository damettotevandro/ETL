import logging
import urllib.request
import socket
import urllib.error
import subprocess
from subprocess import Popen, PIPE, STDOUT

logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


def check_proxy(proxy, timeout=120, ignore_error=False):
    try:
        host, port = proxy.split(":")
    except ValueError as err:
        logger.error(f'check your proxy to see if it correctly entered host:port - you proxy {proxy}')
    logger.info(f'Checking proxy...')
    logger.info(f'Proxy: {host}')
    logger.info(f'Port: {port}')
    socket.setdefaulttimeout(timeout)
    logger.info(f'Time out: {timeout}')
    logger.info(f'Ignore error: {ignore_error}')
    status = ''
    try:
        proxy_handler = urllib.request.ProxyHandler({'http': proxy})
        opener = urllib.request.build_opener(proxy_handler)
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib.request.install_opener(opener)
        req = urllib.request.Request('http://www.google.com')
        sock = urllib.request.urlopen(req)
        status = 'working'
    except urllib.error.HTTPError as e:
        status = 'not working'
        if not ignore_error:
            logger.error(f'check_proxy_error: http error: {e.code}')
            raise urllib.error.HTTPError(e)
    except Exception as detail:
        status = 'not working'
        if not ignore_error:
            logger.error(f"check_proxy_error: exception error: {detail}")
            raise Exception(detail)
    except urllib.error.URLError as url_error:
        status = 'not working'
        if not ignore_error:
            logger.error(f"check_proxy_error: url error: {url_error}")
            raise urllib.error.URLError(url_error)
    finally:
        ms = 0
        p = subprocess.Popen(["ping", "-c 1", host], stdout=subprocess.PIPE)
        resp = str(p.communicate()[0])
        if int(resp.split('transmitted, ')[1].split(' received')[0]) == 0:
            status = 'not working'
            msg_e = '0 packets transmitted'
            if not ignore_error:
                logger.error(f"check_proxy_error: {msg_e}")
                raise Exception(msg_e)

        if not p.returncode != 0:
            status = 'working'
            ms = float(resp.split('time=')[1].split('ms')[0].strip())
        else:
            status = 'not working'
            msg_e = f'return code: {p.returncode}'
            if not ignore_error:
                logger.error(f"check_proxy_error: {msg_e}")
                raise Exception(msg_e)

        logger.info(f'Proxy time: {ms}ms')
        logger.info(f'Proxy status: {status}')
