import logging
logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


def convert_second(sec):
    days = sec // 86400
    sec_rest = sec % 86400
    hours = sec_rest // 3600
    sec_rest = sec_rest % 3600
    minutes = sec_rest // 60
    sec_rest = sec_rest % 60
    times = {
        "d": int(days),
        "h": int(hours),
        "m": int(minutes),
        "s": int(sec_rest),
    }
    times_new = []
    for t in times:
        if times[t] == 0:
            continue
        times_new.append(f'{times[t]}{t}')
    msg = ':'.join(times_new)
    if len(times_new) == 0:
        msg = '0s'
    return msg
