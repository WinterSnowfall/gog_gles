'''
@author: Winter Snowfall
@version: 5.10
@date: 20/09/2025

Warning: Built for use with python 3.6+
'''

class ConstantsInterface:
    OPTIMIZE_QUERY = 'PRAGMA optimize'
    # value separator for multi-valued fields
    MVF_VALUE_SEPARATOR = '; '
    # number of seconds a process will wait to get/put in a queue
    QUEUE_WAIT_TIMEOUT = 10 #seconds
    # allow a process to fully load before starting the next process
    # (helps preserve process start order for logging purposes)
    PROCESS_START_WAIT_INTERVAL = 0.05 #seconds
    HTTP_OK = 200
    HTTP_NOT_FOUND = 404
    HTTP_INTERNAL_SERVER_ERROR = 500
    # list of HTTP error codes that will trigger a proxy restart
    # HTTP 425/429/509 errors will be returned in case of throttling
    PROXY_RESTART_HTTP_CODES = (425, 429, 509)
    # emulate a Firefox browser for increased compatibility
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Connection': 'keep-alive',
    }
    # set the gog_lc cookie to avoid errors bought about by GOG dynamically determining language
    COOKIES = {
        'gog_lc': 'BE_EUR_en-US'
    }
