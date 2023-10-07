import logging
from random import Random
from time import sleep

import requests

PROXIES = [
    "http://190.119.76.68:8080",
    "http://189.222.251.183:999",
    "http://189.201.242.43:999",
]


def get(url, params, cookies, headers, max_retries=3, sleep_time=2, backoff=False):
    attempt = 0
    while attempt <= max_retries:
        try:
            attempt += 1
            sleep(0.5)
            response = requests.get(url, params=params, cookies=cookies, headers=headers)
            if response.status_code == 503:
                raise ValueError("Got 503, gotta slow down")
            return response
        except Exception as e:
            logging.warning("Failed to fetch {}".format(url), e)
            if backoff:
                sleep(sleep_time ** attempt)
            else:
                sleep(sleep_time)
    logging.warning("Failed to fetch {} after {} attempts".format(url, max_retries))


def get_with_proxy(url, params, cookies, headers, max_retries=3, sleep_time=2, backoff=False):
    attempt = 0
    while attempt <= max_retries:
        try:
            attempt += 1
            proxy = PROXIES[Random().randint(0, len(PROXIES) - 1)]
            proxies = {
                "http": proxy,
                "https": proxy
            }
            sleep(0.5)
            response = requests.get(url, params=params, cookies=cookies, headers=headers, proxies=proxies)
            if response.status_code == 503:
                continue
            return response
        except Exception as e:
            logging.warning("Failed to fetch {}".format(url), e)
            if backoff:
                sleep(sleep_time ** attempt)
            else:
                sleep(sleep_time)
    logging.warning("Failed to fetch {} after {} attempts".format(url, max_retries))
