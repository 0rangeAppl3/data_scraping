import sys

from engine.generic_runner import GenericRunner
from engine.spiders.dice.runner import DiceRunner
from engine.spiders.remoteok.runner import RemoteOkRunner


def get_engine(website: str) -> GenericRunner:
    if website == "dice":
        return DiceRunner()
    if website == "remoteok":
        return RemoteOkRunner()


def main(website):
    engine = get_engine(website)
    engine.run()
    scraping(scraping_web.run)()


if __name__ == "__main__()":
    args = sys.argv
    main(args[1])
