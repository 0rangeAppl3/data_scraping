import sys

from engine.generic_runner import GenericRunner
from engine.session import Session
from engine.spiders.dice.runner import DiceRunner
from engine.spiders.remoteok.runner import RemoteOkRunner


def get_engine(website: str, session: Session) -> GenericRunner:
    if website == "dice":
        return DiceRunner(session)
    if website == "remoteok":
        return RemoteOkRunner(session)


def main(website):
    session = Session()
    engine = get_engine(website, session)
    engine.run()


if __name__ == "__main__()":
    args = sys.argv
    main(args[1])
