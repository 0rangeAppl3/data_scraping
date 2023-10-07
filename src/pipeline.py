from engine.generic_runner import GenericRunner, NonSqlRunner
from engine.session import Session
from engine.spiders.dice.runner import DiceRunner, NonSqlDiceRunner
from engine.spiders.remoteok.runner import RemoteOkRunner, NonSqlRemoteOkRunner


def get_engine(website: str, session: Session) -> GenericRunner:
    if website == "dice":
        return DiceRunner(session)
    if website == "remoteok":
        return RemoteOkRunner(session)


def get_non_sql_engine(website: str) -> NonSqlRunner:
    if website == "dice":
        return NonSqlDiceRunner()
    if website == "remoteok":
        return NonSqlRemoteOkRunner()


def run_pipeline(website):
    session = Session()
    engine = get_engine(website, session)
    engine.run()


def run_non_sql_pipeline(website):
    engine = get_non_sql_engine(website)
    engine.run()
