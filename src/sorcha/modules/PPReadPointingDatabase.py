import sqlite3
import pandas as pd
import logging
import sys


def PPReadPointingDatabase(bsdbname, observing_filters, dbquery, surveyname):
    """
    Reads in the pointing database as a Pandas dataframe.

    Parameters
    -----------
    bsdbname : string
        File location of pointing database.

    observing_filters : list of strings
        List of observation filters of interest.

    dbquery : string
        Databse query to perform on pointing database.

    Returns
    -----------
    dfo : pandas dataframe
        Dataframe of pointing database.

    """

    pplogger = logging.getLogger(__name__)

    con = sqlite3.connect(f'file:{bsdbname}?mode=ro', uri=True)

    # For no more than 160 seconds (120 + 40), attempt to read the database.
    # On failure, wait a random number of seconds between 20 and 40 and try again.
    # Adds robustness to read failures, which still occur when hundreds of Sorcha
    # tasks try to read the same pointing database simultaneously even in read-only mode.

    attempt_reading_time = 0
    while True:
        try:
            df = pd.read_sql_query(dbquery, con)
            break
        except Exception as e:
            pplogger.error(
                "ERROR: PPReadPointingDatabase: SQL query on pointing database failed. Check that the query is correct in the config file."
            )
            if attempt_reading_time > 120:
                sys.exit(
                    "ERROR: PPReadPointingDatabase: SQL query on pointing database failed. Check that the query is correct in the config file."
                )
            
            delay_time = randint(20, 40)
            attempt_reading_time += delay_time
            sleep(delay_time)

    df["observationId_"] = df["observationId"]
    df = df.rename(columns={"observationId": "FieldID"})
    df = df.rename(columns={"filter": "optFilter"})  # not to confuse with the pandas filter command
    dfo = df[df.optFilter.isin(observing_filters)].copy()

    # at the moment the RubinSim pointing databases don't record the observation
    # midpoint, so we calculate it. the actual pointings might.

    # once we have the actual pointings this check could be changed to, eg,
    # lsst_sim for the RubinSim pointings, and 'lsst' would produce different
    # behaviour.
    if surveyname in ["lsst", "LSST"]:
        dfo["observationMidpointMJD_TAI"] = dfo["observationStartMJD_TAI"] + (
            (dfo["visitTime"] / 2.0) / 86400.0
        )
    else:
        pplogger.error("ERROR: PPReadPointingDatabase: survey name not recognised.")
        sys.exit("ERROR: PPReadPointingDatabase: survey name not recognised.")

    return dfo
