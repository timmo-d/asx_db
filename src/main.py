import datetime
import time
import logging
import sqlalchemy as db
import pandas as pd

from sqlalchemy import Table, Column, Integer, Date, String, Float
from sqlalchemy.exc import OperationalError
from sqlalchemy_utils import database_exists, create_database

from logging import getLogger, Formatter
import logging.handlers


def main():
    """This function is the main function to run the program.

    :returns:  Nil
    :raises: BaseException
    """

    # establish connection to database server
    engine, connection, metadata = open_db_connection()

    # Check if tables exists, if not, create them
    create_tables(engine, connection, metadata)

    # get company list from Yahoo Finance
    try:
        company_url = 'https://www.asx.com.au/asx/research/ASXListedCompanies.csv'
        print("Getting companies on ASX.")
        df_companies = pd.read_csv(company_url, skiprows=3, names=['name', 'symbol', 'group'])
        df_companies.to_sql('company', engine, if_exists='replace', index=True)
        # TODO remove obsolete pricing data from stock_price table
        logging.info("Successfully updated listed ASX companies.")
    except OperationalError as err:
        logging.warning("Unable to update company list. Error: ", err)

    # update stock_price table with latest data
    stock_price = metadata.tables['stock_price']
    i = 1
    number_of_companies = len(df_companies)

    for row in df_companies.iterrows():
        ticker = row[1][1]
        # getlast recorded date from db
        query = db.select(stock_price.columns.datestamp) \
            .where(stock_price.columns.symbol.ilike(ticker)) \
            .order_by(db.desc(stock_price.columns.datestamp))
        last_date = connection.execute(query).first()
        if last_date is None:
            last_date_epoch = '33456871'
        else:
            last_date_epoch = int(time.mktime(datetime.datetime.strptime(str(last_date[0]), "%Y-%m-%d").timetuple()))
        current_date_epoch = str(int(datetime.datetime.now().timestamp()))

        # get price data for that company between last date and yesterday from YFinance
        url = construct_url(str(ticker), str(last_date_epoch), str(current_date_epoch))

        try:
            msg = str(i) + " of " + str(number_of_companies) + " >> Adding " + ticker + ": " + url
            print(msg)
            logging.info(msg)
            df_stock_prices = pd.read_csv(url, skiprows=1,
                                          names=['datestamp', 'open', 'high', 'low', 'close', 'adjclose', 'volume',
                                                 'symbol'])
            df_stock_prices['symbol'] = ticker
            df_stock_prices = df_stock_prices.reset_index(drop=True)
            df_stock_prices.to_sql('stock_price', engine, if_exists='append', index=False)
        except BaseException as err:
            logging.error("Failed to add %s to database %s", ticker, err)
            #raise err
        i += 1


def open_db_connection():
    """This function establishes a database connection.

    :returns:  engine, connection, metadata
    :raises: None
    """
    db_addr = '192.168.1.106:3306'
    # TODO create user from here and not in database directly
    db_user = 'kodi'
    db_pass = 'kodi'
    db_name = 'asx'
    # engine = db.create_engine('dialect+driver://user:pass@host:port/db')
    try:
        # engine = db.create_engine('sqlite:///asx_db.sqlite')
        url = f"mysql+pymysql://{db_user}:{db_pass}@{db_addr}/{db_name}"
        engine = db.create_engine(url, echo=False)
        if not database_exists(engine.url):
            create_database(engine.url)
        connection = engine.connect()
        metadata = db.MetaData()
        db.MetaData.reflect(metadata, bind=engine)
        logging.info("Successfully connected to DB at %s", url)
    except OperationalError as err:
        logging.critical("Cannot connect to DB %s", err)
        raise err
    return engine, connection, metadata


def create_tables(engine, connection, metadata):
    """This function creates the necessary tables if they are not already created.

    :param engine: database engine
    :param connection: connection to database
    :param metadata: schema metadata
    :returns:  Nil
    :raises: None
    """

    # Check if Company table exists, if not, create it.
    if not engine.dialect.has_table(connection, 'company'):
        Table('company', metadata,
              Column('Id', Integer, primary_key=True, nullable=False),
              Column('name', String(100)),
              Column('symbol', String(5)),
              Column('group', String(100)),
              )

        # Implement the creation
        try:
            metadata.create_all(engine)
            logging.info("Successfully created 'Company' table.")
        except OperationalError as err:
            logging.warning("Unable to create 'Company' table. Error: %s", err)

    # Check if Stock_Price table exists, if not, create it.
    if not engine.dialect.has_table(connection, 'stock_price'):
        Table('stock_price', metadata,
              Column('Id', Integer, primary_key=True, nullable=False),
              Column('datestamp', Date),
              Column('open', Float),
              Column('high', Float),
              Column('low', Float),
              Column('close', Float),
              Column('adjclose', Float),
              Column('volume', Integer),
              Column('symbol', String(5)),
              )
        # Implement the creation
        try:
            metadata.create_all(engine)
            logging.info("Successfully created 'Stock Price' table.")
        except OperationalError as err:
            logging.warning("Unable to create 'Stock Price' table. Error: %s", err)

    # Check if Index table exists, if not, create it.
    if not engine.dialect.has_table(connection, 'index'):
        Table('index', metadata,
              Column('Id', Integer, primary_key=True, nullable=False),
              Column('name', String(100)),
              Column('symbol', String(10)),
              Column('last_date', Date),
              )
        # Implement the creation
        try:
            metadata.create_all(engine)
            logging.info("Successfully created 'Index' table.")
        except OperationalError as err:
            logging.warning("Unable to create 'Index' table. Error: %s", err)

    # Check if Index Price table exists, if not, create it.
    if not engine.dialect.has_table(connection, 'index_price'):
        Table('index_price', metadata,
              Column('Id', Integer, primary_key=True, nullable=False),
              Column('datestamp', Date),
              Column('open', Float),
              Column('high', Float),
              Column('low', Float),
              Column('close', Float),
              Column('adjclose', Float),
              Column('volume', Integer),
              Column('symbol', String(5)),
              )
        # Implement the creation
        try:
            metadata.create_all(engine)
            logging.info("Successfully created 'Index Price' table.")
        except OperationalError as err:
            logging.warning("Unable to create 'Index Price' table. Error: %s", err)


def construct_url(ticker, s_date, e_date):
    """This function constructs the correct URL string to download from Yahoo Finance.

    :param ticker: company symbol
    :param s_date: start date in epoch
    :param e_date: end date in epoch
    :returns:  str -- URL for downloading from Yahoo Finance
    :raises: None
    """

    # crypto: https://query1.finance.yahoo.com/v7/finance/download/BTC-AUD?period1=1557468908&period2=1589091308&interval=1d&events=history
    # stock: https://query1.finance.yahoo.com/v7/finance/download/BHP.AX?period1=1262304000&period2=1589155200&interval=1d&events=history
    # index: https://query1.finance.yahoo.com/v7/finance/download/^AORD?period1=1420070400&period2=1451606400&interval=1d&events=history
    # another site to retrieve data: https://www.marketindex.com.au/data-downloads

    # ticker=ticker.replace("^","%5E")
    # yfURL = "https://query1.finance.yahoo.com/v7/finance/download/" + ticker + "?period1=" + str(''.join(s_date[0])) + "&period2=" + str(e_date) + "&interval=1" + freq + "&events=history"

    # "https://au.finance.yahoo.com/quote/BHP/history?period1=1653045826&period2=1684581826&interval=1mo&filter=history&frequency=1mo&includeAdjustedClose=true")
    # yfURL = "https://au.finance.yahoo.com/quote/" + ticker + "/history?period1=1653045826&period2=1684581826&interval=1mo&filter=history&frequency=1mo&includeAdjustedClose=true"
    url = "https://query1.finance.yahoo.com/v7/finance/download/" + ticker + ".AX?period1=" + s_date + "&period2=" + e_date + "&interval=1d&events=history&includeAdjustedClose=true"
    # print(url)
    return url


def configure_logger():
    LOG_FORMAT = f"%(levelname)s:%(filename)s:%(lineno)d - %(asctime)s - %(message)s"
    logger = getLogger()
    syslogHandler = logging.handlers.SysLogHandler(address=("192.168.1.106", 514))
    logger.setLevel(logging.INFO)
    syslogHandler.setFormatter(Formatter(LOG_FORMAT))
    logger.addHandler(syslogHandler)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    configure_logger()
    main()
