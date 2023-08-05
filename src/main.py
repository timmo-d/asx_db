import datetime
import time
import logging
import sqlalchemy as db
import pandas as pd

from sqlalchemy import Table, Column, Integer, Date, String, Float
from sqlalchemy.exc import OperationalError


def main():
    """This function is the main function to run the program.

    :returns:  Nil
    :raises: None
    """

    # establish connection to database server
    engine, connection, metadata = open_db_connection()

    # Check if tables exists, if not, create them
    create_tables(engine, connection, metadata)

    # get company list from Yahoo Finance
    company_url = 'https://www.asx.com.au/asx/research/ASXListedCompanies.csv'
    print("Getting companies on ASX.")
    df_companies = pd.read_csv(company_url, skiprows=3, names=['name', 'symbol', 'group'])
    df_companies.to_sql('company', engine, if_exists='replace', index=True)
    # TODO remove obsolete pricing data from stock_price table

    # update stock_price table with latest data
    stock_price = metadata.tables['stock_price']
    i = 1
    for row in df_companies.iterrows():
        number_of_companies = len(df_companies)
        ticker = row[1][1]
        # getlast record date
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
            print(str(i) + " of " + str(number_of_companies) + " >> Adding " + ticker + ": " + url)
            df_stock_prices = pd.read_csv(url, skiprows=1, names=['datestamp', 'open', 'high', 'low', 'close', 'adjclose', 'volume', 'symbol'])
            df_stock_prices['symbol'] = ticker
            df_stock_prices = df_stock_prices.reset_index(drop=True)
            df_stock_prices.to_sql('stock_price', engine, if_exists='append', index=False)
        except OperationalError as err:
            logging.error("Failed to add %s to database %s", ticker, err)
            raise err
        i += 1


def open_db_connection():
    """This function establishes a database connection.

    :returns:  engine, connection, metadata
    :raises: None
    """

    # engine = db.create_engine('dialect+driver://user:pass@host:port/db')
    try:
        engine = db.create_engine('sqlite:///asx_db.sqlite')
        connection = engine.connect()
        metadata = db.MetaData()
        db.MetaData.reflect(metadata, bind=engine)
    except OperationalError as err:
        logging.error("Cannot connect to DB %s", err)
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
              Column('name', String),
              Column('symbol', String),
              Column('group', String),
              )
        # Implement the creation
        metadata.create_all(engine)

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
              Column('symbol', String),
              )
        # Implement the creation
        metadata.create_all(engine)

    # Check if Index table exists, if not, create it.
    if not engine.dialect.has_table(connection, 'index'):
        Table('index', metadata,
              Column('Id', Integer, primary_key=True, nullable=False),
              Column('name', String),
              Column('symbol', String),
              Column('last_date', Date),
              )
        # Implement the creation
        metadata.create_all(engine)

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
              Column('symbol', String),
              )
        # Implement the creation
        metadata.create_all(engine)


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
    #print(url)
    return url


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
