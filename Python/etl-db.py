import sys
import os
import pandas
import configparser
import petl
import datetime
import pyodbc

def main():
    print('Starting the ETL process')

    #url_consoles_csv = 'https://raw.githubusercontent.com/walmartdigital/de-challenge/main/data/consoles.csv'
    #url_results_csv = 'https://raw.githubusercontent.com/walmartdigital/de-challenge/main/data/result.csv'
    #mssql_odbcDriver = 'SQL Server'
    #mssql_server = 'localhost,1433'
    #mssql_database = 'ETL_DB'
    #mssql_user = 'sa'
    #mssql_pass = '!etl_DB1'

    #get datafile config
    config = configparser.ConfigParser()
    try:
        config.read('Config.ini')
    except Exception as e:
        print('cound not read the config.ini file' + str(e))
        sys.exit()

    #read setting from configuration file
    url_consoles_csv = config['CONFIG']['url_consoles_csv']
    url_results_csv = config['CONFIG']['url_results_csv']
    mssql_odbcDriver = config['CONFIG']['odbcDriver']
    mssql_server = config['CONFIG']['server']
    mssql_database = config['CONFIG']['database']
    mssql_user = config['CONFIG']['user']
    mssql_pass = config['CONFIG']['pass']

    #load consoles.csv file
    print('loading consoles.csv file...')
    try:
        consoles_csv = petl.fromcsv(url_consoles_csv)
    except Exception as e:
        print('could not read the source file' + str(e))
        sys.exit()


    ###############################################
    ###### transformations for company table ######
    ###############################################

    print('creating company table...')
    #clean white spaces from company columns
    company_string = []
    for s in consoles_csv['company']:
        company_string.append(s.strip())

    #create petl table
    company_table = petl.fromcolumns([company_string], ['company_name'])

    #select distinct values
    company_table = petl.distinct(company_table)

    #sort table and add id column 
    company_table = petl.sort(company_table,'company_name')
    company_table = petl.addrownumbers(company_table)
    company_table = petl.rename(company_table, {'row': 'id_company'})

    print('company table created successfully')


    ##############################################
    ###### transformations for console table #####
    ##############################################

    print('creating console table...')
    #clean white spaces from console column
    console_string = []
    for s in consoles_csv['console']:
        console_string.append(s.strip())

    #create petl table
    console_table = petl.fromcolumns([console_string, company_string], ['console_name', 'company_name'])

    #select distinct values
    console_table = petl.distinct(console_table)

    #join with company table to get id_company
    console_table = petl.leftjoin(console_table, company_table, None, 'company_name', 'company_name')

    #sort table and add id column 
    console_table = petl.sort(console_table,'console_name')
    console_table = petl.addrownumbers(console_table)
    console_table = petl.rename(console_table, {'row': 'id_console', 'id_company': 'company_id_company'})

    #select requiered columns
    console_table = petl.cut(console_table, 'id_console', 'console_name', 'company_id_company')

    print('console table created successfully')

    #load result.csv file
    print('loading results.csv file...')
    try:
        result_csv = petl.fromcsv(url_results_csv)
    except Exception as e:
        print('could not read the source file' + str(e))
        sys.exit()


    ###############################################
    ##### transformations for videogame table #####
    ###############################################

    print('creating videogame table...')
    #clean white spaces from name column
    videogame_string = []
    for s in result_csv['name']:
        videogame_string.append(s.strip())

    #create petl table
    videogame_table = petl.fromcolumns([videogame_string], ['videogame_name'])

    #select distinct values
    videogame_table = petl.distinct(videogame_table)

    #sort table and add id column 
    videogame_table = petl.sort(videogame_table,'videogame_name')
    videogame_table = petl.addrownumbers(videogame_table)
    videogame_table = petl.rename(videogame_table, {'row': 'id_videogame'})

    print('videogame table created successfully')

    ###############################################
    ## transformations for videogame score table ##
    ###############################################

    print('creating videogame_score table...')
    #clean white spaces from date column and transform to date datatype
    date_string = []
    date_format = []
    for s in result_csv['date']:
        date_string.append(s.strip())
        date_format.append(datetime.datetime.strptime(s.strip(), '%b %d, %Y'))

    #clean white spaces from console column
    console_string = []
    for s in result_csv['console']:
        console_string.append(s.strip())

    #cut requiered columns and change columns datatype
    videogame_score_table = petl.cut(result_csv, 'metascore', 'userscore')
    videogame_score_table = petl.convert(videogame_score_table, 'metascore', int)
    videogame_score_table = petl.convert(videogame_score_table, 'userscore', float)

    #add columns to previous table
    videogame_score_table = petl.addcolumn(videogame_score_table, 'date', date_format)
    videogame_score_table = petl.addcolumn(videogame_score_table, 'console_name', console_string)
    videogame_score_table = petl.addcolumn(videogame_score_table, 'videogame_name', videogame_string)

    #join with console and videogame tables to get ids
    videogame_score_table = petl.leftjoin(videogame_score_table, videogame_table, None, 'videogame_name', 'videogame_name')
    videogame_score_table = petl.leftjoin(videogame_score_table, console_table, None, 'console_name', 'console_name')

    #select requiered columns and sort by date
    videogame_score_table = petl.cut(videogame_score_table, 'date', 'metascore', 'userscore', 'id_videogame', 'id_console')
    videogame_score_table = petl.sort(videogame_score_table, 'date', True)
    videogame_score_table = petl.rename(videogame_score_table, {'id_videogame': 'videogame_id_videogame', 'id_console': 'console_id_console'})

    print('videogame_score table created successfully')

    ###############################################
    ###########   database connection   ###########
    ###############################################

    print('connecting to database...')
    #connectionString = 'Driver=%s;Server=%s;Database=%s;Trusted_Connection=yes' %(mssql_odbcDriver, mssql_server, mssql_database)
    connectionString = 'Driver=%s;Server=%s;Database=%s;UID=%s;PWD=%s' %(mssql_odbcDriver, mssql_server, mssql_database, mssql_user, mssql_pass)
    try:
        dbConnection = pyodbc.connect(connectionString)
    except Exception as e:
        print('could not connect to database: ' + str(e))
        sys.exit()


    ###############################################
    ###########   database insert   ###############
    ###############################################

    #insert to company table
    print('insert data into company table...')
    try:
        petl.io.todb(company_table, dbConnection, 'company')
    except Exception as e:
        print('could not write to table company: ' + str(e))
        sys.exit()

    print('insert into company table complete')

    #insert to console table
    print('insert data into console table...')
    try:
        petl.io.todb(console_table, dbConnection, 'console')  
    except Exception as e:
        print('could not write to table console: ' + str(e))
        sys.exit()

    print('insert into console table complete')

    #insert to videogame table
    print('insert data into videogame table...')
    try:
        petl.io.todb(videogame_table, dbConnection, 'videogame')
    except Exception as e:
        print('could not write to table videogame: ' + str(e))
        sys.exit()

    print('insert into videogame table complete')

    #insert to videogame_score table
    print('insert data into videogame_score table...')
    try:
        petl.io.todb(videogame_score_table, dbConnection, 'videogame_score') 
    except Exception as e:
        print('could not write to table videogame_score: ' + str(e))
        sys.exit()

    print('insert into videogame_score table complete')

    print('process completed successfully')

    best_games_consoles_query = 'SELECT A.top_score,B.console_name,D.company_name,C.videogame_name,A.metascore FROM (SELECT *	,ROW_NUMBER() OVER (PARTITION BY console_id_console ORDER BY metascore DESC, date DESC) AS top_score FROM videogame_score) AS A LEFT JOIN console AS B ON A.console_id_console = B.id_console LEFT JOIN videogame AS C ON A.videogame_id_videogame = C.id_videogame LEFT JOIN company AS D ON B.company_id_company = D.id_company WHERE A.top_score <= 10 ORDER BY B.console_name, A.top_score'
    best_games_consoles = petl.fromdb(dbConnection, best_games_consoles_query)
    best_games_consoles = petl.convert(best_games_consoles, 'metascore', int)
    print('\nTop 10 Best Videogames for each console')
    print(petl.lookall(best_games_consoles))
    input("Press Enter to continue...")

    worst_games_consoles_query = 'SELECT A.top_score,B.console_name,D.company_name,C.videogame_name,A.metascore FROM (SELECT *	,ROW_NUMBER() OVER (PARTITION BY console_id_console ORDER BY metascore ASC, date DESC) AS top_score FROM videogame_score) AS A LEFT JOIN console AS B ON A.console_id_console = B.id_console LEFT JOIN videogame AS C ON A.videogame_id_videogame = C.id_videogame LEFT JOIN company AS D ON B.company_id_company = D.id_company WHERE A.top_score <= 10 ORDER BY B.console_name, A.top_score'
    worst_games_consoles = petl.fromdb(dbConnection, worst_games_consoles_query)
    worst_games_consoles = petl.convert(worst_games_consoles, 'metascore', int)
    print('\nTop 10 Worst Videogames for each console')
    print(petl.lookall(worst_games_consoles))
    input("Press Enter to continue...")

    best_games_company_query = 'SELECT A.top_score,B.company_name,C.videogame_name,A.metascore FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY company_id_company ORDER BY metascore DESC, date DESC) AS top_score FROM videogame_score AS A LEFT JOIN console AS B ON A.console_id_console = B.id_console) AS A LEFT JOIN company AS B ON A.company_id_company = B.id_company LEFT JOIN videogame AS C ON A.videogame_id_videogame = C.id_videogame WHERE A.top_score <= 10 ORDER BY A.company_id_company, A.top_score'
    best_games_company = petl.fromdb(dbConnection, best_games_company_query)
    best_games_company = petl.convert(best_games_company, 'metascore', int)
    print('\nTop 10 Best Videogames for each company')
    print(petl.lookall(best_games_company))
    input("Press Enter to continue...")

    worst_games_company_query = 'SELECT A.top_score,B.company_name,C.videogame_name,A.metascore FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY company_id_company ORDER BY metascore ASC, date DESC) AS top_score FROM videogame_score AS A LEFT JOIN console AS B ON A.console_id_console = B.id_console) AS A LEFT JOIN company AS B ON A.company_id_company = B.id_company LEFT JOIN videogame AS C ON A.videogame_id_videogame = C.id_videogame WHERE A.top_score <= 10 ORDER BY A.company_id_company, A.top_score'
    worst_games_company = petl.fromdb(dbConnection, worst_games_company_query)
    worst_games_company = petl.convert(worst_games_company, 'metascore', int)
    print('\nTop 10 Worst Videogames for each company')
    print(petl.lookall(worst_games_company))
    input("Press Enter to continue...")

    best_game_query = 'SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY C.metascore DESC, C.score_date ASC) AS top_score,C.videogame_name,C.metascore FROM (SELECT MIN(date) as score_date,B.videogame_name,MAX(A.metascore) AS metascore FROM videogame_score AS A LEFT JOIN videogame AS B ON A.videogame_id_videogame = B.id_videogame GROUP BY B.videogame_name) AS C ORDER BY top_score ASC'
    best_games = petl.fromdb(dbConnection, best_game_query)
    best_games = petl.convert(best_games, 'metascore', int)
    print('\nTop 10 Best Videogames')
    print(petl.lookall(best_games))
    input("Press Enter to continue...")

    worst_game_query = 'SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY C.metascore ASC, C.score_date ASC) AS top_score,C.videogame_name,C.metascore FROM (SELECT MIN(date) as score_date,B.videogame_name,MIN(A.metascore) AS metascore FROM videogame_score AS A LEFT JOIN videogame AS B ON A.videogame_id_videogame = B.id_videogame GROUP BY B.videogame_name) AS C ORDER BY top_score ASC'
    worst_game = petl.fromdb(dbConnection, worst_game_query)
    worst_game = petl.convert(worst_game, 'metascore', int)
    print('\nTop 10 Worst Videogames')
    print(petl.lookall(worst_game))
    input("Press Enter to continue...")

if __name__ == '__main__':
    main()