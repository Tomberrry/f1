import requests
import pyodbc
import pandas as pd

# Konfiguration für die Verbindung zur MSSQL-Datenbank
server = 'MICHAELKLASSEN\SQLEXPRESS'
database = 'F1'
username = 'sa'
password = 'sa'
connection_string = f'DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'



# Funktion zum Herunterladen der Konstrukteursdaten
def download_and_insert_constructors(connection_string):
    
    # Verbindung zur MSSQL-Datenbank herstellen
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    api_url = 'https://ergast.com/api/f1/constructors.json?limit=30000'

    try:
        # API-Anfrage
        response = requests.get(api_url)
        data = response.json()
        
        insertCount = 0
        # Tabelle bereinigen
        delete_query = f"DELETE FROM Constructors"
        #cursor.execute(delete_query)
        #connection.commit()

        # Überprüfen, ob die Anfrage erfolgreich war und 'name' vorhanden sind
        if response.status_code == 200 and 'MRData' in data and 'ConstructorTable' in data['MRData'] and 'Constructors' in data['MRData']['ConstructorTable']:
            constructors = data['MRData']['ConstructorTable']['Constructors']

            

            # Einträge in die Constructors-Tabelle einfügen
            for constructor in constructors:
                constructor_id = str(constructor['constructorId'])
                constructor_id = constructor_id.replace("-","")
                constructor_name = constructor['name']
                                
                

                # SQL-Abfrage zum Einfügen des Datensatzes
                insert_query = f"BEGIN IF NOT EXISTS (Select * FROM Constructors) BEGIN INSERT INTO Constructors (ConstructorId, ConstructorName) VALUES ('{constructor_id}','{constructor_name}') END END"
                print(insert_query)

                # Datensatz einfügen
                cursor.execute(insert_query)
                connection.commit()

            print("Daten wurden erfolgreich in die Tabelle Constructors eingefügt. Anzahl " + str(insertCount))

        else:
            print(f"Fehler beim Abrufen der Daten von der API. Statuscode: {response.status_code}")

    except Exception as e:
        print(f"Exception: {e}")

    finally:
        # Verbindung und Cursor schließen, wenn sie erstellt wurden
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# Funktion zum Herunterladen der Fahrerdaten
def download_and_insert_drivers(connection_string):
    
    # Verbindung zur MSSQL-Datenbank herstellen
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    api_url = 'https://ergast.com/api/f1/drivers.json?limit=30000'

    try:
        # API-Anfrage
        response = requests.get(api_url)
        data = response.json()
        
        # Tabelle bereinigen
        delete_query = f"DELETE FROM Drivers"
        cursor.execute(delete_query)
        connection.commit()
        
        
        # Überprüfen, ob die Anfrage erfolgreich war und 'name' vorhanden sind
        if response.status_code == 200 and 'MRData' in data and 'DriverTable' in data['MRData'] and 'Drivers' in data['MRData']['DriverTable']:
            drivers = data['MRData']['DriverTable']['Drivers']

            

            # Einträge in die Drivers-Tabelle einfügen
            for driver in drivers:
                driver_id = str(driver['driverId'])
                driver_id = driver_id.replace("-","")
                driver_name = str(driver['givenName'])
                driver_name = driver_name.replace("'","''")
                driver_surname = str(driver['familyName'])
                driver_surname = driver_surname.replace("'","''")
                driver_dateOfBirth = driver['dateOfBirth']
                driver_nationality = driver['nationality']
                
                driver_name = driver_name.replace("'","''")      
                                 
                # SQL-Abfrage zum Einfügen des Datensatzes
                insert_query = f"INSERT INTO Drivers (DriverId, DriverName, DriverSurname, DateOfBirth, Nationality) VALUES ('{driver_id}','{driver_name}','{driver_surname}','{driver_dateOfBirth}','{driver_nationality}')"
                
                # Datensatz einfügen
                cursor.execute(insert_query)
                connection.commit()

            print("Daten wurden erfolgreich in die Tabelle Drivers eingefügt.")

        else:
            print(f"Fehler beim Abrufen der Daten von der API. Statuscode: {response.status_code}")

    except Exception as e:
        print(f"Exception: {e}")

    finally:
        # Verbindung und Cursor schließen, wenn sie erstellt wurden
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Funktion zum Herunterladen der Streckendaten
def download_and_insert_circuits(connection_string):
    
    # Verbindung zur MSSQL-Datenbank herstellen
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    api_url = 'https://ergast.com/api/f1/circuits.json?limit=300'

    try:
        # API-Anfrage
        response = requests.get(api_url)
        data = response.json()
        
        # Tabelle bereinigen
        delete_query = f"DELETE FROM Circuits"
        cursor.execute(delete_query)
        connection.commit()
        
        
        # Überprüfen, ob die Anfrage erfolgreich war und 'name' vorhanden sind
        if response.status_code == 200 and 'MRData' in data and 'CircuitTable' in data['MRData'] and 'Circuits' in data['MRData']['CircuitTable']:
            circuits = data['MRData']['CircuitTable']['Circuits']

            

            # Einträge in die Circuits-Tabelle einfügen
            for circuit in circuits:
                circuit_id = str(circuit['circuitId'])
                circuit_id = circuit_id.replace("-","")
                circuit_name = str(circuit['circuitName'])
                circuit_name = circuit_name.replace("'","''")
                                 
                # SQL-Abfrage zum Einfügen des Datensatzes
                insert_query = f"INSERT INTO Circuits (CircuitId, CircuitName) VALUES ('{circuit_id}','{circuit_name}')"

                # Datensatz einfügen
                cursor.execute(insert_query)
                connection.commit()

            print("Daten wurden erfolgreich in die Tabelle Circuits eingefügt.")

        else:
            print(f"Fehler beim Abrufen der Daten von der API. Statuscode: {response.status_code}")

    except Exception as e:
        print(f"Exception: {e}")

    finally:
        # Verbindung und Cursor schließen, wenn sie erstellt wurden
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# Funktion zum Herunterladen der Renndaten
def download_and_insert_races(connection_string):
    
    # Verbindung zur MSSQL-Datenbank herstellen
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    api_url = 'https://ergast.com/api/f1/'

    try:
        
        #Abfragen pro Saison
        season_query = f"Select Season from Seasons"
        #cursor.execute(season_query)
        #connection.commit()
        
        df = pd.read_sql_query(season_query,connection,'Season')

        # Tabelle bereinigen
        delete_query = f"DELETE FROM Races"
        cursor.execute(delete_query)
        connection.commit()
        
        for season, rows in df.iterrows():
            season = str(season).strip()
            api_url_season = (api_url + season+".json")
            # API-Anfrage
            response = requests.get(api_url_season)
            data = response.json()
            

            # Überprüfen, ob die Anfrage erfolgreich war und 'name' vorhanden sind
            if response.status_code == 200 and 'MRData' in data and 'RaceTable' in data['MRData'] and 'Races' in data['MRData']['RaceTable']:
                races = data['MRData']['RaceTable']['Races']

                

                # Einträge in die Races-Tabelle einfügen
                for race in races:
                    race_id = race['season'] + '-' + race['round']
                    race_seasonId = race['season']
                    race_round = race['round']
                    race_name = race['raceName']
                    race_date = race['date']
                    race_circuit = race['Circuit']['circuitId']
                    race_circuit = race_circuit.replace("-","")
                    
                                    
                    # SQL-Abfrage zum Einfügen des Datensatzes
                    insert_query = f"INSERT INTO Races (RaceId, SeasonId, Round, RaceName, Date, CircuitId) VALUES ('{race_id}','{race_seasonId}','{race_round}','{race_name}','{race_date}','{race_circuit}')"

                    # Datensatz einfügen
                    cursor.execute(insert_query)
                    connection.commit()

                print("Daten wurden erfolgreich in die Tabelle Races eingefügt.")

            else:
                print(f"Fehler beim Abrufen der Daten von der API. Statuscode: {response.status_code}")

    except Exception as e:
        print(f"Exception: {e}")

    finally:
        # Verbindung und Cursor schließen, wenn sie erstellt wurden
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Funktion zum Herunterladen der Ergebnisdaten
def download_and_insert_results(connection_string):
    
    # Verbindung zur MSSQL-Datenbank herstellen
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    api_url = 'https://ergast.com/api/f1/'

    try:
        
        #Abfragen pro Saison
        season_query = f"Select Season from Seasons"
        
        #cursor.execute(season_query)
        #connection.commit()
        
        df = pd.read_sql_query(season_query,connection,'Season')

        # Tabelle bereinigen
        delete_query = f"DELETE FROM Results"
        cursor.execute(delete_query)
        connection.commit()
        idCount = 0
        
        for season, rows in df.iterrows():
            season = str(season).strip()
            api_url_result = (api_url + season+"/results.json?limit=30000000")
            # API-Anfrage
            response = requests.get(api_url_result)
            data = response.json()
            
            race_query = f"Select Round from Races where SeasonId =" +season
            df2 = pd.read_sql_query(race_query,connection,'Round')
            
            round = 1 
            #for round, rows in df2.iterrows():
            #    round = int(str(round).strip())
                #print (data['MRData']['RaceTable']['Races'][0]['Results'])
                # Überprüfen, ob die Anfrage erfolgreich war und 'name' vorhanden sind
            if response.status_code == 200 and 'MRData' in data and 'RaceTable' in data['MRData'] and 'Races' in data['MRData']['RaceTable']:
                
                    
                races = data['MRData']['RaceTable']['Races']
                # Einträge in die Results-Tabelle einfügen
                for race in races:
                    
                    results = data['MRData']['RaceTable']['Races'][round-1]['Results']
                    #print(results)
                    
                    for result in results:
                        resultId = idCount
                        seasonId = race['season']
                        raceId = race['season']+'-'+race['round']
                        driverId = result['Driver']['driverId']
                        driverId = driverId.replace("-","")
                        laps = result['laps']
                        grid = result['grid']
                        #If +2 Laps, no Time in Data
                        time=''
                        if(['Time'] in data['MRData']['RaceTable']['Races'][round-1]['Results']):
                            time = result['Time']['millis']
                        status = result['status']
                        points = result['points']
                        constructorId = str(result['Constructor']['constructorId'])
                        constructorId = constructorId.replace("-","")
                        pos = result['position']
                        number = result['number']
                        posText = result['positionText']
                        
                        idCount += 1                                 
                        # SQL-Abfrage zum Einfügen des Datensatzes
                        insert_query = f"INSERT INTO [dbo].[Results] ([ResultId],[SeasonId],[RaceId],[DriverId],[Laps],[Grid],[Time],[Status],[Points],[ConstructorId],[Pos],[Number],[PosText]) VALUES ('{resultId}','{seasonId}','{raceId}','{driverId}','{laps}','{grid}','{time}','{status}','{points}','{constructorId}','{pos}','{number}','{posText}')"
                        
                        print(insert_query)
                        
                        # Datensatz einfügen
                        cursor.execute(insert_query)
                        connection.commit()
                    round += 1
                print("Daten wurden erfolgreich in die Tabelle Results eingefügt.")

            else:
                print(f"Fehler beim Abrufen der Daten von der API. Statuscode: {response.status_code}")

    except Exception as e:
        print(f"Exception: {e}")

    finally:
        # Verbindung und Cursor schließen, wenn sie erstellt wurden
        if cursor:
            cursor.close()
        if connection:
            connection.close()



# Konstrukteursdaten laden aufrufen (DONE)
#download_and_insert_constructors(connection_string)

# Fahrerdaten laden aufrufen (DONE)
#download_and_insert_drivers(connection_string)

# Streckendaten laden aufrufen (DONE)
#download_and_insert_circuits(connection_string)

# Renndaten laden aufrufen (DONE)
#download_and_insert_races(connection_string)

# Ergebnisdaten laden aufrufen (DONE)
download_and_insert_results(connection_string)

