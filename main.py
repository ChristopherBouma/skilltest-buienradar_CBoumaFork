import requests
import pandas as pd
import sqlite3
import schedule
import time
import seaborn as sns
import matplotlib.pyplot as plt

url = "https://data.buienradar.nl/2.0/feed/json" #link from the buienradar endpoint

def main():
    print("Running Weather Checks:")
    CheckWeather()
    schedule.every(10).minutes.do(CheckWeather)
    print("Ten minutes for the next check:")
    while True: #There's certainly a nicer way to do this, but for now it'll do
        schedule.run_pending()
        time.sleep(30) #Sleeps for 30 seconds, so will be at most 30 seconds late


def CheckWeather():
    response = requests.get(url) #read the data from buienradar
    if response.status_code == 200:
        data = response.json()
        #print(data)
    else:
        print(f"Error: {response.status_code}")

    conn = sqlite3.connect("stationsData.db")
    cursor = conn.cursor()
    
    #Build the Weather Stations table
    cursor.execute("""
    CREATE TABLE If NOT EXISTS weatherStations (
        stationid INTEGER PRIMARY KEY,
        stationname TEXT,
        lat REAL,
        long REAL, 
        regio TEXT
    )
    """) #For some reason it wasn't able to read "lon"? So I changed it long, hope that's okay

    #Build the Weather Stations Measurements Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weatherStationMeasurements (
        measurementid INTEGER PRIMARY KEY,
        timestamp TEXT,
        temperature REAL,
        groundtemperature REAL,
        feeltemperature REAL,
        windgusts REAL,
        windspeedBft INTEGER,
        humidity INTEGER,
        precipitation REAL,
        sunpower REAL,
        stationid INTEGER,
        FOREIGN KEY (stationid) REFERENCES weatherStations(stationid) ON DELETE CASCADE
    )
    """) 

    #This wipes all the data from the tables. If we want to store a day's worth of data we can easily add a check to it, or store it somewhere after we're done. For now, I just wipe it every time.
    cursor.executescript("""
        DELETE FROM weatherStationMeasurements;
        DELETE FROM weatherStations;
        VACUUM;
    """) 
    
    weatherStationMeasurementsLocal = []    #I originally just made them locally before putting them in the table, so this is a remnant of that, figured I'd leave it to show my work
    weatherStationsLocal = []    
    shortcutStations = data.get("actual", {}).get("stationmeasurements", [])
    measurementid = 0
    for station in shortcutStations:
        #print(station.get('stationid'))
        weatherStationMeasurementsLocal.append((
            measurementid,
            station.get("timestamp"),
            station.get("temperature"),
            station.get("groundtemperature"),
            station.get("feeltemperature"),
            station.get("windgusts"),
            station.get("windspeedBft"),
            station.get("humidity"),
            station.get("precipitation"),
            station.get("sunpower"),
            station.get("stationid")
        ))

        weatherStationsLocal.append((
            station.get("stationid"),
            station.get("stationname"),
            station.get("lat"),
            station.get("lon"),
            station.get("regio")
        ))
        measurementid += 1
    
    cursor.executemany("""
    INSERT INTO weatherStationMeasurements (measurementid, timestamp, temperature, groundtemperature, feeltemperature, windgusts, windspeedBft, humidity, precipitation, sunpower, stationid)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, weatherStationMeasurementsLocal)

    cursor.executemany("""
    INSERT INTO weatherStations (stationid, stationname, lat, long, regio)
    VALUES (?,?,?,?,?)
    """, weatherStationsLocal) #Unable to read "lon" for some reason so used long!

    #print(weatherStationMeasurements)
    #print(weatherStations)


    #Questions 5-8
    findHighestTemperature(cursor)
    findAverageTemperature(cursor)
    findBiggestDiffFeelActual(cursor)
    findStationsInNorthSea(cursor)

    #Question 9
    generateBarChartForTemps(conn)

    conn.commit()
    conn.close()


#I put these all as seperate functions because it just felt right, I'm happy to conform to whatever standard you guys like to use :)

def findHighestTemperature(database):
    print("Question 5: \n")
    database.execute("""
    SELECT wSM.stationid, wSM.temperature, wS.stationname
    FROM weatherStationMeasurements wSM
    JOIN weatherStations wS ON wSM.stationid = wS.stationid
    WHERE wSM.temperature = (SELECT MAX(temperature) FROM weatherStationMeasurements)
    """)
    results = database.fetchall()
    for result in results: #in case there's multiple stations with the same temp
        print("The highest temperature is: " + str(result[1]) + " in " + str(result[2]) + " with stationid: " + str(result[0]) + "\n")


def findAverageTemperature(database):
    print("Question 6: \n")
    database.execute("""
    SELECT AVG(temperature) from weatherStationMeasurements
    """)
    results = database.fetchone()
    print("The average temperature is: " + str(results[0]) + "\n")

def findBiggestDiffFeelActual(database):
    print("Question 7: \n")
    database.execute("""
    SELECT wSM.stationid, ABS(wSM.feeltemperature - wSM.temperature) AS temp_diff, wS.stationname
    FROM weatherStationMeasurements wSM
    JOIN weatherStations wS ON wSM.stationid = wS.stationid
    ORDER BY temp_diff DESC
    """)
    results = database.fetchone()
    print("The greatest temperature diff between feel and reality is: " + str(results[1]) + " in " + str(results[2]) + " with stationid: " + str(results[0]) + "\n")

def findStationsInNorthSea(database):
    print("Question 8: \n")
    database.execute("""
    SELECT stationid, stationname FROM weatherStations WHERE regio LIKE '%Noordzee%'      
    """)
    results = database.fetchall() #in case they get sneaky and build another one
    for result in results:
        print("After a silly amount of time looking up the exact co-ordinate specifics of what counts as the North Sea, I remembered the regio element, which tells us that " + str(result[1]) + " is within the North Sea with stationid: " + str(result[0]) + "\n")
    

def generateBarChartForTemps(database): #Pretty much entirely ChatGPT genned
    weatherDF = pd.read_sql_query("SELECT * FROM weatherStationMeasurements", database)
    stationsDF = pd.read_sql_query("SELECT * FROM weatherStations", database)

    latest_temperatures = weatherDF.sort_values(by="timestamp").groupby("stationid").last().reset_index()
    latest_temperatures = latest_temperatures.merge(stationsDF, on="stationid")
    latest_temperatures = latest_temperatures.sort_values(by="temperature", ascending=False)
    plt.figure(figsize=(12, 6))
    sns.barplot(x="stationname", y="temperature", data=latest_temperatures, palette="coolwarm_r")
    plt.title("Latest Recorded Temperature for Each Weather Station")
    plt.xlabel("Weather Station")
    plt.ylabel("Temperature (°C)")
    plt.xticks(rotation=90)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.subplots_adjust(bottom=0.3)
    plt.show()



if __name__ == '__main__':
    main()