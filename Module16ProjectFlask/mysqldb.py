import mysql.connector

def insertMBTARecord(mbtaList):
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="MyNewPass",
    database="MBTAdb"
    )

    mycursor = mydb.cursor()
    #complete the following line to add all the fields from the table
    sql = "insert into mbta_buses (bearing, current_status, current_stop_sequence, id, label, longitude, latitude, occupancy_status, updated_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    for mbtaDict in mbtaList:
        #complete the following line to add all the fields from the table
        val = ( mbtaDict['bearing'], mbtaDict['current_status'], mbtaDict['current_stop_sequence'], mbtaDict['id'], mbtaDict['label'], mbtaDict['longitude'], mbtaDict['latitude'], mbtaDict['occupancy_status'], mbtaDict['updated_at'])
        mycursor.execute(sql, val)

    mydb.commit()