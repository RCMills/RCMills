from threading import Timer
from flask import Flask, render_template
import time
import json
from MBTAApiClient import callMBTAApi

# ------------------
#    BUS LOCATION  
# ------------------

# Initialize buses list by doing an API call to the MBTA database below
buses = None

#Update the function below
def update_data():
    return callMBTAApi() #returns a list of bus data as defined in the method


def status(buses):
    for bus in buses:
        print(f"lat: {bus['latitude']}, long: {bus['longitude']}")


def timeloop():
    print(f'--- ' + time.ctime() + ' ---')
    buses = update_data()
    #status(buses)
    Timer(10, timeloop).start()
timeloop()

# ----------------
#    WEB SERVER  
# ----------------

# create application instance
app = Flask(__name__)

# root route - landing page
@app.route('/')
def root():
    return render_template('index.html')

# root route - landing page
@app.route('/location')
def location():
    #print(buses)
    return (json.dumps(callMBTAApi()))


# start server - note the port is 3000
if __name__ == '__main__':
    app.run(port=3000)