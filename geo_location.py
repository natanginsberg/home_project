from flask import Flask, request
from flask import Response
from pymongo import MongoClient
import requests
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from geopy.geocoders import Nominatim
from geopy import distance

app = Flask(__name__)

location_url = url = "https://maps.googleapis.com/maps/api/distancematrix/json?origins=40.6655101%2C-73.89188969999998&destinations=40.659569%2C-73.933783%7C40.729029%2C-73.851524%7C40.6860072%2C-73.6334271%7C40.598566%2C-73.7527626&key=YOUR_API_KEY"


@app.route('/')
@app.route('/hello')
def hello_world():
    return Response(status=200)


def is_server_connected(client):
    try:
        client.admin.command('ismaster')
        return True
    except ConnectionFailure:
        return False


def get_distance_with_packages(source, destination):
    geolocator = Nominatim(user_agent="Your_Name")
    location_1 = geolocator.geocode(source)
    location_2 = geolocator.geocode(destination)
    if location_1 is None or location_2 is None:
        return None
    return distance.distance((location_1.latitude, location_1.longitude),
                             (location_2.latitude, location_2.longitude)).km


@app.route('/distance')
def get_distance():
    source = request.args.get('source')
    destination = request.args.get('destination')
    if source > destination:
        locations = source + " " + destination
    else:
        locations = destination + " " + source
    client = MongoClient('localhost', 27017)
    if is_server_connected(client):
        db = client.homeProjects
        distance_returned = db.locations.find({"id": locations}, {"distance": 1, "requests": 1}).limit(1)
        distance_object = next(distance_returned, None)
        if distance_object:
            db.locations.update_one({"id": locations}, {"$set": {"requests": distance_object["requests"] + 1}})
            return Response(str(distance_object["distance"]), status=200)
    try:
        distance_between_cities = get_distance_with_packages(source, destination)
        if distance_between_cities is None:
            return Response("Error: Cannot locate the locations requested", status=400)
        try:
            db = client.homeProjects
            db.locations.insert_one({"id": locations, "distance": distance_between_cities, "requests": 1})
        except ServerSelectionTimeoutError as err:
            pass
        return Response(str(distance_between_cities), status=200)

    except ConnectionError:
        return Response("Error: Unable to connect to our server at the moment", status=410)


@app.route('/health')
def get_health():
    client = MongoClient('localhost', 27017)
    if is_server_connected(client):
        return Response(status=200)
    else:
        Response("Error: Unable to connect to our server at the moment", status=500)




if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=8080)
