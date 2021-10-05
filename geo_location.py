import json

from flask import Flask, request
from flask import Response
from pymongo import MongoClient
import requests
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from geopy.geocoders import Nominatim
from geopy import distance

app = Flask(__name__)


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


def update_max_selects_collection(db, total_requests):
    max_requests_document = db.maxRequests.find()
    max_requests = next(max_requests_document, None)
    if max_requests:
        if max_requests['requests'] < total_requests:
            db.maxRequests.update_one({"_id": max_requests["_id"]}, {"$set": {"requests": total_requests}})
    else:
        db.maxRequests.insert_one({"requests": total_requests})


@app.route('/distance')
def get_distance():
    source = request.args.get('source')
    destination = request.args.get('destination')
    if source > destination:
        locations = source + "/" + destination
    else:
        locations = destination + "/" + source
    client = MongoClient('localhost', 27017)
    if is_server_connected(client):
        db = client.homeProjects
        distance_returned = db.locations.find({"locations": locations}, {"distance": 1, "requests": 1}).limit(1)
        distance_object = next(distance_returned, None)
        if distance_object:
            total_requests = distance_object["requests"] + 1
            update_max_selects_collection(db, total_requests)
            db.locations.update_one({"locations": locations}, {"$set": {"requests": total_requests}})
            return Response(response=json.dumps({"distance": str(distance_object["distance"])}), status=200,
                            mimetype='application/json')
    try:
        distance_between_cities = get_distance_with_packages(source, destination)
        if distance_between_cities is None:
            return Response("Error: Cannot locate the locations requested please check your input", status=400)
        try:
            db = client.homeProjects
            db.locations.insert_one(
                {"locations": locations, "distance": distance_between_cities, "requests": 1},
            )
            update_max_selects_collection(db, 1)
        except ServerSelectionTimeoutError as err:
            pass
        return Response(response=json.dumps({"distance": distance_between_cities}),
                        status=200,
                        mimetype='application/json')

    except ConnectionError:
        return Response("Error: Unable to connect to our server at the moment", status=410)


@app.route('/health')
def get_health():
    client = MongoClient('localhost', 27017)
    if is_server_connected(client):
        return Response(status=200)
    else:
        return Response("Error: Unable to connect to our server at the moment", status=500)


@app.route('/popularsearch')
def get_popular_search():
    client = MongoClient('localhost', 27017)
    if is_server_connected(client):
        db = client.homeProjects
        max_requests_document = db.maxRequests.find()
        max_requests = next(max_requests_document, None)
        if max_requests:
            most_searched_query_cursor = db.locations.find({"requests": max_requests["requests"]}).limit(1)
            most_searched_query_object = next(most_searched_query_cursor, None)
            if most_searched_query_object:
                source, destination = most_searched_query_object["locations"].split('/')[0], \
                                      most_searched_query_object["locations"].split('/')[1]
                return Response(
                    response=json.dumps(
                        {"source": source, "destination": destination, "hits": most_searched_query_object["requests"]}),
                    status=200, mimetype='application/json')
        return Response(
            "Error: No searches found", status=300)
    else:
        return Response("Error: Unable to connect to our server at the moment", status=500)


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=8080)
