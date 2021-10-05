import json

from flask import Flask, request
from flask import Response
from pymongo import MongoClient
import requests
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from geopy.geocoders import Nominatim
from geopy import distance

TIMED_OUT_ERROR = "Error: Server timed out"
NO_CONNECTION_ERROR = "Error: Unable to connect to our server at the moment"
NO_SEARCHES_FOUND_ERROR = "Error: No searches found"
INVALID_INPUT_ERROR = "Error: Cannot locate the locations requested please check your input"
SERVER_ERROR = "Error: We are unable to get the distance at the moment"
DISTANCE_NOT_FOUND_ERROR = "Error: Unable to find the distance between the two locations"

app = Flask(__name__)


class DistanceNotFoundException(Exception):
    """Raised when the distance was not found"""
    pass

class InvalidInputException(Exception):
    """Raised when the input value is too small"""
    pass


@app.route('/')
@app.route('/hello')
def hello_world():
    return Response(status=200)


def get_distance_with_packages(source, destination):
    geolocator = Nominatim(user_agent="Your_Name")
    location_1 = geolocator.geocode(source)
    location_2 = geolocator.geocode(destination)
    if location_1 is None or location_2 is None:
        return None
    return distance.distance((location_1.latitude, location_1.longitude),
                             (location_2.latitude, location_2.longitude)).km


def update_max_selects_collection(db, total_hits):
    max_hits_document = db.maxRequests.find()
    max_hits = next(max_hits_document, None)
    if max_hits:
        if max_hits['hits'] < total_hits:
            db.maxRequests.update_one({"_id": max_hits["_id"]}, {"$set": {"hits": total_hits}})
    else:
        db.maxRequests.insert_one({"hits": total_hits})


def add_data_db(db, locations, distance_between_cities, total_hits):
    db.locations.insert_one(
        {"locations": locations, "distance": distance_between_cities, "hits": total_hits},
    )
    if total_hits > 1 or (total_hits == 1 and db.maxRequests.count_documents == 0):
        update_max_selects_collection(db, total_hits)


def check_if_data_exists(client, locations):
    db = client.homeProjects
    distance_returned = db.locations.find({"locations": locations}, {"distance": 1, "hits": 1}).limit(1)
    distance_object = next(distance_returned, None)
    return distance_object


def get_distance_with_google_maps(source, destination):
    url = "https://maps.googleapis.com/maps/api/distancematrix/json?origins=%s&destinations=%s&key=AIzaSyDGvN2RbPJ1c-13CGv3DDruF7MVx5wECNo" % (
        source, destination)
    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    if response.ok:
        json_data = json.loads(response.text)
        if json_data["status"] == 'OK':
            if "distance" in json_data["rows"][0]["elements"][0]:
                return json_data["rows"][0]["elements"][0]["distance"]["value"]
            else:
                raise DistanceNotFoundException
    raise InvalidInputException


def get_location_string(source, destination):
    if source > destination:
        return source + "/" + destination
    else:
        return destination + "/" + source


def distance_getter():
    source = request.args.get('source')
    destination = request.args.get('destination')
    location_string = get_location_string(source, destination)
    client = MongoClient('localhost', 27017)
    try:
        db = client.homeProjects
        existing_query = check_if_data_exists(client, location_string)
        if existing_query:
            total_hits = existing_query["hits"] + 1
            update_max_selects_collection(db, total_hits)
            db.locations.update_one({"locations": location_string}, {"$set": {"hits": total_hits}})
            return Response(response=json.dumps({"distance": str(existing_query["distance"])}), status=200,
                            mimetype='application/json')
    except ConnectionFailure:
        pass
    except ServerSelectionTimeoutError:
        pass
    try:
        distance_between_cities = get_distance_with_google_maps(source, destination) / 1000
        if distance_between_cities is None:
            return Response(INVALID_INPUT_ERROR, status=400)
        try:
            add_data_db(client.homeProjects, location_string, distance_between_cities, 1)
        except ServerSelectionTimeoutError as err:
            pass
        except ConnectionFailure:
            pass
        return Response(response=json.dumps({"distance": distance_between_cities}),
                        status=200,
                        mimetype='application/json')
    # these are internet thrown exception thus the search failed
    except InvalidInputException:
        return Response(INVALID_INPUT_ERROR, status=420)
    except DistanceNotFoundException:
        return Response(DISTANCE_NOT_FOUND_ERROR, status=430)
    except requests.exceptions.RequestException:
        return Response(SERVER_ERROR, status=430)


def post_distance():
    client = MongoClient('localhost', 27017)
    try:
        request_data = request.json
        source = request_data["source"]
        destination = request_data["destination"]
        location_string = get_location_string(source, destination)
        distance_between_cities = request_data["distance"]
        db = client.homeProjects
        try:
            existing_query = check_if_data_exists(client, location_string)
            if existing_query:
                db.locations.update_one({"locations": location_string}, {"$set": {"distance": distance_between_cities}})
                return Response(
                    response=json.dumps(
                        {"source": source, "destination": destination, "hits": existing_query["hits"]}), status=201,
                    mimetype='application/json')
            else:
                add_data_db(client.homeProjects, location_string, distance_between_cities, 0)
                return Response(
                    response=json.dumps(
                        {"source": source, "destination": destination, "hits": 0}), status=201,
                    mimetype='application/json')
        except ServerSelectionTimeoutError:
            return Response(TIMED_OUT_ERROR, status=410)

    except ConnectionFailure:
        return Response(NO_CONNECTION_ERROR, status=500)


@app.route('/distance', methods=['GET', 'POST'])
def get_distance():
    if request.method == 'POST':
        return post_distance()
    else:
        return distance_getter()


def is_server_connected(client):
    try:
        client.admin.command('ismaster')
        return True
    except ConnectionFailure:
        return False


@app.route('/health')
def get_health():
    client = MongoClient('localhost', 27017)
    if is_server_connected(client):
        return Response(status=200)
    else:
        return Response(NO_CONNECTION_ERROR, status=500)


@app.route('/popularsearch')
def get_popular_search():
    client = MongoClient('localhost', 27017)
    try:
        db = client.homeProjects
        max_hits_document = db.maxRequests.find()
        max_hits = next(max_hits_document, None)
        if max_hits:
            most_searched_query_cursor = db.locations.find({"hits": max_hits["hits"]}).limit(1)
            most_searched_query_object = next(most_searched_query_cursor, None)
            if most_searched_query_object:
                source, destination = most_searched_query_object["locations"].split('/')[0], \
                                      most_searched_query_object["locations"].split('/')[1]
                return Response(
                    response=json.dumps(
                        {"source": source, "destination": destination, "hits": most_searched_query_object["hits"]}),
                    status=200, mimetype='application/json')
        return Response(
            NO_SEARCHES_FOUND_ERROR, status=300)
    except ConnectionFailure:
        return Response(
            NO_CONNECTION_ERROR, status=500)


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=8080)
