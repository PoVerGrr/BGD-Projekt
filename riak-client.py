from riak import RiakClient
import json

# properties
PORT = 8087
BUCKET = "BGD-project"
DATA_PATH = r"data/all_data.json"
YEARS = ("2016", "2017", "2018", "2019", "2020")
PROVINCES = {
    "POLSKA": "0",
    "DOLNOSLASKIE": "200000",
    "KUJAWSKO-POMORSKIE": "400000",
    "LUBELSKIE": "600000",
    "LUBUSKIE": "800000",
    "LODZKIE": "1000000",
    "MALOPOLSKIE": "1200000",
    "MAZOWIECKIE": "1400000",
    "OPOLSKIE": "1600000",
    "PODKARPACKIE": "1800000",
    "PODLASKIE": "2000000",
    "POMORSKIE": "2200000",
    "SLASKIE": "2400000",
    "SWIETOKRZYSKIE": "2600000",
    "WARMINSKO-MAZURSKIE": "2800000",
    "WIELKOPOLSKIE": "3000000",
    "ZACHODNIOPOMORSKIE": "3200000"
}
# connection
client = RiakClient(pb_port=PORT)
bucket = client.bucket(BUCKET)


# load data into Riak
def load_data(path, b):
    with open(path, "r") as file:
        data = json.load(file)
        for key in data:
            value = data[key]
            bucket.new(key, data=value).store()


# get single element from special province
def get_single_data(b, province, name, year):
    fetched = b.get(province)
    return fetched.data[name + "_" + year]


def get_all_years_data(b, province, name):
    result = []
    for year in YEARS:
        data = get_single_data(b, province, name, year)
        result.append(data)
    return result



load_data(DATA_PATH, bucket)


# test 1: get_single_data function output should be: 5,87
print(get_single_data(bucket, PROVINCES["ZACHODNIOPOMORSKIE"], "mieso", "2016"))

# test 2: get_all_years_data output should be: ['5,87', '5,83', '5,78', '5,57', '5,95']
print(get_all_years_data(bucket, PROVINCES["ZACHODNIOPOMORSKIE"], "mieso"))




# 3. przetworzenie danych i odpowiedź
