import riak
import json
import pandas as pd

# properties
from riak import RiakClient


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
KEYS_LIST= ("produkty_zbozowe", "mieso", "owoce", "warzywa", "cukier", "ryby_owoce_morza",
            "mleko", "jogurty", "sery", "tluszcze", "jaja")

# connection
client = RiakClient(pb_port=PORT)
bucket = client.bucket(BUCKET)


# load data into Riak
def load_data(path, b):
    with open(path, "r") as file:
        data = json.load(file)
        for key in data:
            value = data[key]
            b.new(key, data=value).store()


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


def food_sum(b, provinces, target):
    dictionary = {}
    for province in provinces:
        monthly_amount_of_food = [0] * len(target)

        for food_type in target:
            all_years_food_type_amount = get_all_years_data(b, provinces[province], food_type)

            for index, item in enumerate(all_years_food_type_amount):
                all_years_food_type_amount[index] = float(item.replace(",", "."))

            zipped_lists = zip(all_years_food_type_amount, monthly_amount_of_food)
            monthly_amount_of_food = [round(x + y, 2) for (x, y) in zipped_lists]
        dictionary[province] = monthly_amount_of_food
    return dictionary

def percent_of_total(b, provinces, total_name, number_you_want_as_percent_name):
    percent_dickt = {}
    for province in provinces:
        total = get_all_years_data(b, provinces[province], total_name)
        for index, item in enumerate(total):
            total[index] = float(item.replace(",", "."))
        number_you_want_as_percent = get_all_years_data(b, provinces[province], number_you_want_as_percent_name)
        for index, item in enumerate(number_you_want_as_percent):
            number_you_want_as_percent[index] = float(item.replace(",", "."))
        zipped_lists = zip(number_you_want_as_percent, total)
        percent_dickt[province] = dict(zip(YEARS, [round((x / y)*100, 2) for (x, y) in zipped_lists]))
    return percent_dickt




def test():
    # test 1
    print("\nTest1: output should be 5,87")
    print("Test1: result:", get_single_data(bucket, PROVINCES["ZACHODNIOPOMORSKIE"], "mieso", "2016"))

    # test 2
    print("\nTest2: output should be ['5,87', '5,83', '5,78', '5,57', '5,95']")
    print("Test2: result:", get_all_years_data(bucket, PROVINCES["ZACHODNIOPOMORSKIE"], "mieso"))


load_data(DATA_PATH, bucket)
#Średnia miesięczna ilość spożywanego jedzenia dla województw w latach 2016-2020
monthly_all_food_consumption_df = pd.DataFrame(food_sum(bucket, PROVINCES, KEYS_LIST))

#Procent wydawanych pieniędzy na żywność w stosunku do dochodu dla wojewodztw w latach 2016-2020
percent_of_money_spend_for_food_by_income_df = pd.DataFrame(percent_of_total(bucket, PROVINCES, "dochod", "wydatki_na_zywnosc"))

#Procent wydawanych pieniedzy na żywność w stosunku do wydatków dla województw w latach 2016-2020
percent_of_money_spend_for_food_by_expenses_df =pd.DataFrame(percent_of_total(bucket, PROVINCES, "wydatki_ogolem", "wydatki_na_zywnosc"))

#test()

print(f"Średnia miesięczna ilość spożywanego jedzenia (w kilogramach):\n",monthly_all_food_consumption_df.head())
print(f"Stosunek wydatków ponoszonych na żywność do wydatków ogólnie (w procentach):\n",percent_of_money_spend_for_food_by_expenses_df.head())
#print(food_sum(bucket, PROVINCES, KEYS_LIST))