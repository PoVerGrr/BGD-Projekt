from riak import RiakClient
import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# properties
PORT = 8087
BUCKET = "BGD-project"
DATA_PATH = r"data/all_data.json"
OUTPUT_PATH = "output/"
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
KEYS_LIST = ("produkty_zbozowe", "mieso", "owoce", "warzywa", "cukier", "ryby_owoce_morza",
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
        dictionary[province] = dict(zip(YEARS, monthly_amount_of_food))
    return dictionary


def data_to_heatmap(b, provinces, name):
    output = {}
    for province in provinces:
        all_data = get_all_years_data(b, provinces[province], name)
        for index, item in enumerate(all_data):
            all_data[index] = round(float(item.replace(",", ".")), 2)
        output[province] = dict(zip(YEARS, all_data))
    # print(output)
    return output


def generate_heatmaps(data, title, file):
    plt.subplots(1, 1, figsize=(18, 8))
    sns.heatmap(data, annot=True, linewidths=.5, cmap="YlGnBu", fmt='g')
    plt.title(title)
    plt.savefig(file + ".png")


load_data(DATA_PATH, bucket)

# Średnia miesięczna ilość spożywanego jedzenia dla województw w latach 2016-2020
monthly_all_food_consumption_df = pd.DataFrame(food_sum(bucket, PROVINCES, KEYS_LIST)).T

# Procent wydawanych pieniędzy na żywność w stosunku do dochodu dla wojewodztw w latach 2016-2020
money_spend_for_food_df = pd.DataFrame(data_to_heatmap(bucket, PROVINCES, "wydatki_na_zywnosc")).T

# Procent wydawanych pieniedzy na żywność w stosunku do wydatków dla województw w latach 2016-2020
money_spend_in_general_df = pd.DataFrame(data_to_heatmap(bucket, PROVINCES, "wydatki_ogolem")).T

sns.set()

generate_heatmaps(money_spend_in_general_df, "Średnie miesięczne wydatki (w złotych)/os dla wojewodztw w latach 2016-2020", OUTPUT_PATH+"Figure_1")
generate_heatmaps(money_spend_for_food_df, "Średnie miesięczne wydatki na żywność (w złotych)/os dla wojewodztw w latach 2016-2020", OUTPUT_PATH+"Figure_2")
generate_heatmaps(monthly_all_food_consumption_df, "Średnia miesięczna ilość spożywanego jedzenia (w kilogramach) na osobę dla województw w latach 2016-2020", OUTPUT_PATH+"Figure_3")

plt.show()
