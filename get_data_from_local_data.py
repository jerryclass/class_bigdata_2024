import pandas


# string convert int
def str_to_int(str):
    try:
        num = int(str)
    except ValueError:
        num = int(0)
    return num


# read data form csv
selected_columns = ["PseudoID", "city_code", "organization_id", "grade", "class"]
data = pandas.read_csv("data/edu_bigdata_2024.csv", usecols=selected_columns, nrows=20)

# show columns of data
print(data.columns)

# create user_data_table
for index, row in data.iterrows():
    pseudo_id = str_to_int(row["PseudoID"])
    city_code = row["city_code"]
    organization_id = str_to_int(row["organization_id"])
    grade = str_to_int(row["grade"])
    user_class = str_to_int(row["class"])

    csv_data = (
        f"PseudoID:{pseudo_id} city_code:{city_code} organization_id:{organization_id}"
    )
    print(csv_data)
