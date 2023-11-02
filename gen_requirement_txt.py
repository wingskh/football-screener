# pip3 freeze > requirements2.txt
import pandas as pd

with open("requirements.txt", "r") as file:
    data1 = file.readlines()
with open("requirements2.txt", "r") as file:
    data2 = file.readlines()
    
data = data1 + data2
formatted_data = [x.strip().split("==") for x in data]
df = pd.DataFrame(formatted_data, columns=["package", "version"])
df = df[~df["package"].duplicated(keep="first")]
output = (df["package"] + "==" + df["version"]).values.tolist()

file = open("items.txt", "w")
for line in output:
    file.write(line + "\n")
file.close()
