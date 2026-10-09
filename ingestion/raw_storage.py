import json 
from pathlib import Path

file_path = "./data/raw/BTCUSDT_1m_sample.json"

def save_data(data):
    # converting our data to json readable before saving it
    json_data = json.dumps(data,indent=4)

    # ensuring that the path directory of the file is safe to write (basically checking if it exists if not then it will be created)
    target_dir = Path(file_path).parent
    target_dir.mkdir(parents=True,exist_ok=True)
    
    #writing file
    with open(file_path,'w', encoding="utf-8") as f:
        f.write(json_data)

def read_data():
    with open(file_path, 'r', encoding="utf-8") as f:
        data = json.load(f)

    return data