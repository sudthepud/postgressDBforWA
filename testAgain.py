from PostgresDB2 import PostgresDatabase
from tqdm import tqdm
import json
from os.path import isfile

# Provide the correct connection URL
connection_url = "postgres://sudheesh:Mtnu8Zg49OPLshMTe05TLjPABoAzs1Sq@dpg-cn4g12i1hbls73ag4dv0-a.ohio-postgres.render.com/cbdbwa"
db = PostgresDatabase(connection_url)

print("Columns for 'test_images' table:", db.get_schema("test_images"))

images_path = "bdd100k/images/10k/train/"
labels_path = "bdd100k/labels/bdd100k_labels_images_val.json"

IMAGE_WIDTH = 1280
IMAGE_HEIGHT = 720
count = 0

with open(labels_path) as f:
    labels = json.load(f)

for i in tqdm(range(len(labels))):
    image_label = labels[i]
    if "labels" not in image_label.keys():
        continue

    image_path = images_path + image_label["name"]
    if not isfile(image_path):
        #print(f"Image file not found: {image_path}")
        count = count + 1
        continue

    image_dict = {
        "x_res": IMAGE_WIDTH,
        "y_res": IMAGE_HEIGHT,
        "filepath": images_path + image_label["name"]
    }
    
    #print(f"Checking file path: {image_dict['filepath']}")

    db._download_image(images_path + image_label["name"])

print(count)