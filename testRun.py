from PostgresDB2 import PostgresDatabase
from tqdm import tqdm
import json
from os.path import isfile

connection_url = "postgres://sudheesh:Mtnu8Zg49OPLshMTe05TLjPABoAzs1Sq@dpg-cn4g12i1hbls73ag4dv0-a.ohio-postgres.render.com/cbdbwa"
db = PostgresDatabase(connection_url)

images_path = "bdd100k/images/10k/train/"
labels_path = "bdd100k/labels/bdd100k_labels_images_val.json"

IMAGE_WIDTH = 1280
IMAGE_HEIGHT = 720

with open(labels_path) as f:
    labels = json.load(f)

for i in tqdm(range(len(labels))):
    image_label = labels[i]
    if "labels" not in image_label.keys():
        continue

    image_path = images_path + image_label["name"]
    if not isfile(image_path):
        #print(f"Image file not found: {image_path}")
        continue

    image_dict = {
        "x_res": IMAGE_WIDTH,
        "y_res": IMAGE_HEIGHT,
        "filepath": images_path + image_label["name"]
    }
    image_id = db.insert_row_with_image("test_images", image_dict)

    annotations = image_label["labels"]
    class_id_map = {
        'pedestrian': 0, 
        'car': 1,
        'truck': 1,
        'bus': 1,
        'motor': 1,
        'traffic light': 2,
        'traffic sign': 3
    }
    for annotation in annotations:
        category = annotation["category"]
        if category not in class_id_map:
            continue
        class_id = class_id_map[category]

        x_1 = annotation['box2d']['x1']
        y_1 = annotation['box2d']['y1']
        x_2 = annotation['box2d']['x2']
        y_2 = annotation['box2d']['y2']
        
        x_center_norm = (x_1 + x_2)/(2 * IMAGE_WIDTH)
        y_center_norm = (y_1 + y_2)/(2 * IMAGE_HEIGHT)
        
        width_norm = abs(x_2 - x_1)/IMAGE_WIDTH
        height_norm = abs(y_2 - y_1)/IMAGE_HEIGHT

        label_dict = {
            "class_id": class_id,
            "class": category,
            "center_norm_x": x_center_norm,
            "center_norm_y": y_center_norm,
            "width_norm": width_norm,
            "height_norm": height_norm,
            "image_id": image_id,
            "dataset": "bdd100k"
        }
        db.insert_row("test_bb_labels", label_dict)

db.commit()