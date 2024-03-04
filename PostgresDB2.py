"""
This file contains the PostgresDatabase class which connects the 
user to the database on CAE storage
"""
import psycopg2
import os
import time
import hashlib
from tqdm import tqdm 
from collections import defaultdict
from wa_infra_tools.ssh_utils.SSHClient import SSHClient

class PostgresDatabase:
    def __init__(self, connection_url):
        """
        Constructs a PostgresDatabase object by connecting to the database using a connection URL.

        @param connection_url: str - PostgreSQL connection URL
        """
        self.connection = psycopg2.connect(connection_url)
        self.cursor = self.connection.cursor()
        self.uncommitted_image_paths = []

      

    def sql(self, sql_string):
        """
        Executes any sql command on the database

        @param sql_string : str     - string containing sql command
        @return List[Tuple[any...]] - list of the result of the query (if there is a response)
        """
        self.cursor.execute(sql_string)
        return self.cursor.fetchall() if self.cursor.pgresult_ptr is not None else []

    def commit(self):
        """
        Commits transaction on the sql database AND sends all images that were inserted into
        the database to the remote machine
        """
        self.connection.commit()
        if self.uncommitted_image_paths:
            for i in tqdm(range(len(self.uncommitted_image_paths))):
                local_filepath, remote_filepath, remote_filename  = self.uncommitted_image_paths[i]
                print(remote_filename)
                print(local_filepath)
                self._upload_image(local_filepath, remote_filepath, remote_filename)
        self.uncommitted_image_paths = []

    def get_schema(self, table_name):
        """
        Gets schema of any table in the database

        @param table_name : str - table name 
        """
        assert table_name in self.tables, f"Table {table_name} is not in the database"
        return self.sql(
            f"""
            SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
            """)

    def preview_table(self, table_name, limit=10):
        """
        Previews the data of any table in the database

        @param table_name : str - table name 
        @param limit : int      - number of rows to be shown (default: 10)
        """
        assert table_name in self.tables, f"Table {table_name} is not in the database"
        return self.sql(
            f"""
            SELECT * 
                FROM {table_name} 
                LIMIT {limit}
            """)

    def insert_row(self, table_name, value_dict):
        """
        Insert row into the database. Does not commit the insertion into the database until commit is called
        Note: This function will NOT prepare to send any images you want to insert
        upon commiting the transaction

        @param table_name : str             - table to insert into
        @param value_dict : Dict[str, any]  - dictionary of column name, value pairs to insert
        """
        self._check_schema(table_name, value_dict)
        col_names = []
        col_values = []
        for name, val in value_dict.items():
            col_names.append(name)
            if isinstance(val, str):  # Enclose string values in quotes
                val = "'" + val.replace("'", "''") + "'"  # Escape any single quotes in the string
            col_values.append(str(val))

        self.sql(
            """
            INSERT INTO {} ({})
            VALUES ({})
            """.format(
                table_name,
                ", ".join(col_names),
                ", ".join(col_values)
            )
        )


    def insert_row_with_image(self, table_name, value_dict, image_id_col="image_id"):
        """
        Insert row into the database with image. Does not commit the insertion into the database or send the 
        image until commit is called

        @param table_name : str             - table to insert into
        @param value_dict : Dict[str, any]  - dictionary of column name, value pairs to insert
        @param image_id_col: str            - name of the id (primary key) column in the table (default: image_id)
        """
        assert "filepath" in value_dict, "The values you insert must contain a filepath attribute with the local filepath of the image"

        # get local filepath from values
        local_filepath = value_dict["filepath"]
        assert os.path.isfile(local_filepath), f"The path {local_filepath} to the image is invalid"

        # read the image file in binary format
        with open(local_filepath, 'rb') as file:
            binary_data = file.read()

        # insert row with binary data of the image
        value_dict["image_data"] = psycopg2.Binary(binary_data)
        self.insert_row(table_name, value_dict)

        # get image_id of the row
        image_id = self.sql(f"SELECT max({image_id_col}) FROM {table_name}")[0][0] or 0

        # commit the transaction
        self.connection.commit()

        return image_id


    def download_data(self, table_name, label_column_names, output_path, format_type, image_id="image_id", filter_sql=[]):
        """
        Downloads data from existing table on the database. Executes and commits any filters from filter_sql 
        before downloading. Download format can be specified by format_type. Images and labels will be downloaded 
        to output_path/images and output_path/labels respectively.

        @param table_name : str                 - table name
        @param label_column_names : List[str]   - list containing the columns to be in the label file in order
        @param output_path : str                - path to save images/labels in
        @param format_type : str                - download format
        @param image_id : str                   - column that the two tables will be joined on
        @param filter_sql : List[str]           - list of sql queries to execute before joining the tables
        """
        for sql_op in filter_sql:
            self.sql(sql_op)
        self.commit()

        records = self.sql(f"SELECT * FROM {table_name}")

        column_names = {self.cursor.description[i][0] : i for i in range(len(self.cursor.description))}
        assert all([name in column_names for name in label_column_names]), "Invalid column name in label column names"

        if format_type == "yolo":
            self._download_data_yolo(records, label_column_names, column_names, output_path, image_id)

    def join_and_download_data(self, images_table_name, labels_table_name, label_column_names, output_path, format_type, image_id="image_id", filter_sql=[]):
        """
        Downloads data from the join of two tables (image table and labels table). Executes and commits
        any filters from filter_sql onto the table before the join. Download format can be specified
        by format_type. Images and labels will be downloaded to output_path/images and output_path/labels
        respectively.

        @param images_table_name : str          - table that contains the images
        @param labels_table_name : str          - table that contains the labels
        @param label_column_names : List[str]   - list containing the columns to be in the label file in order
        @param output_path : str                - path to save images/labels in
        @param format_type : str                - download format
        @param image_id : str                   - column that the two tables will be joined on
        @param filter_sql : List[str]           - list of sql queries to execute before joining the tables
        """
        temp_table_name = images_table_name + "_" + labels_table_name + "_tmp"
        filter_sql.append(
            f"""
            DROP TABLE IF EXISTS {temp_table_name};
            SELECT *
                INTO {temp_table_name}
                FROM {images_table_name}
                JOIN {labels_table_name}
                USING ({image_id})
            """
        )
        self.download_data(temp_table_name, label_column_names, output_path, format_type, image_id, filter_sql)
        self.sql(f"DROP TABLE {temp_table_name}")
        self.commit()

    def _download_data_yolo(self, records, label_column_names, column_names, output_path, image_id):
        """
        Data download specifically for yolo format

        @param records : List[Tuple[any...]]    - contents of a sql table containing a filepath column and all the label_column_names
        @param label_column_names : List[str]   - list containing the columns to be in the label file in order
        @param column_names: Dict[str, int]     - maps column names to their index in a row of the table
        @param output_path : str                - path to save images/labels in
        @param image_id : str                   - column that the two tables will be joined on
        """
        image_remote_filepaths = {}
        label_dict = defaultdict(str)
        for row in records:
            image_id_val = row[column_names[image_id]]
            if image_id_val not in image_remote_filepaths:
                image_remote_filepaths[image_id_val] = row[column_names["filepath"]]

            label = " ".join([str(row[column_names[name]]) for name in label_column_names])
            label_dict[image_id_val] += label + "\n"

        if output_path[-1] != "/":
            output_path += "/"
        if not os.path.exists(output_path + "images"):
            os.mkdir(output_path + "images")
        if not os.path.exists(output_path + "labels"):
            os.mkdir(output_path + "labels")

        print("Downloading images")
        image_remote_filepaths = list(image_remote_filepaths.items())
        for i in tqdm(range(len(image_remote_filepaths))):
            image_id_val, path = image_remote_filepaths[i]
            image_extension = "." + path.split(".")[-1]
            self._download_image(output_path + "images/" + str(image_id_val) + image_extension, path)

        print("Writing labels")
        labels = list(label_dict.items())
        for i in tqdm(range(len(labels))):
            image_id_val, label_str = labels[i]
            with open(output_path + "labels/" + str(image_id_val) + ".txt", "w") as f:
                f.write(label_str)

    def _check_schema(self, table_name, value_dict):
        """
        Checks that the keys of the value dict are in the table schema

        @table_name : str - table name
        """
        assert table_name in self.tables, "Table name is not in the database"
        schema = self.get_schema(table_name)
        schema_dict = {col[0]: col[1] for col in schema}
        for col_name in value_dict.keys(): # iterate through columns in the val dict
            assert col_name in schema_dict, "Invalid column name in the value dictionary"
            if schema_dict[col_name] == "character varying": # if column is string type
                # make sure that the value is contained in ''
                new_val = value_dict[col_name]
                if new_val[0] != "'":
                    new_val = "'" + new_val
                if new_val[-1] != "'":
                    new_val += "'"
                value_dict[col_name] = new_val

    def _upload_image(self, local_filepath, remote_filpath, remote_filename):
        """
        Uploads image to the remote database

        @local_filepath : str   - path to image on local machine
        @remote_filename : str  - what to call file in the database
        """
        with open(local_filepath, 'rb') as file:
            binary_data = file.read()
        insert_query = "INSERT INTO images (filepath, image_data) VALUES (%s, %s)"
        self.cursor.execute(insert_query, (local_filepath, psycopg2.Binary(binary_data)))
        self.connection.commit()


    def _download_image(self, remote_filename):
        """
        Fetches all image data from the remote database and writes it to a file

        @remote_filename : str  - name of file in the database
        """
        select_query = "SELECT * FROM test_images WHERE filepath = %s"
        self.cursor.execute(select_query, (remote_filename,))
        row = self.cursor.fetchone()
        if row is None:
            print(f"No image found with filename: {remote_filename}")
        else:
            # Write the entire row of image data to a file
            with open('testOutput.txt', 'a') as f:
                f.write(f"Image ID: {row[0]}\n")
                f.write(f"X Resolution: {row[1]}\n")
                f.write(f"Y Resolution: {row[2]}\n")
                f.write(f"Image Data Size: {row[3]} bytes\n")  # assuming row[3] is the size of the image data
                f.write(f"Filepath: {row[4]}\n")
                f.write("-"*30 + "\n")  # add a separator line between entries

    @property
    def tables(self):
        """
        Lists all the tables in the database
        """
        records = self.sql("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        return [record[0] for record in records]