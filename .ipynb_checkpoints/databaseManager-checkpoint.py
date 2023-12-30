import sqlite3
from typing import List
import pandas as pd

class DatabaseManager:
    def __init__(self, db_name: str, table_name: str = 'my_table'):
        self.db_name = db_name
        self.table_name = table_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self, element_type: str = 'REAL'):
        """
        Creates a table in the database with the specified element type.

        Args:
            element_type (str, optional): The data type for the 'list_element' column. Defaults to 'REAL' because in this case the main list we want to store GamePoints list which has elements of type 'REAL'.
        """
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                list_element {element_type}
            )
        ''')
        self.conn.commit()

    def create_table_for_object(self, obj):
        """
        Creates a table in the SQLite database based on the attributes of the provided object.
    
        Parameters:
        - obj (object): The object whose attributes will be used to define the table structure.
    
        Example Usage:
        ```
        # Assuming obj is an instance of a class with attributes representing table columns
        db_manager = DatabaseManager('example.db')
        db_manager.create_table_for_object(obj)
        ```
    
        Note: Ensure that the object attributes accurately represent the desired table columns and their data types.
        """
        table_properties = ', '.join([f'{attr.upper()} {str(type(obj[attr]).__name__)}' for attr in obj.index if not callable(getattr(obj, attr)) and not attr.startswith("__")])
        table_properties = table_properties.upper()

        column_types = {
            'STR': 'TEXT',
            'INT64': 'INTEGER',
            'FLOAT64': 'REAL',
        }
        
        columns = table_properties.split(', ')
        updated_columns = []
        for column in columns:
            column_name, column_type = column.split(' ')
            updated_type = column_types.get(column_type, column_type)
            updated_columns.append(f"{column_name} {updated_type}")
        updated_columns_string = ', '.join(updated_columns)

        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {updated_columns_string}
            )
        ''')
        conn.commit()

    def delete_table(self):
        """
        Deletes the specified table from the database.
        """
        self.cursor.execute(f'DROP TABLE IF EXISTS {self.table_name}')
        self.conn.commit()
        print(f'Table {self.table_name} deleted.')

    def save_list_to_database(self, element_list):
        """
        Saves a list of elements to the database.

        Args:
            element_list (list): The list of elements to be saved to the database.
        """
        element_type = type(element_list[0]).__name__
        self.create_table(element_type)
        
        for element in element_list:
            self.cursor.execute(f'INSERT INTO {self.table_name} (list_element) VALUES (?)', (element,))
        self.conn.commit()

    def save_object_list(self, list_of_objects: pd.DataFrame):
        if list_of_objects.empty:
            print("List of objects is empty. No action taken.")
            return
        
        create_table_for_object(list_of_objects.iloc[0])
        list_of_dataframes = [list_of_objects]
        concatenated_df = pd.concat(list_of_dataframes)
        list_of_tuples = [tuple(row) for _, row in concatenated_df.iterrows()]
        
        self.cursor.executemany(f'INSERT INTO {self.table_name} VALUES ({",".join(["?" for _ in range(len(list_of_tuples[0]))])})', list_of_tuples)
        self.conn.commit()
        print(f'All the objects were stored in: {self.table_name}')


    def set_table_name(self, new_table_name: str):
        """
        Sets a new table name for the DatabaseManager instance. This is benefitial when we want to create new tables and store some lists in it.

        Args:
            new_table_name (str): The new table name.
        """
        self.table_name = new_table_name
        print(f'Table name set to: {self.table_name}')

    def get_element_list(self):
        """
        Retrieves the entire list of elements from the specified table.

        Returns:
            list: The list of elements stored in the table.
        """
        self.cursor.execute(f'SELECT list_element FROM {self.table_name}')
        result = self.cursor.fetchall()
        return [item[0] for item in result]

    def get_all(self):
        """
        Retrieves the all the elements from the specified table.
        """
        self.cursor.execute(f'SELECT * FROM {self.table_name}')
        result = self.cursor.fetchall()
        return result

    def close_connection(self):
        """
        Closes the connection to the database. Necessary for better resource management.
        """
        self.conn.close()