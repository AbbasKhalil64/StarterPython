import json
import csv
from datetime import datetime
import os # importing the os module

import sqlite3 as sql
import numpy as np
import re

from typing import List


def main():
    
    global write_datetime # access the global variable
    global current_datetime
    
    # using a global variable to store the datetime object
    current_datetime = datetime.now()

    # use a global variable to store a flag for writing the datetime
    write_datetime = False

    custom_print(current_datetime.strftime("%A, %B %d, %Y %I:%M:%S %p"))


    # Creating an empty list to store each row as a dictionary
    Oscar_Awards = []

    Path_File_JSON = r"C:\Users\AbbasKhalil\Desktop\Python Projects\datasets-main\oscarAwards.json"

    with open(Path_File_JSON, "r") as f:
        json_data = json.load(f)

    custom_print("Film_Year", "Ceremony_Year", "Ceremony", "Category", "Name", "Film", "Winner")
    
    
    count = 0

    # Use the correct key name "values" to access the list of rows from the json_data dictionary
    for row in json_data["values"]:
        # Create a dictionary for each row with keys and values matching the column names and values
        row_dict = {
            "Film_Year": int(row[0]),
            "Ceremony_Year": int(row[1]),
            "Ceremony": int(row[2]),
            "Category": str(row[3]),
            "Name": str(row[4]),
            "Film": str(row[5]),
            "Winner": convert_to_int(row[6])
        }
        #Winner = convert_to_int(row[6])

        # Append the row dictionary to the Oscar_Awards list
        Oscar_Awards.append(row_dict)


    # Dumping the whole python dictionary
    #print(Oscar_Awards)
    custom_print()
    

    custom_print("The first 100 numbers in the list: \n")
    # Here I will print the first 100 rows in the dictionary to test its content
    #for row_dict in Oscar_Awards[:100]:
    for row_dict in Oscar_Awards[:100]:
        # Print a message with the count variable and the row dictionary values
        custom_print(f"Printing row number {count}: {', '.join([repr(value) for value in row_dict.values()])}")
        # Increment count by 1
        count += 1
    
    custom_print()

    count = 0
    custom_print("The last 100 numbers in the list: \n")
    for row_dict in Oscar_Awards[-100:]:
        # Print a message with the count variable and the values of the row dictionary
        # The join method is invoked to format the list of the object view of values
        # The str function is used to convert the values to strings
        custom_print(f"Printing row number {count}: {', '.join([repr(value) for value in row_dict.values()])}")
        # Increment count by 1
        count += 1

    #return Oscar_Awards


    # Create a Sqllite3 DB file and connect it.
    conn = sql.connect("C:\\Users\\AbbasKhalil\\Python VS Code\\sqlitefirst.db")

    conn.row_factory = sql.Row # set the row factory attribute
    #creating the cursor
    cur = conn.cursor()

    # Check if the table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='OA_Ceremony_Events'")
    result = cur.fetchone()

    # If the result is None, then the table does not exist
    if result is None:
        #creating a unique events in Oscar Awards Ceremony
        cur.execute("""CREATE TABLE IF NOT EXISTS OA_Ceremony_Events (
                Film_Year INTEGER,
                Ceremony_Year INTEGER,
                Ceremony INTEGER,
                Category TEXT,
                Name TEXT,
                Film TEXT, 
                Winner INTEGER,
                UNIQUE (Film_Year, Ceremony_Year, Ceremony, Category, Name, Film, Winner) ON CONFLICT IGNORE
            )""")
    
        # This is the code for inserting data into the table using a tuple syntax
        rows = [tuple(d.values()) for d in Oscar_Awards]
        cur.executemany("INSERT OR IGNORE INTO OA_Ceremony_Events (Film_Year, Ceremony_Year, Ceremony, Category, Name, Film, Winner) VALUES (?, ?, ?, ?, ?, ?, ?)", rows)

        #cur.executemany("INSERT INTO OA_Ceremony_Events (Film_Year, Ceremony_Year, Ceremony, Category, Name, Film, Winner) VALUES (?, ?, ?, ?, ?, ?, ?)", rows)

        conn.commit()


    # Execute a PRAGMA query to get the information about the table OA_Ceremony_Events
    cur.execute("PRAGMA table_info(OA_Ceremony_Events)")

    # Fetch all the results from the query and store them in a list & print
    results = cur.fetchall()

    custom_print("\n \033[34mThe SQLite file Table's Headings and their Attributes:\033[0m")
    for result in results:
        custom_print(f"\n {tuple(result)}")
    custom_print()


    # Invoking the query function 
    Query_SQL_Table("OA_Ceremony_Events", cur)

    # Closing Connection to sqlitefirst.db file
    conn.close()

    custom_print("Here:")
    custom_print(Oscar_Awards[:2])


    # Invoke the Numpy function
    Build_and_Query_Numpy_Array(Oscar_Awards)



def convert_to_int(value):
    if value.lower() == 'true':
        return 1
    elif value.lower() == 'false':
        return 0
    else:
        return None


def Query_SQL_Table(QTable, cur):
    
    custom_print("The table name is:", QTable)
    # rest of the code
    custom_print()

    # Create a list of valid choices
    valid_choices: List = [1, 2, 3, 4, 5] + ["exit"]
    valid_choices = [str(x) for x in valid_choices]
    #valid_choices = [1, 2, 3, 4, 5, "exit"]
    custom_print("\033[34mFor brevity and test reasons, please choose one of these options:\033[0m \n")
    
    custom_print("1. The name of Film winning Best Picture")
    custom_print("2. The Actor who won the Best Actor the most")
    custom_print("3. The Actress who won the Best Actress the most")
    custom_print("4. What are the most awarded film, and in what category in a Ceremony")
    custom_print("5. The names of films nominated for best picture per ceremony year \n")

    user_choice = input("Enter the number of your choice here, or 'exit' to quit: ")
    #print(valid_choices)

    if user_choice == "exit":
        return
    else:
        while not user_choice in valid_choices[:5]:
            custom_print("That is not a valid choice. Try again!")
            user_choice = input("Enter the number of your choice here: ")
            if user_choice == "exit": return

    user_choice = int(user_choice)

    #if user_choice != "exit":
    try:
        # Match the user's choice to a case
        match user_choice:
            case 1:
                Year_of_Ceremony = int(input("For which Year: "))
                cur.execute("SELECT Film FROM " + QTable + " WHERE Ceremony_Year = ? AND (Category = 'OUTSTANDING PICTURE' OR Category = 'OUTSTANDING MOTION PICTURE' OR Category = 'BEST MOTION PICTURE' OR Category = 'BEST PICTURE') AND Winner = 1", (Year_of_Ceremony,))

                result = cur.fetchone()
                custom_print(result[0])

            case 2:
                cur.execute("SELECT Name, COUNT(Name) AS WinningActor FROM " + QTable + " WHERE (Category = 'ACTOR' OR Category = 'ACTOR IN A LEADING ROLE') AND Winner = 1 GROUP BY Name ORDER BY WinningActor DESC LIMIT 1")

                result = cur.fetchone()
                custom_print(f"\033[33mActor {result['Name']} won the highest awards with {result['WinningActor']} in total\033[0m")

                # print(result[0])

            case 3:
                cur.execute("SELECT Name, COUNT(Name) AS WinningActress FROM " + QTable + " WHERE (Category = 'ACTRESS' OR Category = 'ACTRESS IN A LEADING ROLE') AND Winner = 1 GROUP BY Name ORDER BY WinningActress DESC LIMIT 1")

                result = cur.fetchone()
                custom_print(f"\033[33mActress {result['Name']} won the highest awards with {result['WinningActress']} in total\033[0m")

            case 4:
                cur.execute("SELECT Film, Ceremony_Year, COUNT(Category) AS WinningCategory, GROUP_CONCAT(Category) AS WinningCategories FROM " + QTable + " WHERE Winner = 1 AND Film IS NOT NULL AND Film <> '' GROUP BY Film, Ceremony_Year ORDER BY WinningCategory DESC, Ceremony_Year DESC LIMIT 1")

                result = cur.fetchone()
                custom_print(f"\033[33mIn {result['Ceremony_Year']} the film {result['Film']} won the highest awards with {result['WinningCategory']} in total; in the following categories: {result['WinningCategories']}\033[0m")

            case 5:
                Year_of_Ceremony = int(input("For which Year: "))
                cur.execute("SELECT Ceremony_Year AS Ceremony_Year, GROUP_CONCAT(Film) AS NomFilms FROM " + QTable + " WHERE Ceremony_Year = ? AND (Category = 'OUTSTANDING PICTURE' OR Category = 'OUTSTANDING MOTION PICTURE' OR Category = 'BEST MOTION PICTURE' OR Category = 'BEST PICTURE') AND Film IS NOT NULL AND Film <> '' GROUP BY Ceremony_Year ORDER BY Ceremony_Year DESC LIMIT 1", (Year_of_Ceremony,))

                result = cur.fetchone()
                custom_print(f"\033[33mIn {result['Ceremony_Year']} these are the films {result['NomFilms']} nominated for the Academy Awards in the BEST PICTURE category\033[0m")


    except Exception as e:
        custom_print("An error occurred:", e)

    
def Build_and_Query_Numpy_Array(Oscar_Events):

    # Check the state of Oscar_Events, by printing the Keys and the values of the dictionary at index 0
    custom_print()
    custom_print(Oscar_Events[0].keys())
    custom_print(Oscar_Events[0].values())
    custom_print()

    # Populating the OA_Events list with the values (through .get) of the passed Oscar_Events
    OA_Events = []
    for row in Oscar_Events:
        OA_Events.append(
            dict(
                Film_Year=row.get("Film_Year"),
                Ceremony_Year=row.get("Ceremony_Year"),
                Ceremony=row.get("Ceremony"),
                Category=row.get("Category"),
                Name=row.get("Name"),
                Film=row.get("Film"),
                Winner=row.get("Winner")
            )
        )
    
    # A printout test of the generated dictionaries of row at index 0
    custom_print(OA_Events[0].keys())
    custom_print(OA_Events[0].values())
    custom_print()

    # Defining a structured object for a numpy array
    dt = np.dtype([
        ("Film_Year", np.int32),
        ("Ceremony_Year", np.int32),
        ("Ceremony", np.int32),
        ("Category", np.str_, 50),
        ("Name", '<U400'),
        ("Film", np.str_, 50),
        ("Winner", np.int32)
    ])
    

    # creating an initial values for the first row of the array, to eliminate access error
    data = [(0, 0, 0, "", None, "", 0)]
    
    # Create an empty (the initial values in 'data' list) structured numpy array with the defined dtype
    Numpy_OA = np.rec.array(data, dtype=dt)

    # Test the values of the created numpy record array
    custom_print(f"The shape of the array is: {Numpy_OA.shape}")
    custom_print(Numpy_OA[0])
    custom_print("1") # A dummy print to assist debug
    custom_print(Numpy_OA.ndim)


    # Iterate over the OA_Events list and append each dictionary as a row to the structured array
    # The general rule used: numpy.append(arr, values, axis=None)
    for event in OA_Events:
        Numpy_OA = np.append(Numpy_OA, np.array(tuple(event.values()), dtype=dt))

    # Remove the dummy row at the beginning of the array
    Numpy_OA = np.delete(Numpy_OA, 0)
    custom_print(f"The shape of the array is: {Numpy_OA.shape}")
    custom_print(Numpy_OA[0])


    #Numpy_OA = np.array(Numpy_OA.tolist())
    custom_print(f"The shape of the array is: {Numpy_OA.shape}")
    custom_print("34")


    # To print the first 3 rows, and last 3 rows of the numpy array
    custom_print(Numpy_OA[:])
    custom_print("2")
    custom_print(Numpy_OA.ndim)

    custom_print(Numpy_OA.dtype.names)

    # Find the length of the longest string in the Name field
    max_length = np.max(np.char.str_len(Numpy_OA["Name"]))

    # Based on this resultant value printout, the datatype "Name" shall be adjusted to a value able to handle the largest string length.
    
    custom_print(f"The maximum length of a string in the Name field is {max_length}.")

    # Ask the user to input a name
    Search_input = input("Please enter the name you like to search for, here: ")

    # Convert the search input to lowercase
    Search_input_lower = Search_input.lower()

    # Convert the names in the array to lowercase
    Numpy_OA_lower = np.char.lower(Numpy_OA["Name"])

    # Create a boolean mask that indicates which rows contain the name inputted by the user
    mask = np.char.find(Numpy_OA_lower, Search_input_lower) != -1

    # Use the boolean mask to select the rows that contain the name inputted by the user
    Searched_Values = Numpy_OA[mask]

    # Get the indices of the rows that contain the name inputted by the user
    indices = np.where(mask)[0]

    # Print to check the length of Searched_Values
    
    object_view_length = len(Searched_Values)
    custom_print(object_view_length)
    # Print to check the length of Searched_Values
    #print(len(Searched_Values))

    # Print the rows that contain the name inputted name, along with their unique index, after parallel iteration using zip wrapping
    if object_view_length != 0:
        for i, row in zip(indices, Searched_Values):
            custom_print(f"{i} {row}")
    else: 
        custom_print(f"There is {object_view_length} records with that name")


    custom_print("last")


def custom_print(*args, **kwargs):
    global write_datetime # access the global variable
    global current_datetime
    
    dt = current_datetime # store the datetime object
    # this function takes the same arguments as the built-in print function
    print(*args, **kwargs) # print to the console as usual

    Path_File_CSV = "C:\\Users\\AbbasKhalil\\Python VS Code\\assign4_print_log.csv"

    # check if the file exists
    file_exists = os.path.exists(Path_File_CSV)

    if file_exists: # if the file exists
        # get its size
        file_size = os.path.getsize(Path_File_CSV)
        if file_size > 0: # if the file is not empty
            # read the existing file as a list
            with open(Path_File_CSV, 'r', newline='') as f_csv:
                reader = csv.reader(f_csv)
                old_data = list(reader)
        else: # if the file is empty
            # use an empty list as the old data
            old_data = []
    else: # if the file does not exist
        # use an empty list as the old data
        old_data = []

    # create a new list with only the print arguments as a row
    new_data = [list(args)]

    if not write_datetime: # if the flag is False
        # insert the datetime as a row at the beginning of the new list
        new_data.insert(0, [dt])
        # set the flag to True
        write_datetime = True

    # concatenate the new list with the old one at the top
    final_data = new_data + old_data

    # write the final list to the file
    with open(Path_File_CSV, 'w', newline='') as f_csv:
        writer = csv.writer(f_csv)
        writer.writerows(final_data)


main()

"""
def custom_print(*args, **kwargs):
    dt = Emboss_Date_Time() # store the datetime object
    # this function takes the same arguments as the built-in print function
    print(*args, **kwargs) # print to the console as usual

    Path_File_CSV = "C:\\Users\\AbbasKhalil\\Python VS Code\\assign4_print_log.csv"

    # create a new list with the datetime and print arguments as rows
    new_data = [[dt], list(args)]

    # check if the file exists
    file_exists = os.path.exists(Path_File_CSV)

    if file_exists: # if the file exists
        # get its size
        file_size = os.path.getsize(Path_File_CSV)
        if file_size > 0: # if the file is not empty
            # read the existing file as a list
            with open(Path_File_CSV, 'r', newline='') as f_csv:
                reader = csv.reader(f_csv)
                old_data = list(reader)
            # concatenate the new list with the old one at the top
            final_data = new_data + old_data
        else: # if the file is empty
            # use the new list as the final one
            final_data = new_data
    else: # if the file does not exist
        # use the new list as the final one
        final_data = new_data

    # write the final list to the file
    with open(Path_File_CSV, 'w', newline='') as f_csv:
        writer = csv.writer(f_csv)
        writer.writerows(final_data)
"""
