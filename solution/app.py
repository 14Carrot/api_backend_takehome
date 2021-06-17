from flask import Flask, request, jsonify
import sqlite3
import json
from pprint import pprint
from itertools import chain
 

app = Flask(__name__)

DBPATH = "../awake_labs_take_home.db"

@app.route("/data/store", methods=['GET',"POST"])
def messages_route():
    """
    An Api endpoint which takes the values from a json file, restructures them and stores them in a SQLite database.

    Returns: 
        A message indicating the state of the request and its corresponding status code. 
    """

    try: 
        # converts the json into a dictionary (filename has been hardcoded)
        file_dict = get_json_as_dict("data exported")
        

        # converts the dictionary into three array of tuples, each representing a different table 
        # List comprehension was utilized since it is a specialized and fast method of parsing through
        # large datasets. It is equivalent or faster than map (compiled C), and more readable
        current_user_table_array = [get_current_user_tuple(user["data"], user["id"], user["data"]["heartRates"]) for user in file_dict]
        hist_user_table_array = [get_history_user_tuple(user["id"], heart_rate) for user in file_dict for heart_rate in user["data"]["heartRates"]]
        hist_state_table_array = [get_history_state_tuple(user["id"], user["data"]) for user in file_dict]

        # Connect to the sqllite db. To improve the performance add a password to encrypt the .db file.
        # Unfortunately, I ran out of time 
        connection = sqlite3.connect(DBPATH)
        
            
        with connection:
            # Executes insertion calls in order to store the data in the database.
            # Executemany is faster than execute since sqllite uses executemany for each execute. 
            # If this ends up being the rate limiting step (which it most likely will) invoking this:
            # connection.execute('PRAGMA journal_mode = WAL')
            # may lead to a solution but it coould also be dangerous 
            connection.executemany('INSERT INTO current_user_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)', current_user_table_array)
            connection.executemany('INSERT INTO history_user_data VALUES (?, ?, ?, ?, ?, ?)', hist_user_table_array)
            connection.executemany('INSERT INTO history_state_data VALUES (?, ?, ?, ?)', hist_state_table_array)

        return "Records were inserted successfully", 200
    # case where an exception was caught
    except:
        return "Record insertion was unsuccessful",500


@app.route("/data/delete", methods=['GET',"POST"])
def delete_route():
    """
    An Api endpoint which deletes all values in the data tables

    Returns: 
        A message indicating the state of the request and its corresponding status code.
    """
    try: 
        connection = sqlite3.connect(DBPATH)
        with connection:
            delete_all(connection, "history_user_data")
            delete_all(connection, "history_state_data")
            delete_all(connection, "current_user_data")


        return "Records were deleted successfully", 200

    except:
        return "Record deletion was unsuccessful",500


@app.route("/data/select/<table>", methods=["GET"])
def select_route(table):
    """
    An Api endpoint which selects all values from a table.

    Returns: 
        A message indicating the state of the request and its corresponding status code.
    """

    try: 
        connection = sqlite3.connect(DBPATH)
        with connection:
            cursor = connection.cursor()
            sqlite_select_query = "SELECT * from " + table

            cursor.execute(sqlite_select_query)
            records = cursor.fetchall()


        return jsonify(records), 200
    # case where an exception was caught
    except:
        return "Record selectin was unsuccessful",500



def delete_all(conn, table):

    sql = 'DELETE FROM ' + table + ";"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    

def get_json_as_dict(filename):
    """
    A method that reads the file, then generates an array of dictionaries
    Parameters:
        filename: string representing the filename
    Returns: 
        an array that reprresents the data
    """
    with open(filename + ".json") as json_file:
        data = json.load(json_file)
        return data

def get_current_user_tuple(data, user_id, heart_rates):
    """
    A method that generates a tuple that will be inserted into current_user_data table
    Parameters:
        data: the dictionry mapped by the data key
        user_id: the integer mapped by the id key
        heart_rates: the array mapped by the heartRates key
    Returns: 
        a tuple that represents the most current data from one user
    """
    heart_rate_json = heart_rates[-1]
    current_user_tup = (user_id, 
                        data["time"], 
                        data["currentBpm"], 
                        data["anxietyLevel"], 
                        data["baselineProgress"], 
                        heart_rate_json["rrInterval"],
                        heart_rate_json["motion"],
                        data["state"]["batteryLevel"])
    return current_user_tup

def get_history_user_tuple(user_id, heart_rate):
    """
    A method that generates a tuple that will be inserted into history_user_data table
    Parameters:
        user_id: the integer mapped by the id key
        heart_rate: the json contained within the array mapped by the heartRates key
    Returns: 
        a tuple that represents a heart rate, histrical data point from one user
    """
    history_user_tup = (user_id,
                        heart_rate["time"],
                        heart_rate["rrInterval"],
                        heart_rate["heartRate"],
                        heart_rate["anxietyLevel"],
                        heart_rate["motion"])
    return history_user_tup

def get_history_state_tuple(user_id, data):
    """
    A method that generates a tuple that will be inserted into history_state_data table
    Parameters:
        user_id: the integer mapped by the id key
        data: the dictionry mapped by the data key
    Returns: 
        a tuple that represents miscillaneus state, historical data from one user
    """
    history_state_tup = (user_id,
                        data["time"],
                        data["baselineProgress"],
                        data["state"]["batteryLevel"])
    return history_state_tup

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
