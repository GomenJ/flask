from flask import Flask, request, jsonify, send_from_directory, g
from flask_cors import CORS
import pyodbc
import os
from dotenv import load_dotenv
import logging  # Import logging

load_dotenv()  # Load environment variables from .env file

app = Flask(__name__, static_folder="dist", static_url_path="")

# --- Configure Logging ---
# Configure this properly based on your deployment needs
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)
CORS(app)

SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
CONN_STR = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"


def get_db():
    """
    Opens a new database connection if there is none yet for the
    current application context. Reuses connection if already opened
    during the same request.
    """
    if not CONN_STR:  # Check if connection string is valid from startup
        app.logger.error("Database connection string is not configured.")
        return None

    if "db" not in g:
        try:
            # Connect using the globally defined CONN_STR
            g.db = pyodbc.connect(CONN_STR)
            app.logger.debug("New DB connection established for request context.")
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            app.logger.error(f"Failed to connect to database: {sqlstate} - {ex}")
            g.db = None  # Store None to indicate failure within this request
    # Return the connection (or None if connection failed)
    return g.db


@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    db = g.pop("db", None)  # Get connection from g, or None if not set/failed
    if db is not None:
        db.close()
        app.logger.debug("DB connection closed for request context.")
    if error:  # Log any errors that occurred during the request handling
        app.logger.error(f"Flask teardown_appcontext caught an error: {error}")


@app.route("/api/v1/gas", methods=["GET"])
def get_data_by_date_and_fuente():
    conn = get_db()

    # 2. Check if connection was successful
    if conn is None:
        # Connection failed during get_db() or config was bad
        return jsonify(
            {"error": "Database service unavailable"}
        ), 503  # 503 Service Unavailable is appropriate

    try:
        trade_date = request.args.get("trade_date")
        fuente = request.args.get("fuente")

        if not trade_date or not fuente:
            return jsonify({"error": "Both trade_date and fuente are required"}), 400

        query = """
        SELECT id, trade_date, flow_date, indice, precio, fuente, usuario, fecha_creacion, fecha_actualizacion
        FROM GAS
        WHERE trade_date = ? AND fuente = ?
        ORDER BY flow_date
        """

        cursor = conn.cursor()
        cursor.execute(query, (trade_date, fuente))
        result = cursor.fetchall()
        cursor.close()  # Close the cursor when done with it
        # Convert result to JSON format
        data = [
            {
                "id": row[0],
                "tradeDate": row[1].strftime("%Y-%m-%d"),
                "flowDate": row[2].strftime("%Y-%m-%d"),
                "indice": row[3],
                "precio": float(row[4]),
                "fuente": row[5],
                "usuario": row[6],
                "fechaCreacion": row[7].strftime("%Y-%m-%d %H:%M:%S"),
                "fechaActualizacion": row[8].strftime("%Y-%m-%d %H:%M:%S"),
            }
            for row in result
        ]

        return jsonify({"data": data}), 200

    except Exception as e:
        print(f"Error fetching data: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/gas/<indice>", methods=["GET"])
def get_matching_dates(indice):
    indices = ["HH", "EP", "HSC", "SCL", "WAH"]
    if indice not in indices:
        return jsonify({"error": "Invalid indice"}), 400
    conn = get_db()

    # 2. Check if connection was successful
    if conn is None:
        # Connection failed during get_db() or config was bad
        return jsonify(
            {"error": "Database service unavailable"}
        ), 503  # 503 Service Unavailable is appropriate

    try:
        query = """
            SELECT DISTINCT trade_date
            FROM GAS 
            WHERE indice = ?
            ORDER BY trade_date DESC
        """

        cursor = conn.cursor()
        cursor.execute(query, (indice,))
        dates = [row.trade_date.strftime("%Y-%m-%d") for row in cursor.fetchall()]
        cursor.close()

        return jsonify({"availableDates": dates}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/gas/<string:indice>/<string:trade_date>", methods=["GET"])
def get_gas_data(indice, trade_date):
    conn = get_db()

    # 2. Check if connection was successful
    if conn is None:
        # Connection failed during get_db() or config was bad
        return jsonify(
            {"error": "Database service unavailable"}
        ), 503  # 503 Service Unavailable is appropriate

    try:
        indices = ["HH", "EP", "HSC", "SCL", "WAH"]
        if indice not in indices:
            return jsonify({"error": "Invalid indice"}), 400
        query = """
        SELECT TOP 36 * FROM GAS
        WHERE trade_date = ? AND indice = ?
        ORDER BY flow_date
        """
        # ORDER BY fecha_creacion ASC

        cursor = conn.cursor()

        cursor.execute(query, (trade_date, indice))

        data = cursor.fetchall()
        cursor.close()  # Close the cursor when done with it
        result = [
            dict(zip([column[0] for column in cursor.description], row)) for row in data
        ]

        return jsonify(result), 200

    except Exception as e:
        print("Error fetching data:", e)
        return jsonify({"error": "Failed to fetch data"}), 500


@app.route("/api/v1/gas", methods=["GET", "POST"])
def get_all_fees():
    """Retrieve all fee data"""
    conn = get_db()

    if conn is None:
        # Connection failed during get_db() or config was bad
        return jsonify(
            {"error": "Database service unavailable"}
        ), 503  # 503 Service Unavailable is appropriate

    cursor = conn.cursor()
    try:
        cursor.execute("SELECT TOP 10 volumen, meses, fee, fee_version FROM gas_fee")
        results = cursor.fetchall()
        cursor.close()  # Close the cursor when done with it

        fees = [
            {"volumen": row[0], "meses": row[1], "fee": row[2], "fee_version": row[3]}
            for row in results
        ]

        return jsonify({"fees": fees}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def store_data():
    conn = get_db()

    # 2. Check if connection was successful
    if conn is None:
        # Connection failed during get_db() or config was bad
        return jsonify(
            {"error": "Database service unavailable"}
        ), 503  # 503 Service Unavailable is appropriate

    try:
        if not request.is_json:
            return jsonify({"error": "Invalid JSON format"}), 400

        data_list = request.get_json()
        # WARNING: This code is insecure and vulnerable to SQL injection attacks
        query = """
        INSERT INTO GAS (trade_date, flow_date, indice, precio, fuente, usuario, fecha_creacion, fecha_actualizacion)
        VALUES
        """

        cursor = conn.cursor()

        if not isinstance(data_list, list):
            return jsonify({"error": "Expected a list of data objects"}), 400

        values_to_insert = ""  # List to hold the values for the multi-insert

        for data in data_list:
            try:
                trade_date = data.get("tradeDate")
                flow_date = data.get("flowDate")
                indice = data.get("indice")
                precio = data.get("precio")
                fuente = data.get("fuente")
                usuario = data.get("usuario")
                fecha_creacion = data.get("fechaCreacion")
                fecha_actualizacion = data.get("fechaActualizacion")

                values_to_insert += f"('{trade_date}', '{flow_date}', '{indice}', '{precio}', '{fuente}', '{usuario}', '{fecha_creacion}', '{fecha_actualizacion}'), "

            except Exception as inner_e:
                return jsonify(
                    {"error": f"Error processing one of the items: {str(inner_e)}"}
                ), 500

        if len(values_to_insert) > 1:  # Check if there is data to insert
            query += f"{values_to_insert}"
            query = query[:-2]

            cursor.execute(query)
            conn.commit()
            conn.close()  # Close the cursor when done with it

            return jsonify({"message": "Data inserted successfully"}), 201
        else:
            return jsonify({"message": "No data to insert"}), 204  # 204 No Content

    except Exception as e:
        print(f"Error processing data list: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/gas/fee/<int:meses>", methods=["GET"])
def get_gas_fee(meses):
    """Retrieve a fee based on months and volumen"""
    conn = get_db()

    if conn is None:
        # Connection failed during get_db() or config was bad
        return jsonify(
            {"error": "Database service unavailable"}
        ), 503  # 503 Service Unavailable is appropriate
    cursor = conn.cursor()

    try:
        volumen = request.args.get("volumen", type=int)

        if volumen is None:
            return jsonify({"error": "Missing volumen"}), 400

        query = """
            SELECT id, volumen, meses, fee, fee_version
            FROM gas_fee
            WHERE volumen = (
                SELECT MAX(volumen)
                FROM gas_fee
                WHERE volumen <= ?
            )
            AND meses = (
                SELECT MIN(meses)
                FROM gas_fee
                WHERE meses >= ?
            );
        """

        cursor.execute(query, (volumen, meses))
        result = cursor.fetchone()
        cursor.close()  # Close the cursor when done with it

        if result is None:
            return jsonify({"error": "No matching fee found"}), 404

        fee = {
            "id": result[0],
            "volumen": result[1],
            "meses": result[2],
            "fee": result[3],
            "fee_version": result[4],
        }

        return jsonify({"fee": fee}), 200

    except Exception as e:
        print(f"Error: {e}")  # More detailed error logging
        return jsonify({"error": "An error occurred"}), 500  # General server error.


@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({"status": "ok"}), 200


@app.route("/")
def serve_index():
    assert app.static_folder is not None, (
        "Static folder must be configured for serve_index route."
    )
    return send_from_directory(app.static_folder, "index.html")


@app.errorhandler(404)
def not_found(e):
    path = request.path
    if "." not in path.split("/")[-1] and not path.startswith("/api/"):
        assert app.static_folder is not None, (
            "Static folder must be configured for serve_index route."
        )
        return send_from_directory(app.static_folder, "index.html")
    # Otherwise, return the default 404 response
    return e  # Or return jsonify({"error": "Not Found"}), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)  # Set debug=False in production
