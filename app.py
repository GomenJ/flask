from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

app = Flask(__name__, static_folder="dist", static_url_path="")

CORS(app)

# Database connection details (loaded from environment variables)
SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

try:
    conn_str = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit()  # Exit if database connection fails


@app.route("/api/v1/gas", methods=["GET"])
def get_data_by_date_and_fuente():
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

        cursor.execute(query, (trade_date, fuente))
        result = cursor.fetchall()

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

    try:
        query = """
            SELECT DISTINCT trade_date
            FROM GAS 
            WHERE indice = ?
            ORDER BY trade_date DESC
        """
        cursor.execute(query, (indice,))
        dates = [row.trade_date.strftime("%Y-%m-%d") for row in cursor.fetchall()]

        return jsonify({"availableDates": dates}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/gas/<string:indice>/<string:trade_date>", methods=["GET"])
def get_gas_data(indice, trade_date):
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
        cursor.execute(query, (trade_date, indice))

        data = cursor.fetchall()
        result = [
            dict(zip([column[0] for column in cursor.description], row)) for row in data
        ]

        return jsonify(result), 200

    except Exception as e:
        print("Error fetching data:", e)
        return jsonify({"error": "Failed to fetch data"}), 500


@app.route("/api/v1/gas", methods=["POST"])
def store_data():
    try:
        if not request.is_json:
            return jsonify({"error": "Invalid JSON format"}), 400

        data_list = request.get_json()
        # WARNING: This code is insecure and vulnerable to SQL injection attacks
        query = """
        INSERT INTO GAS (trade_date, flow_date, indice, precio, fuente, usuario, fecha_creacion, fecha_actualizacion)
        VALUES
        """

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

            return jsonify({"message": "Data inserted successfully"}), 201
        else:
            return jsonify({"message": "No data to insert"}), 204  # 204 No Content

    except Exception as e:
        conn.rollback()
        print(f"Error processing data list: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/gas/fee", methods=["GET"])
def get_all_fees():
    """Retrieve all fee data"""
    try:
        cursor.execute("SELECT TOP 10 volumen, meses, fee, fee_version FROM gas_fee")
        results = cursor.fetchall()

        fees = [
            {"volumen": row[0], "meses": row[1], "fee": row[2], "fee_version": row[3]}
            for row in results
        ]

        return jsonify({"fees": fees}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/gas/fee/<int:meses>", methods=["GET"])
def get_gas_fee(meses):
    """Retrieve a fee based on months and volumen"""
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


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve(path):
    print("Requested path:", path)  # Debugging log
    file_path = os.path.join(app.static_folder, path)
    if path != "" and os.path.exists(file_path):
        print("Serving static file:", file_path)
        return send_from_directory(app.static_folder, path)
    else:
        print("Serving index.html")
        return send_from_directory(app.static_folder, "index.html")


@app.errorhandler(404)
def not_found(e):
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)  # Set debug=False in production
