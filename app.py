from flask import Flask, jsonify, send_file, request
import uuid
import threading
import os
from report_generator import generate_report

app = Flask(__name__)

# Store report status
report_status = {}
report_paths = {}

@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    report_id = str(uuid.uuid4())
    report_status[report_id] = "Running"
    threading.Thread(target=run_report, args=(report_id,)).start()
    return jsonify({"report_id": report_id})

def run_report(report_id):
    try:
        report_path = generate_report(report_id)
        report_paths[report_id] = report_path
        report_status[report_id] = "Complete"
    except Exception as e:
        report_status[report_id] = f"Error: {str(e)}"

@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')
    if report_id not in report_status:
        return jsonify({"error": "Invalid report_id"}), 404
    status = report_status[report_id]
    if status == "Running":
        return jsonify({"status": "Running"})
    elif status == "Complete":
        return send_file(report_paths[report_id], as_attachment=True, download_name=f"report_{report_id}.csv")
    else:
        return jsonify({"error": status}), 500

if __name__ == '__main__':
    app.run(debug=True)