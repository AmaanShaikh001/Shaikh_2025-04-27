# Shaikh_2025-04-27

# Store Monitoring API to track uptime and downtime of restaurants

## INSTRUCTIONS:

1. Install dependencies with this command <pip install -r requirements.txt>

2. To load the data run <python database.py> (Assuming CSVs are in the same folder as the script)

3. Then <python app.py> to start the server.

## Trigger a New Report in Postman:

1. If you haven't installed Postman, download it from https://www.postman.com/downloads/ and install it.

2. Open Postman and create a new request:
    Set the request type to POST.
    Enter the URL: http://127.0.0.1:5000/trigger_report.
    Click "Send." You should get a response like {"report_id": "some-unique-id"}

3. Copy the report_id, then create another request:
Set the request type to GET.
    Enter the URL: http://127.0.0.1:5000/get_report?report_id=your_copied_id (replace with the actual ID).
    Click "Send." You should see {"status": "Running"} initially, and later a CSV download if complete.

## Ideas for imporoving the solution

1. Process large datasets in chunks using pandas chunksize parameters to reduce memory usage for larger files.

2. Implement parallel processing using Python's multiprocessing to calculate uptime/downtime for multiple stores simultaneously to reduce report generation time.

3. Enhance error handling with detailed logs to track issues during report generation for better debugging.


# LINKS

Sample Output CSV: - https://drive.google.com/drive/folders/1SkOLtiSGkljwmNheRB4u1swRhaGWR7J1?usp=sharing