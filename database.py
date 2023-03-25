import mysql.connector
import pandas as pd
from django.conf import settings

# Define the database connection parameters
db_config = {
    'host': 'xx',
    'user': 'xx',
    'password': 'xx',
    'database': 'test_sbg',
}

# Connect to MySQL
try:
    conn = mysql.connect(
        host="xx",
        user="xx",
        password="xx",
        database="test_sbg"
    )
    print("Connected to MySQL database")

except Error as e:
    print("Error connecting to MySQL database:", e)

# Define the function to upload CSV file to MySQL database
def upload_csv_to_mysql(file_path):
    # Read the CSV file into a pandas dataframe
    df = pd.read_csv(file_path)

    # Create table based on DataFrame columns
	columns = ', '.join([f"{col} VARCHAR(255)" for col in df.columns])
	create_table_query = f"CREATE TABLE IF NOT EXISTS my_table ({columns})"
	try:
	    cursor = conn.cursor()
	    cursor.execute(create_table_query)
	    print("Table created successfully")
	except Error as e:
	    print("Error creating table:", e)

	# Load DataFrame into MySQL table
	for _, row in df.iterrows():
	    values = "', '".join([str(val) for val in row.values])
	    insert_query = f"INSERT INTO my_table ({', '.join(df.columns)}) VALUES ('{values}')"
	    try:
	        cursor.execute(insert_query)
	        conn.commit()
	    except Error as e:
	        print("Error inserting row:", e)

    # Close the cursor and connection to the MySQL database
    cursor.close()
    conn.close()

# Define the Django view to handle file uploads and database insertion
def handle_file_upload(request):
    if request.method == 'POST' and request.FILES['file']:
        file = request.FILES['file']
        file_path = settings.MEDIA_ROOT / file.name
        with open(file_path, 'wb') as f:
            f.write(file.read())
        upload_csv_to_mysql(file_path)
    return render(request, 'upload_complete.html')
