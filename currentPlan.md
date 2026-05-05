


CSP554 — Big Data Technologies
Stock Market Data Processing Pipeline
Complete Project Execution Plan

Team Members
Phase	Members	Email	Deadline
Phase 1	Muthu Nageswaran	mkalyaninarayanamoor@hawk.illinoistech.edu	April 30
Phase 1	Anusha Venkatesh	avenkatesh2@hawk.illinoistech.edu	April 30
Phase 2	Shiva Raghav Rajasekar	srajasekar@hawk.illinoistech.edu	May 2
Phase 2	Arya Shetty	ashetty19@hawk.illinoistech.edu	May 2
Phase 3	Om Ashokkumar Patel	opatel8@hawk.illinoistech.edu	May 4
Phase 3	Souptik Sinha	ssinha21@hawk.illinoistech.edu	May 4


Illinois Institute of Technology  |  Spring 2025
 
Project Overview
This document is your step-by-step execution guide. Read your phase completely before starting. Every step, every command, every decision is explained here so you can complete your part even if you have never done this before.

What Are We Building?
A data pipeline that works like a factory assembly line:

Yahoo Finance  →  CSV Files  →  Kafka  →  Python Processor  →  DynamoDB  →  Charts

Each part has one job. You build them one at a time.
Technology Stack (All Free)
Tool	Purpose	Cost
yfinance (Python library)	Download stock market data	Free, no account needed
Apache Kafka (via Docker)	Stream data like a live feed	Free, open source
Docker Desktop	Runs Kafka on your laptop	Free
Python 3.11+	All programming	Free
AWS DynamoDB	Cloud database to store results	Free tier (25GB)
Pandas + Matplotlib	Data analysis and charts	Free, open source
Folder Structure — Create This First (Everyone)
Create ONE shared folder called stock-pipeline. Inside it create these subfolders:
•	stock-pipeline/
○	data/          ← CSV files go here
○	producer/      ← sends data to Kafka
○	consumer/      ← reads from Kafka, processes data
○	storage/       ← saves to DynamoDB
○	analysis/      ← charts and statistics
○	README.md      ← project description

NOTE: Use Google Drive or GitHub to share code between team members. All 6 people should have access to the same folder.
 
PHASE 1 — Data Collection & Kafka Setup
Phase 1
Members: Muthu Nageswaran & Anusha Venkatesh
Deadline: Deadline: April 30

You are responsible for two things: (1) downloading all the stock data, and (2) setting up Kafka so data can flow through it. Everything in Phase 2 depends on your work being done correctly.
Part A — Install Everything (Both Members Do This)
Before writing any code, you need to install these tools on your computer.
Step 1: Install Python
1.	Go to python.org in your browser
2.	Click the big yellow Download button
3.	Run the installer — IMPORTANT: check the box that says 'Add Python to PATH' before clicking Install
4.	After install, open a terminal (Command Prompt on Windows, Terminal on Mac) and type: python --version
5.	You should see something like: Python 3.11.4 — if yes, Python is installed correctly
Step 2: Install Docker Desktop
6.	Go to docker.com/products/docker-desktop
7.	Download the version for your operating system (Windows or Mac)
8.	Install it. After install, open Docker Desktop from your applications
9.	You will see a whale icon in your system tray (Windows) or menu bar (Mac)
10.	Docker needs to be RUNNING (whale icon visible) every time you work on this project
Step 3: Install VS Code (Code Editor)
11.	Go to code.visualstudio.com
12.	Download and install
13.	This is where you will write all your Python code
Step 4: Install Python Libraries
14.	Open a terminal/command prompt
15.	Run this command exactly:
pip install yfinance pandas kafka-python boto3
16.	Wait for it to finish. You will see 'Successfully installed...' at the end
17.	If you see any red errors, try: pip3 install yfinance pandas kafka-python boto3
Part B — Download Stock Market Data (Muthu)
You will download 5 years of historical stock data for 5 companies. This data will be the input to your entire pipeline.
Step 1: Create the download script
18.	Open VS Code
19.	Open the stock-pipeline folder
20.	Inside the data/ folder, create a new file called download_data.py
21.	Write a Python script that does the following:
○	Import yfinance and pandas libraries
○	Create a list of 5 stock symbols: AAPL, GOOGL, MSFT, TSLA, AMZN
○	For each stock symbol, use yf.download() to get daily data from 2020-01-01 to 2024-12-31
○	Add a column called 'symbol' to each downloaded dataset with the stock name
○	Save each dataset as a CSV file in the data/ folder (e.g., AAPL.csv, GOOGL.csv, etc.)
○	Print how many rows were downloaded for each stock
Step 2: Run the download script
22.	Open terminal, navigate to your stock-pipeline folder
23.	Run: python data/download_data.py
24.	Wait about 30 seconds. You should see messages like 'Saved 1258 rows for AAPL'
25.	Check the data/ folder — you should have 5 CSV files there
Step 3: Verify your data
26.	Open AAPL.csv in VS Code or Excel
27.	You should see columns: Date, Open, High, Low, Close, Volume, symbol
28.	You should have about 1,250 rows (one row per trading day for 5 years)
29.	Total across all 5 files: approximately 6,250 rows

TIP: Save all 5 CSV files to your shared Google Drive folder immediately after downloading so the whole team has access.
Part C — Set Up Kafka Using Docker (Anusha)
Kafka is a messaging system. Think of it as a post office. Your producer will put stock records into Kafka like dropping letters in a mailbox. The consumer (Phase 2) will pick them up one by one. Docker lets you run Kafka on your laptop without any server setup.
Step 1: Create the Docker Compose file
30.	In your stock-pipeline/ folder (the main folder, not inside any subfolder), create a file called docker-compose.yml
31.	This file tells Docker what to start. You need two services:
○	Zookeeper: Kafka's internal manager. You don't interact with it directly, it just needs to run
○	Kafka: The actual messaging system. Runs on port 9092
32.	The file should configure Zookeeper on port 2181 and Kafka on port 9092, and tell Kafka where Zookeeper is

NOTE: Ask your professor or Claude to give you the docker-compose.yml content if you are unsure what to write — it is a configuration file, not Python code.
Step 2: Start Kafka
33.	Make sure Docker Desktop is running (whale icon visible)
34.	Open terminal, navigate to the stock-pipeline/ folder
35.	Run: docker-compose up -d
36.	The -d means it runs in the background. First time may take 2-3 minutes to download images
37.	After it finishes, run: docker ps
38.	You should see two containers listed — one with 'zookeeper' in the name and one with 'kafka' in the name
39.	If both appear with status 'Up', Kafka is running successfully
Step 3: Create a Kafka Topic
A topic is like a named mailbox in Kafka. Your producer will send to this topic, your consumer will read from it.
40.	Find the exact name of your Kafka container by running: docker ps
41.	Copy the container name (it will look something like 'stock-pipeline-kafka-1')
42.	Run this command (replace CONTAINER_NAME with your actual container name):
docker exec -it CONTAINER_NAME kafka-topics --create --topic stock-market-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
43.	You should see: Created topic stock-market-data
44.	Verify it exists: docker exec -it CONTAINER_NAME kafka-topics --list --bootstrap-server localhost:9092
Part D — Write the Kafka Producer (Both Members)
The producer is a Python script that reads your CSV files row by row and sends each row as a message to Kafka. This simulates a live data stream — the records arrive one by one with a small delay, just like real stock data would arrive during market hours.
Step 1: Create producer.py
45.	Inside the producer/ folder, create a file called producer.py
Step 2: What the script must do (write code to accomplish these steps)
46.	Import the required libraries: kafka-python's KafkaProducer, json, time, pandas, and os
47.	Create a KafkaProducer that connects to localhost:9092 and converts messages to JSON bytes before sending
48.	Read all 5 CSV files from the data/ folder one by one
49.	For each row in each CSV file, send the row as a JSON message to the topic called 'stock-market-data'
50.	After each send, wait 0.1 seconds (this creates the simulated real-time effect)
51.	Print a status message every 100 records so you can see progress
52.	After all records are sent, print 'All data sent successfully'
Step 3: Test the producer
53.	Open TWO terminal windows side by side
54.	In terminal 1, run the producer: python producer/producer.py
55.	In terminal 2, run the Kafka console consumer to verify messages are arriving:
docker exec -it CONTAINER_NAME kafka-console-consumer --topic stock-market-data --bootstrap-server localhost:9092 --from-beginning
56.	You should see JSON records printing in terminal 2 as the producer sends them
57.	If you see JSON data flowing — Phase 1 is complete!
Phase 1 Deliverables Checklist
Item	Done?
Python installed and working (python --version shows 3.11+)	
Docker Desktop installed and running	
All Python libraries installed (yfinance, pandas, kafka-python, boto3)	
5 CSV files downloaded in data/ folder (~6250 rows total)	
CSV files shared on Google Drive	
docker-compose.yml created	
Kafka running (docker ps shows 2 containers)	
Topic 'stock-market-data' created	
producer.py written and tested	
Messages visible in Kafka console consumer	

IMPORTANT: Phase 2 cannot start without working Kafka and CSV files. Communicate with Phase 2 team when your work is ready.
 
PHASE 2 — Data Processing, Consumer & DynamoDB
Phase 2
Members: Shiva Raghav Rajasekar & Arya Shetty
Deadline: Deadline: May 2

You are responsible for the core intelligence of the pipeline. You will write the consumer that reads from Kafka, performs calculations on each record, and saves the results to AWS DynamoDB. You pick up where Phase 1 left off.

IMPORTANT: Before starting, confirm with Phase 1 team that Kafka is running and the producer works. You need both to test your consumer.
Part A — Set Up AWS DynamoDB (Shiva)
DynamoDB is Amazon's cloud database. It stores your processed stock records permanently. AWS has a free tier that is more than enough for this project.
Step 1: Create an AWS Account
58.	Go to aws.amazon.com
59.	Click 'Create an AWS Account'
60.	You will need: an email address, a password, and a credit/debit card (you will NOT be charged if you stay in free tier)
61.	Complete the sign-up process. Choose 'Basic Support' (free) when asked
62.	After sign-up, go to the AWS Console at console.aws.amazon.com

TIP: The team can share one AWS account. One person creates it and shares the credentials with the person writing the storage code.
Step 2: Create the DynamoDB Table
63.	In the AWS Console, use the search bar at the top and type 'DynamoDB'
64.	Click DynamoDB from the results
65.	Click the orange 'Create table' button
66.	Fill in the table details exactly as follows:
○	Table name: StockData
○	Partition key: symbol (type: String) — this is like the category column
○	Sort key: date (type: String) — this makes each symbol+date row unique
67.	Leave all other settings as default
68.	Click 'Create table' at the bottom
69.	Wait 1-2 minutes. The table status will change from 'Creating' to 'Active'
70.	When Active, your database is ready
Step 3: Create an IAM User (Security Credentials)
You need credentials so your Python code can access DynamoDB. Never use your root account credentials in code.
71.	In AWS Console, search for 'IAM' and click it
72.	In the left menu, click 'Users'
73.	Click 'Create user'
74.	Username: stock-pipeline-user
75.	Click Next. On the permissions screen, select 'Attach policies directly'
76.	Search for 'AmazonDynamoDBFullAccess' and check the checkbox next to it
77.	Click Next, then Create user
78.	Click on the user you just created
79.	Go to the 'Security credentials' tab
80.	Scroll down to 'Access keys', click 'Create access key'
81.	Select 'Local code' as the use case, click Next, then Create
82.	IMPORTANT: Download the CSV file. This file contains your Access Key ID and Secret Access Key. You cannot see the Secret Key again after closing this window

IMPORTANT: Keep the Access Key CSV file safe. Treat it like a password. Never share it publicly or put it on GitHub.
Step 4: Configure AWS on Your Computer
83.	Open terminal
84.	Run: aws configure
85.	If you get 'aws command not found', install it: pip install awscli
86.	Enter your Access Key ID when asked
87.	Enter your Secret Access Key when asked
88.	Default region: type us-east-1
89.	Default output format: type json
90.	Press Enter after each entry
Part B — Write the Kafka Consumer (Arya)
The consumer is the brain of the pipeline. It listens to Kafka, receives each stock record, runs calculations on it, and sends the enriched record to DynamoDB. It runs simultaneously with the producer.
Understanding the calculations you need to implement
For every record that arrives from Kafka, you must calculate these 4 things:

Calculation	What It Means	Formula / Logic
Percentage Change	How much did the stock price change today?	((Close - Open) / Open) x 100
7-Day Moving Average	Average closing price over last 7 days. Smooths out noise.	Keep last 7 Close prices per stock, calculate average
Anomaly Flag	Was today's price movement unusually large?	If |pct_change| > 3%, mark as anomaly = True
Volume Spike Flag	Was today's trading volume unusually high?	If today's volume > 2x average volume of last 7 days, mark as spike = True

NOTE: You need to keep separate tracking for each stock symbol. For example, AAPL's moving average should only use AAPL's prices, not mix with TSLA.
Step 1: Create consumer.py
91.	Inside the consumer/ folder, create a file called consumer.py
Step 2: What the script must do
92.	Import libraries: KafkaConsumer from kafka, json, boto3, collections (for deque/defaultdict)
93.	Create a data structure to store recent prices per stock — a dictionary where each key is a stock symbol and value is a list of the last 7 closing prices
94.	Create a data structure for recent volumes per stock — same idea
95.	Create a KafkaConsumer that connects to localhost:9092 and subscribes to the topic 'stock-market-data'
96.	Create a boto3 DynamoDB resource connected to us-east-1
97.	Get a reference to the 'StockData' table
98.	Start an infinite loop that reads messages from Kafka:
○	Parse each message from JSON to a Python dictionary
○	Extract: symbol, date, open, high, low, close, volume
○	Calculate pct_change = ((close - open) / open) * 100, rounded to 2 decimal places
○	Add close price to that stock's recent price list. Keep only the last 7 values
○	Calculate moving_avg_7 = average of recent prices list (only if 7 or more values exist)
○	Add volume to that stock's recent volume list. Keep only last 7 values
○	Calculate avg_volume = average of recent volume list
○	Set is_anomaly = True if absolute value of pct_change > 3, else False
○	Set volume_spike = True if volume > 2 * avg_volume, else False
○	Call a save_to_dynamodb function with the enriched record
○	Print the record to the screen
Step 3: Write the save_to_dynamodb function
99.	This function receives the enriched record as a dictionary
100.	It uses boto3 to call table.put_item()
101.	The item saved must include: symbol, date, open, high, low, close, volume, pct_change, moving_avg_7, is_anomaly, volume_spike
102.	All numeric values must be converted to strings or Decimal before saving (DynamoDB does not accept Python floats directly)
103.	Wrap the put_item call in a try/except block to handle any errors gracefully
Step 4: Test the consumer and producer together
104.	Open TWO terminal windows
105.	Terminal 1: Run the producer: python producer/producer.py
106.	Terminal 2: Run the consumer: python consumer/consumer.py
107.	You should see the consumer printing enriched records as the producer sends them
108.	After 2-3 minutes, go to the AWS DynamoDB console, click your StockData table, click 'Explore table items'
109.	You should see records appearing in the table with all your calculated fields

TIP: Run both producer and consumer for at least 5 minutes to accumulate enough data for Phase 3 analysis.
Phase 2 Deliverables Checklist
Item	Done?
AWS account created	
DynamoDB table 'StockData' created with symbol (PK) and date (SK)	
IAM user created with DynamoDB access	
AWS credentials configured on local machine (aws configure)	
consumer.py written with all 4 calculations	
Consumer successfully reads from Kafka topic	
Records saved to DynamoDB with all fields	
Verified data in DynamoDB console	
Producer + Consumer run simultaneously without errors	

IMPORTANT: Phase 3 team needs the DynamoDB table populated with data. Run the full pipeline (producer + consumer) until all 6,250 records are processed before handing off.
 
PHASE 3 — Analysis, Charts & Final Report
Phase 3
Members: Om Ashokkumar Patel & Souptik Sinha
Deadline: Deadline: May 4

You are responsible for the final layer: pulling data from DynamoDB, generating analysis charts, writing the README, and putting together the final report. Your work is what the professor will see and evaluate most directly.

IMPORTANT: Before starting, confirm with Phase 2 that DynamoDB is populated. You can check: AWS Console → DynamoDB → StockData → Explore table items. You should see thousands of records.
Part A — Analysis Script (Om)
You will write a Python script that pulls all processed data from DynamoDB and runs analysis on it.
Step 1: Install additional libraries
110.	Open terminal and run:
pip install matplotlib boto3 pandas
Step 2: Create analysis.py
111.	Inside the analysis/ folder, create a file called analysis.py
Step 3: Pull data from DynamoDB
112.	Use boto3 to connect to DynamoDB
113.	Use table.scan() to retrieve all records from StockData table
114.	DynamoDB may return data in pages (1MB limit per scan). Use a loop that checks for 'LastEvaluatedKey' and keeps scanning until all data is retrieved
115.	Load all records into a Pandas DataFrame
116.	Convert numeric columns (close, pct_change, moving_avg_7, volume) from string to float/int
Step 4: Generate these 5 charts (save each as PNG in analysis/charts/)

Chart	Type	What It Shows	How To Create
Chart 1: Stock Price Over Time	Line chart	Closing price for each of the 5 stocks from 2020-2024	Filter by each symbol, plot Date vs Close, one line per stock, use different colors
Chart 2: Moving Average vs Actual Price	Line chart	AAPL's actual price alongside its 7-day moving average	Filter AAPL records, plot Date vs Close AND Date vs moving_avg_7 on same chart
Chart 3: Anomaly Detection	Line + scatter	Price line with red dots on anomaly days	Plot price line, then overlay scatter plot of only rows where is_anomaly=True in red
Chart 4: Volume Spikes	Bar chart	Daily volume for TSLA with spikes highlighted in red	Bar chart of volume, color bars red where volume_spike=True, blue otherwise
Chart 5: Anomaly Count Per Stock	Bar chart	Which stock had the most anomalous days	Group by symbol, count rows where is_anomaly=True, plot as bar chart

Step 5: For each chart, use this structure
117.	Create a new matplotlib figure with plt.figure(figsize=(12, 6))
118.	Plot your data
119.	Add title using plt.title()
120.	Add axis labels using plt.xlabel() and plt.ylabel()
121.	Add a legend using plt.legend()
122.	Save with plt.savefig('analysis/charts/chart_name.png', dpi=150, bbox_inches='tight')
123.	Close with plt.close() before the next chart
Step 6: Print a statistics summary
At the end of analysis.py, print these statistics to the terminal:
•	Total records processed
•	Total anomalies detected
•	Anomaly rate percentage (anomalies / total * 100)
•	Stock with highest average daily change
•	Stock with most volume spikes
•	Date range of data (earliest to latest date)

TIP: Screenshot this terminal output — it goes in your report as 'Pipeline Results'.
Part B — README File (Souptik)
The README is the first thing anyone reads when they look at your project. Write it clearly.
Create README.md in the main stock-pipeline/ folder
The README must contain these sections in order:
124.	Project Title and Description (2-3 sentences: what it does, what technologies it uses)
125.	Architecture Diagram — create a simple text-based diagram showing the pipeline flow:
yfinance → CSV → Kafka Producer → [Kafka Topic] → Kafka Consumer → DynamoDB → Analysis
126.	Team Members — list all 6 names and emails
127.	Prerequisites — list everything that needs to be installed (Python 3.11+, Docker, AWS account, pip libraries)
128.	Setup Instructions — step by step how to set up from scratch:
○	Clone/download the project
○	Install libraries command
○	Start Kafka command
○	Configure AWS command
129.	How to Run — in exact order:
○	Step 1: python data/download_data.py
○	Step 2: docker-compose up -d
○	Step 3: python producer/producer.py  (run in terminal 1)
○	Step 4: python consumer/consumer.py  (run in terminal 2, simultaneously)
○	Step 5: Wait for all records to process
○	Step 6: python analysis/analysis.py
130.	Expected Output — describe what the user should see when everything works
131.	Project Structure — show the folder structure
132.	References — copy from the proposal
Part C — Final Report (Both Members)
The final report is a document (PDF or Word) that explains what you built and what you found. Write it clearly as if explaining to someone who has not seen your project.
Report Sections
Section	Content	Length
1. Introduction	What problem does this solve? Why is stock market data hard to process? What does your pipeline do?	half page
2. Architecture	Paste your pipeline diagram. Explain each component in 1-2 sentences each.	1 page
3. Data Source	Explain yfinance, what stocks you chose, why, how many records, date range.	half page
4. Real-Time Simulation	Explain why you simulated real-time instead of using live data. Quote the theme: 'real-time or simulated'.	quarter page
5. Processing Logic	Explain your 4 calculations: pct_change, moving average, anomaly detection, volume spike. No code needed, just plain English.	1 page
6. Results & Charts	Paste all 5 charts. Below each chart, write 2-3 sentences explaining what it shows.	2 pages
7. Pipeline Statistics	Paste your terminal output statistics. How many records? How many anomalies? What patterns did you find?	half page
8. Challenges	What was difficult? What errors did you hit? How did you solve them?	half page
9. Future Work	What would you add with more time? (real-time API, ML model, live dashboard)	quarter page
10. References	Copy references from the proposal. Add yfinance and Apache Kafka documentation links.	quarter page

TIP: Write the report in Google Docs so everyone can collaborate. Export to PDF at the end.
Part D — Architecture Diagram (Souptik)
Create a proper architecture diagram using draw.io (free, no download needed — open diagrams.net in your browser).
Step 1: Go to diagrams.net
133.	Open diagrams.net in your browser (it's free, no account needed)
134.	Click 'Create New Diagram'
135.	Select Blank Diagram
Step 2: Draw these boxes connected by arrows (left to right)
Box	Shape	Color
Yahoo Finance / yfinance	Cylinder (database shape)	Blue
CSV Files	Document shape	Green
Kafka Producer	Rectangle	Orange
Kafka Topic: stock-market-data	Queue shape or rectangle	Dark Orange
Kafka Consumer + Processor	Rectangle (with 4 calculations listed inside)	Purple
AWS DynamoDB	Cylinder	Red
Analysis Layer	Rectangle	Teal
Charts & Reports	Document shape	Green
Step 3: Add labels to arrows
•	CSV Files → Kafka Producer: 'Row by row replay'
•	Kafka Producer → Kafka Topic: 'JSON messages'
•	Kafka Topic → Consumer: 'Real-time stream'
•	Consumer → DynamoDB: 'Enriched records'
•	DynamoDB → Analysis: 'Historical query'

136.	Export as PNG (File → Export As → PNG)
137.	Save to analysis/charts/architecture_diagram.png
138.	Use this diagram in the report Section 2
Phase 3 Deliverables Checklist
Item	Done?
analysis.py pulls all data from DynamoDB	
Chart 1: Stock prices over time — saved as PNG	
Chart 2: Moving average vs actual price — saved as PNG	
Chart 3: Anomaly detection plot — saved as PNG	
Chart 4: Volume spikes — saved as PNG	
Chart 5: Anomaly count per stock — saved as PNG	
Statistics summary printed and screenshotted	
README.md complete with all sections	
Architecture diagram created and saved	
Final report written with all 10 sections	
All 5 charts included in report with explanations	
Report exported as PDF	
 
Common Problems & How to Fix Them
Problem	Likely Cause	Fix
pip install fails	Python not in PATH	Reinstall Python, check 'Add to PATH' box. Try pip3 instead of pip
Docker containers not starting	Docker Desktop not open	Open Docker Desktop, wait for whale icon, then try again
Kafka connection refused	Kafka not running	Run 'docker ps' — if no containers, run 'docker-compose up -d' again
Producer runs but consumer sees nothing	Topic name mismatch	Check topic name is exactly 'stock-market-data' in both files
DynamoDB access denied	Wrong credentials	Run 'aws configure' again with correct keys from your IAM CSV file
yfinance returns empty data	Date range issue	Try a different date range. Use 2022-01-01 to 2023-12-31 as a backup
Moving average shows None for first records	Not enough data yet	This is normal — moving average only calculates after 7 records per stock
DynamoDB put_item fails	Float values not accepted	Convert all numeric values to str() or Decimal() before saving
Charts look empty	Data not loaded correctly	Print the DataFrame head/shape before plotting to verify data is there
Consumer crashes after running	Memory or connection issue	Add try/except around the main loop and restart consumer

Quick Reference — Who Does What
Task	Person	Deadline
Install Python, Docker, VS Code	Everyone (individually)	Before Phase 1
Download stock CSV files	Muthu	April 30
Set up Kafka (docker-compose + topic)	Anusha	April 30
Write Kafka producer	Muthu + Anusha	April 30
Set up AWS DynamoDB + IAM	Shiva	May 2
Write Kafka consumer + processing logic	Arya	May 2
Write analysis.py + 5 charts	Om	May 4
Write README.md	Souptik	May 4
Architecture diagram	Souptik	May 4
Final report	Om + Souptik	May 4

If a Professor Asks — Key Talking Points

Q: Why did you simulate real-time instead of using live data? A: The project theme explicitly says 'real-time or simulated'. Free API tiers provide end-of-day data, not live streams. Replaying historical data through Kafka is standard industry practice — Netflix, Uber, and financial firms all test pipelines this way. Our architecture is identical to a live setup; only the data source would change.

Q: Why Kafka instead of AWS Kinesis? A: The theme says 'Kafka or AWS Kinesis'. Kafka is open-source and industry-standard. Our architecture mirrors exactly what a Kinesis-based system would do — the streaming concepts are identical.

Q: What would you add with more time? A: (1) Connect to a real-time market data API, (2) Add a machine learning model for price prediction, (3) Build a live dashboard using Grafana or Streamlit, (4) Add automated alerts for anomaly detection.


Good luck, team. Build it one step at a time.


Note: We completed all phase just remaining with report but skip that for now.