# Fraud Detection Big Data Project

## Description
This project aims to analyze a financial transaction dataset to detect fraud and understand fraud trends based on different categories, periods, and geographic locations.

## Dataset Used
The dataset "fraud transactions dataset" is accessible on [Kaggle](https://www.kaggle.com/datasets/dermisfit/fraud-transactions-dataset/data). This dataset includes details such as:
- Date and time of the transaction
- Transaction amount in USD
- Credit card number
- Merchant involved in the transaction
- Transaction category (e.g., food, entertainment, etc.)
- Personal information of the cardholder (first name, last name, gender, address, city, state, zip code, date of birth, occupation)
- Fraud indicator (1 for fraud, 0 for legitimate)
- 
## Processing Pipeline
### Lambda Architecture
#### Batch Processing
- **MapReduce Job for Transaction Category Analysis:**
  - Counts the number of fraudulent transactions for each transaction category.
- **MapReduce Job for Date and City Analysis:**
  - Analyzes the number of fraudulent transactions by year and city.

#### Streaming Processing
- **Socket Server:**
  - Reads data line by line and sends it continuously.
- **Kafka:**
  - Kafka producer reads data line by line and sends it to a Kafka cluster.
  - Kafka consumers process the data in real-time.

### Data Storage
- **MySQL:**
  - Stores structured and historical data.
- **MongoDB:**
  - Stores real-time and unstructured data.

### Data Visualization
- **MongoDB Charts:**
  - Creates interactive visualizations of the data to understand fraud trends and generate reports.

## MapReduce Job Results
### Transaction Category Analysis
- **Top categories affected by fraud:**
  - **grocery_pos: 1743 fraudulent transactions**
  - **shopping_net: 1713 fraudulent transactions**
  - **shopping_pos: 843 fraudulent transactions**

### Date and City Analysis
- **Top cities affected by fraud in January 2019:**
  - **Hampton: 20 fraudulent transactions**
  - **Ironton: 13 fraudulent transactions**

## Project Usage
**Clone the repository:**
   ```bash
   git clone https://github.com/wiem2000/BigData_fraudulent_transactions.git
   ```
## Visualization
**Dashboard link**: 
**Demo link** : https://drive.google.com/file/d/125qUtlNldyzjKP0dIdNxySRzoc-gVeF6/view?usp=drivesdk
