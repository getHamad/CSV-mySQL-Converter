# CSV to MySQL Converter


## Table of Contents
- [About the Project](#about-the-project)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Methods](#methods)
  - [getDataframe](#getdataframe)
  - [getInfo](#getinfo)
  - [getColumnLength](#getcolumnlength)
  - [columnsCheck](#columnscheck)
  - [cleanDataset](#cleandataset)
  - [convertDataType](#convertdatatype)
  - [autoConvertDatatype](#autoconvertdatatype)
  - [data_sql_modifier](#data_sql_modifier)
  - [export_as_mysql](#export_as_mysql)
- [License](#license)
- [Acknowledgements](#acknowledgements)
- [Disclaimer](#disclaimer)

## About the Project
The `convertor` class handles various operations on a DataFrame loaded from a CSV file, including loading, cleaning, converting data types, and exporting to a MySQL-compatible format. The class uses pandas for DataFrame operations and supports detailed error handling and logging for robust functionality.

#
 Project Code `P.PmSQL.003`



## Getting Started
To get a local copy up and running, follow these simple steps, and use the included dataset that is included in /assets folder as an example.

### Prerequisites
Make sure you have the following installed:
- Python 3.8+
- pandas
- python-dotenv (optional)

### Installation
1. Clone the repository
   \\```bash
2. Navigate to the project directory
   \\```bash
   cd CSV-mySQL-Converter
   \\```
3. Install dependencies
   \\```bash
   pip install -r requirements.txt
   \\```

## Usage
To use the `convertor` class, you need to instantiate it with the path to your CSV file and then call the desired methods.

### Example
\\```python
from convertor import convertor

#### Initialize the convertor with the path to your CSV file
conv = convertor(filePath="C:/data/myfile.csv")

#### Get the DataFrame
df = conv.getDataframe()

#### Print a concise summary of the DataFrame
conv.getInfo()

#### Get the number of columns in the DataFrame
num_columns = conv.getColumnLength()

#### Check for the presence of columns with NA/null values
num_na_columns = conv.columnsCheck()

#### Clean the dataset by removing rows with any NA/null values
cleaning_message = conv.cleanDataset()

#### Convert the data type of a specified column
status, message = conv.convertDataType('column_name', 'int64')

#### Automatically convert the data types of the DataFrame's columns
status, message = conv.autoConvertDatatype()

#### Generate a SQL CREATE TABLE statement based on the DataFrame's schema
status, sql_statement = conv.data_sql_modifier(table_name='my_table', output=2)

#### Export data to a MySQL formatted file
status, message = conv.export_as_mysql(table_name='my_table', output=2, path="C:/data/output")


## Methods

### `getDataframe`
Returns the DataFrame loaded from the CSV file.

#### Returns
- `DataFrame`: The DataFrame loaded from the CSV file.

### `getInfo`
Prints a concise summary of the DataFrame.

#### Returns
- `None`

### `getColumnLength`
Returns the number of columns in the DataFrame.

#### Returns
- `int`: The number of columns in the DataFrame.

### `columnsCheck`
Checks for the presence of columns with NA/null values.

#### Returns
- `int`: The number of columns that contain NA/null values.

### `cleanDataset`
Cleans the dataset by removing rows with any NA/null values.

#### Returns
- `str`: A message indicating the result of the cleaning operation.

### `convertDataType`
Converts the data type of a specified column in the DataFrame.

#### Parameters
- `attr` (str): The name of the column to be converted.
- `dataType` (str): The target data type for the column. Possible values: 'string', 'int32', 'int64', 'bool', 'float64', 'float32', 'float', 'object'.

#### Returns
- `tuple`: A tuple containing a boolean indicating success or failure, and a message with details.

### `autoConvertDatatype`
Automatically converts the data types of the DataFrame's columns.

#### Returns
- `tuple`: A tuple containing a boolean indicating success or failure, and a message with details.

### `data_sql_modifier`
Generates a SQL CREATE TABLE statement based on the given DataFrame's schema.

#### Parameters
- `table_name` (str, optional): The name of the table to be created. Defaults to 'tableXyz'.
- `output` (int, optional): Determines the output format. If 1, returns the fetched column info. If 2, prints and returns the SQL statement. Defaults to 2.

#### Returns
- `tuple`: A tuple where the first element is a boolean indicating success or failure, and the second element is either:
  - A list of tuples (column_name, datatype) if `output` is 1.
  - A SQL CREATE TABLE statement if `output` is 2.
  - An error message if there is a failure.

### `export_as_mysql`
Exports data to a MySQL formatted file.

#### Parameters
- `table_name` (str, optional): The name of the table to which data will be exported. Defaults to 'tableXyz'.
- `output` (int, optional): Specifies the type of output modification to be applied. Defaults to 2. Available choices: 1 and 2.
- `path` (str, optional): The file path where the output will be saved. Defaults to "Output".

#### Returns
- `tuple`: A tuple containing a boolean status and a message. 
  - (True, "File has been created successfully!") on success.
  - (False, "Error message") on failure, where "Error message" is a description of what went wrong OR default error message.

## License
Distributed under the MIT License. See `LICENSE` for more information.

## Acknowledgements
- [Pandas](https://pandas.pydata.org/)
- [Python](https://www.python.org/)
- [Kaggle](https://www.kaggle.com/datasets)

## Disclaimer
**Note:** This project is developed by an early-stage developer, and it represents one of my initial projects. As such, it may contain bugs, errors, or unexpected behavior. While I have made efforts to ensure the functionality and reliability of the convertor class, there may still be areas that require improvement or refinement.

**Use at Your Own Risk:** Please be aware that using this convertor class in production environments or critical systems is not recommended without thorough testing and validation. It is advisable to evaluate the class's performance and functionality before deploying it in sensitive applications.

**Contributions and Feedback:** I welcome contributions, suggestions, and feedback from the community to enhance and improve this project. If you encounter any issues or have ideas for enhancements, please feel free to open an issue or submit a pull request on GitHub.

Thank you for your understanding and cooperation.
