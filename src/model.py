import pandas as pd
from pandas import DataFrame

class convertor:
    """
    A class to handle operations on a dataframe loaded from a CSV file, 
    including cleaning, data type conversion, and exporting.
    """
    
    def __init__(self, filePath:str = "C:/file/path/here") -> None:
        """
        Initialize the class with the path to a CSV file and load it into a dataframe.

        Args:
            filePath (str, optional): The file path to the CSV file. Defaults to "C:/file/path/here".
        """
        self.path = filePath
        self.dataframe = DataFrame
        
        try:
            self.dataframe = pd.read_csv(filePath, encoding='latin-1')
        except FileNotFoundError:
            print("File Not Found | The provided filepath is invalid")
        except Exception:
            return None
        else:
            print("File has been read successfully!")
        
    def getDataframe(self) -> DataFrame:
        """
        Get the dataframe loaded from the CSV file.

        Returns:
            DataFrame: The dataframe loaded from the CSV file.
        """
        return self.dataframe
    
    def getInfo(self):
        """
        Print a concise summary of the dataframe.

        Returns:
            None
        """
        return self.getDataframe().info()

    def getColumnLength(self) -> int:
        """
        Get the number of columns in the dataframe.

        Returns:
            int: The number of columns in the dataframe.
        """
        return len(self.dataframe.columns)
    
    def columnsCheck(self) -> int:
        """
        Check for the presence of columns with NA/null values.

        Returns:
            int: The number of columns that contain NA/null values.
        """
        na_counter = 0
        for i in self.dataframe:
            if self.dataframe[f'{i}'].isnull().sum() > 0:
                na_counter += 1
            else:
                continue        
        return na_counter
    
    def cleanDataset(self) -> str:
        """
        Clean the dataset by removing rows with any NA/null values.

        Returns:
            str: A message indicating the result of the cleaning operation.
        """
        try:  
            if self.columnsCheck() > 0:
                self.dataframe = self.dataframe.dropna(how='any',axis=0)
            else:
                return True ,f"Dataset is clear of any na/null values"
        except Exception as e:
            return False, f"An error occurred while cleaning columns of the selected dataframe | {e}"
        else:
            return True ,f"Dataframe has been updated and cleared of any na/null values"

    def convertDataType(self, attr: str, dataType: str) -> tuple:
        """
        Convert the data type of a specified column in the dataframe.

        Args:
            attr (str): The name of the column to be converted.
            dataType (str): The target data type for the column. 
            Possible values: 
            'string', 'int32', 'int64', 
            'bool', 'float64', 'float32', 
            'float', 'object'.

        Raises:
            UserWarning (1): If the column name is not found in the dataframe.
            UserWarning (2): If the specified data type is not a valid option.

        Returns:
            tuple: A tuple containing a boolean indicating success or failure, 
                and a message with details.
        """
        try:
            if attr not in self.getDataframe():
                    raise UserWarning
        except UserWarning:
                return False, f"column not in dataframe"
        else:
            try:
                datatypes_options = ['string', 'int32', 'int64', 
                                     'bool', 'float64', 'float32', 
                                     'float', 'object']
                if dataType.lower() not in datatypes_options:
                    raise UserWarning
            except UserWarning:
                return False, f"An error occurred while processing datatype options"
            else:
                # Converting the datatype of that selected column
                try:
                    self.dataframe = self.dataframe.astype({
                        f'{attr}': f'{dataType.lower()}'
                    })
                except:
                    return False, f"An error occurred while converting the selected attribute"
                else:
                    print(self.dataframe[f"{attr}"].dtype)
                    return True, f"Attribute is converted successfully as {self.dataframe[f'{attr}'].dtype}"
    
    def autoConvertDatatype(self) -> tuple:
        try:
            validating_data = self.columnsCheck()
            if validating_data > 0:
                raise UserWarning
        except UserWarning:
            return False, f"Unable To Export Data | Your dataframe contains na/null values"
        except Exception as e:
            return False, f"An error occurred while validating data of the selected dataframe | {e}"
        else:
            try:
                self.dataframe = self.dataframe.convert_dtypes(
                infer_objects=True, convert_string=True, convert_integer=True, 
                convert_boolean=True, convert_floating=True)
            except:
                return False, f"An error occurred while converting datatypes on the selected dataframe"
            else:
                return True, f"Datatypes conversion is successful\nIt is advised to double check the dataframe"
            
    def data_sql_modifier(self, table_name:str = 'tableXyz', output:int = 2) -> tuple:
        """
        Generates a SQL CREATE TABLE statement based on the given DataFrame's schema.

        Args:
            table_name (str, optional): The name of the table to be created. Defaults to 'tableXyz'.
            output (int, optional): Determines the output format. If 1, returns the fetched column info. If 2, prints and returns the SQL statement. Defaults to 2.
            df (DataFrame): represents the dataframe stored/handled within the class/object.
        Raises:
            UserWarning: will be raised if there is a mismatch/failure/ and missing data OR certain requirements are not met.

        Returns:
            tuple: A tuple where the first element is a boolean indicating success or failure, and the second element is either:
                - A list of tuples (column_name, datatype) if `output` is 1.
                - A SQL CREATE TABLE statement if `output` is 2.
                - An error message if there is a failure.
        """
        df = self.dataframe
        try:
            head_statement = f"CREATE TABLE {table_name} ("
            dataframe_info = []
            for column in df:
                column_name = str(column)
                column_datatype = str(df[column_name].dtype)
                column_name = column_name.replace(" ", "_")
                tuple = (column_name.lower(), column_datatype.lower())
                dataframe_info.append(tuple)
        except Exception as e:
            return False, f"An error occurred while reading datatypes on the selected dataframe | {e}"
        else:
            try:
                fetched_info = []
                sql_dType_options = [
                ('string', 'VARCHAR(255)'), ('int64', 'BIGINT'), ('float64', 'FLOAT'), ('DOUBLE', 'DOUBLE'), 
                ('bool', 'BOOL'), ('object', 'VARCHAR(255)'), ('datetime64[ns]', 'TIMESTAMP'), ('BLOB', 'BLOB')               
                ]
                for content in dataframe_info:
                    cname = content[0]
                    ctype = content[1]
                    for option in sql_dType_options:
                        if ctype == option[0]:
                            fetched_tuple = (cname, option[1])
                            fetched_info.append(fetched_tuple)
                        else:
                            continue
            except Exception as e:
                return False, f"An error occurred while fetching datatypes of the selected dataframe | {e}"
            else:
                if output == 1:
                    return True, fetched_info
                else:
                    try:
                        processed_columns = len(dataframe_info)
                        fetched_columns = len(fetched_info)
                        if processed_columns != fetched_columns:
                            raise UserWarning
                        else:
                            print(f'| Processed columns: {processed_columns}\n| Fetched columns: {fetched_columns}')
                    except UserWarning:
                        return False, f"An error occurred while validating the fetched datatypes of the selected dataframe | Not all columns are fetched, some are missing" # edit the return message
                    except Exception as e:
                        return False, f"An error occurred while validating the fetched datatypes of the selected dataframe | {e}"
                    else:
                        try:
                            body_statement = ""
                            tuple_count = len(fetched_info)
                            for tuple in fetched_info:
                                if tuple_count > 1:
                                    tuple_count -= 1
                                    body_statement = f"{body_statement}\n{tuple[0]} {tuple[1]},"
                                else:
                                    body_statement = f"{body_statement}\n{tuple[0]} {tuple[1]}\n);\n"
                        except Exception as e:
                            return False, f"An error occurred while generating the SQL statement of the selected dataframe | {e}"
                        else:
                            sql_statement = f"{head_statement}{body_statement}"
                            return True, sql_statement

    def export_as_mysql(self, table_name:str = 'tableXyz', output:int = 2, path:str = "Output") -> tuple:
        """
        Exports data to a MySQL formatted file.

        Args:
            table_name (str, optional): The name of the table to which data will be exported. Defaults to 'tableXyz'.
            output (int, optional): Specifies the type of output modification to be applied. Defaults to 2.
                Available Choices: 1, and 2 (If 1, returns the fetched column info. If 2, prints and returns the SQL statement. Defaults to 2.)
            path (str, optional): The file path where the output will be saved. Defaults to "Output".
                Note: Do not include a file formate as '.txt' to the output chosen path or name. 
                Example Of Path Formate: /C:/Folder/converter/assets/file_name (the function will export the output file in a .txt formate automatically)

        Raises:
            UserWarning(1): If the dataframe contains NaN or null values.
            UserWarning(2): If data_sql_modifier fails to modify/return the data.

        Returns:
            tuple: A tuple containing a boolean status and a message. 
                   - (True, "File has been created successfully!") on success.
                   - (False, "Error message") on failure, where "Error message" is a description of what went wrong OR default error message.
        """
        try:
            validating_data = self.columnsCheck()
            insert_statement = f"INSERT INTO {table_name} VALUES \n"
            if validating_data > 0:
                raise UserWarning
        except UserWarning:
            return False, f"Unable To Export Data | Your dataframe contains na/null values"
        except Exception as e:
            return False, f"An error occurred while validating data of the selected dataframe | {e}"
        else:
            try:
                state, imported_content = self.data_sql_modifier(table_name, output)
                if state == False:
                    raise UserWarning
            except UserWarning:
                state, content
            else:
                try:
                    file = open(path + ".txt", "w")
                except Exception as e:
                    return False, f"An error occurred while creating output file | {e}"
                else:
                    try:
                        imported_content = imported_content + "\n"
                        file.write(imported_content)
                    except Exception as e:
                        return False, f"An error occurred while writing 'imported_content' to output file | {e}"
                    else:
                        try:
                            file.write(insert_statement)
                        except Exception as e:
                            return False, f"An error occurred while writing 'insert_statement' to output file | {e}"
                        else:
                            try:
                                content = []
                                for i in range(len(self.getDataframe())):
                                    record = tuple(self.getDataframe().iloc[i])
                                    content.append(record)
                            except Exception as e:
                                return False, f"An error occurred while fetching data | {e}"
                            else:
                                    try:
                                        counter = len(content)
                                        for record in content:
                                            counter -= 1
                                            if counter > 0:
                                                file.write(str(record) + ',' + '\n')
                                            else:
                                                file.write(str(record) + ';')

                                    except Exception as e:
                                        return False, f"An error occurred while writing output file | {e}"
                                    else:
                                        try:
                                            file.close()
                                        except Exception as e:
                                            return False, f"An error occurred while closing output file | {e}"
                                        else:
                                            return True, f"File has been created successfully!"