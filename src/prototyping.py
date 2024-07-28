import pandas as pd
from model import convertor

df = pd.read_csv("/root/.code/converter/assets/spotify-2023.csv", encoding='latin-1')
df = df.convert_dtypes(
infer_objects=True, convert_string=True, convert_integer=True, 
convert_boolean=True, convert_floating=True)

def prototype(table_name:str = 'tableXyz', output:int = 2) -> tuple:
    while True:
        try:
            head_statement = f"CREATE TABLE {table_name} AS ("
            dataframe_info = []
            for column in df:
                column_name = str(column)
                column_datatype = str(df[column_name].dtype)
                tuple = (column_name.lower(), column_datatype.lower())
                dataframe_info.append(tuple)
        except Exception as e:
            return False, f"An error occurred while reading datatypes on the selected dataframe | {e}"
        else:
            try:
                fetched_info = []
                sql_dType_options = [
                    ('string', 'VARCHAR'), ('int64','BIGINT'), ('float64','FLOAT'), ('bool','BOOL'), ('object', 'VARCHAR'), ('BLOB'), ('DOUBLE') 
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
                            #print(fetched_info)
                            print(f'| Processed columns: {processed_columns}\n| Fetched columns: {fetched_columns}')
                    except UserWarning:
                        return False, f"An error occurred while validating the fetched datatypes of the selected dataframe | {e}" # edit the return message
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
                                    body_statement = f"{body_statement}\n{tuple[0]} {tuple[1]}\n);"
                            
                            print(f"{head_statement}\n{body_statement}")
                        except Exception as e:
                            return False, f"An error occurred while generating the SQL statement of the selected dataframe | {e}"
                        else:
                            sql_statement = f"{head_statement}\n{body_statement}"
                            return True, sql_statement
            
print(prototype("hamad"))
stat, response = prototype("hamad", 2)
print(response)