import pandas as pd
import sqlalchemy

from sqlalchemy.orm import sessionmaker

class SQLiteRepo:
    def __init__(self, connection: str):
        engine = sqlalchemy.create_engine(connection)
        self.engine = engine

    def query(self, sql_query):
        with self.engine.connect() as conn:
            
            return pd.read_sql(sqlalchemy.text(sql_query), conn)
            
            
    def upload_df_to_db(self, df: pd.DataFrame, table_name: str, if_exists: str):
        """
        Uploads a DataFrame to a database table with schema validation.
        
        Args:
            df (pd.DataFrame): DataFrame to upload.
            table_name (str): Target table name.
            connection_string : SQLAlchemy engine object.
            if_exists (str): action to take if table alreading exists in db (append, replace, fail)
        
        Returns:
            bool: True if committed successfully, False otherwise.
        """
        
        inspector = sqlalchemy.inspect(self.engine)
    
        # Get existing table columns
        if table_name not in inspector.get_table_names():
            print(f"❌ Table '{table_name}' does not exist in the database.")
            return False
        
        db_columns = [col["name"] for col in inspector.get_columns(table_name) ]
        db_columns = [c for c in db_columns if "id" not in c]
        df_columns = [c for c in df.columns if "_id" not in c]
    
        # Validate schema
        missing_in_df = [c for c in db_columns if c not in df_columns]
        extra_in_df = [c for c in df_columns if c not in db_columns]
    
        if missing_in_df or extra_in_df:
            print("❌ Column mismatch detected:")
            if missing_in_df:
                print(f"   - Missing in DataFrame: {missing_in_df}")
            if extra_in_df:
                print(f"   - Extra in DataFrame: {extra_in_df}")
            return False
    
        # Transaction block
        Session = sessionmaker(self.engine)
        session = Session()
        with session.connection() as conn:
            try:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)
                conn.commit()
                print(f"✅ Successfully uploaded {len(df)} rows to '{table_name}'")
            except Exception as e:
                print("❌ Transaction failed, rolling back:", e)
                conn.rollback()
                return False
            finally:
                if conn:
                    conn.close()