from pymysql import *

class MysqlConnector():
  
    connector = None
    cursor = None
    
    def __init__(self, host, port, database, user, password):
        if (MysqlCOnnector.connector == None or MysqlCOnnector.cursor == None):
            MysqlCOnnector.connector = connect(host=host, port=port, database=database, user=user, password=password)
            MysqlCOnnector.cursor = MysqlCOnnector.connector.cursor()
        else:
            pass
          
    def execute(self, sql_string):
        self.cursor.execute(sql_string)
        self.connector.commit()
        
def insert_to_mysql(dosqls, host, port, database, user, password):
    mysql = MysqlConnector(host=host, port=port, database=database, user=user, password=password)
    mysql.execute(dosqls)
            
            
