## trying to use 'neo4j' package to interact with Neo4j database by using cypher language
from neo4j import GraphDatabase

class Neo4jConnection:
  
    driver = None
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        if Neo4jConnection.driver == None:
            try:
                self.driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
            except Exception as e:
                print("Failed to create the driver:", e)
        else:
            pass
          
    def close(self):
        if self.driver is not None:
            self.driver.close()
            
    def query(self, query, db=None):
        assert self.driver is not None, "Driver not initialized!"
        session = None
        response = None
        session = self.driver(database=db) if db is not None else self.driver.session()
        response = list(session.run(query))
        return response



