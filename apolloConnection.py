from pyapollo import ApolloClient

## build a function to get config from apollo (appid: public-dip-etl)
client = ApolloClient(app_id = 'public-uba-etl',
                      cluster = 'default',
                      config_server_url = 'http://prod.apolloph.net:8080')

def get_config():
    return client.get_values()
