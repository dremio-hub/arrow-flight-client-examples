from pyarrow import flight
import pyarrow as pa

class HttpDremioClientAuthHandler(flight.ClientAuthHandler):

    def __init__(self, username, password):
        super(flight.ClientAuthHandler, self).__init__()
        self.basic_auth = flight.BasicAuth(username, password)
        self.token = None

    def authenticate(self, outgoing, incoming):
        auth = self.basic_auth.serialize()
        outgoing.write(auth)
        self.token = incoming.read()

    def get_token(self):
        return self.token

username = '<USERNAME>'
password = '<PASSWORD>'
sql = '''<SQL_QUERY>'''

client = flight.FlightClient('grpc+tcp://<DREMIO_COORDINATOR_HOST>:32010')
client.authenticate(HttpDremioClientAuthHandler(username, password)) 
info = client.get_flight_info(flight.FlightDescriptor.for_command(sql))
reader = client.do_get(info.endpoints[0].ticket)
batches = []
while True:
    try:
        batch, metadata = reader.read_chunk()
        batches.append(batch)
    except StopIteration:
        break
data = pa.Table.from_batches(batches)
df = data.to_pandas()