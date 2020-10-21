# Python Arrow Flight Client Exmaple
The Arrow Flight endpoint in Dremio is exposed on port `32010`.

## Instructions on using this Python client
- Install and setup Python3 
- Install pyarrow
  - `pip install pyarrow`
- Customize `example.py`
  - Replace `username`, `password` with the corresponding credentials of your Dremio user
  - Replace `sql` with the query you want to run in Dremio
- From a command line or terminal window, execute `example.py` with `python3 example.py`
