import dataset
from config import config

# Login
constr = config['CONSTR']
constr %= config['DB_KEY']

db = dataset.connect(constr, reflect_metadata=False)

table = db[config.get('TABLE_NAME')]


def upsert(row_dict):
    table.upsert(row_dict, ['id'], ensure=False)
