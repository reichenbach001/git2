
from sql import Database
db=Database()
got = db.execute2('SELECT last_update,share_id FROM last_check WHERE share_id=2161110547458064;')
print(type(got))