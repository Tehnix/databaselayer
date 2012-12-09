## Database abstraction layer ##
To easier work with databases.

### Python 3 ###
The current branch and onwards will be focused on python 3. If you need python 2 support, then you need to use version 1.0 (look for it in the tags).


### Example of usecase ###

<pre># Import the module
from databaselayer import database

# Create the database
myDB = database.Database('SQLite', 'database.sql')
# Create a table
myDB.execute(
    'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT)'
)
# Insert a few people in the users table
myDB.insert('users', {'username': 'John'})
myDB.insert('users', {'username': 'Tom'})
</pre>