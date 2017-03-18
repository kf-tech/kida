PyDB

PyDB is a database accessing library which help to access RDBMS as key/value storage in python.
It is metadata driven, so user may write a row as :

    context.save('TABLE_NAME', {'id':1, 'name':'john'}

PyDB support composited key for writing/getting/deleting, keys can be either loaded from database (PRIMARY KEY or UNIQUE KEY)
or configured manually.

PyDB supports MySQL only now, an older version also supported Oracle.


