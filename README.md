kida
====

kida (key/value interfaced data accessing)is a database accessing library which help to access RDBMS as key/value storage in python.
It is metadata driven, so user may write a row as :

    context.save('TABLE_NAME', {'id':1, 'name':'john'}

kida support composited key for writing/getting/deleting, keys can be either loaded from database (PRIMARY KEY or UNIQUE KEY)
or configured manually.

kida supports MySQL/Oracle now.

kida's original name was PyDB.

