var config = {
	// optional parameter.  maximum events written to the replicationDestinations events tables per database operation.  minimum value:  10000.
	maxEventsPerWrite: 10000,
	// optional parameter.  database connection timeout in milliseconds.  default value:  15000.
	connectTimeout: 15000,
	// connection parameters to immutable event source database events table
	eventSource: {
		host: 'localhost',
		databaseName: 'sourceDb',
		// optional parameter.  connection attempt will fail if missing and needed by database.
		user: 'user1',
		// optional parameter.  connection attempt will fail if missing and needed by database.
		password: 'password1'
	},
	// array of connection parameters to one or more replication databases events tables.  one replication destination is required.
	replicationDestinations: [
		{
			host: 'localhost',
			databaseName: 'replicationDb1',
			// optional parameter.  connection attempt will fail if missing and needed by database.
			user: 'user1',
			// optional parameter.  connection attempt will fail if missing and needed by database.
			password: 'password1'
		},
		{
			host: 'localhost',
			databaseName: 'replicationDb2',
			// optional parameter.  connection attempt will fail if missing and needed by database.
			user: 'user1',
			// optional parameter.  connection attempt will fail if missing and needed by database.
			password: 'password1'
		}
	]
};

module.exports = config;