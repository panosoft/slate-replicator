var config = {
	// maximum events read from events table per database operation.  optional parameter.  minumum value 50000.
	maxEventsPerRead: 50000,
	// end program when this number of event differences is detected.  optional parameter.  will check all events if
	// not specified no matter how many events don't match.
	maxDiffs: 100,
	// optional parameter.  database connection timeout in milliseconds.  default value:  15000.
	connectTimeout: 15000,
	// connection parameters to database for one of the events table in the comparison
	events1Params: {
		host: 'localhost',
		databaseName: 'sourceDb',
		user: 'user1',
		password: 'password1'
	},
	// connection parameters to database for second events table in the comparison
	events2Params: {
		host: 'localhost',
		databaseName: 'sourceDb',
		user: 'user1',
		password: 'password1'
	}
};

module.exports = config;