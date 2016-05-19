const co = require('co');
const R = require('ramda');
const dbUtils = require('@panosoft/slate-db-utils');
const utils = require('./utils');

const _private = {
	logger: null,
	eventSourceDbClient: null,
	eventSourceConnectionString: '',
	maxEventsPerRead: 50000,
	maxEventsPerWrite: 10000,
	replicationClients: {
		maxEventSourceEventId: 0,
		replicationClientsList: []
	}
};	

const safeJSONParse = s => {
	try {
		return JSON.parse(s);
	}
	catch(err) {
		_private.logger.error({err: err}, `Error parsing string:  ${'\n'}${s}${'\n'}`);
		return null;
	}
};

const formatInsertStatement = (events) => {
	const eventList = R.reduce((acc, event) => {
		const id = event.id;
		const ts = event.ts;
		const entity_id = event.entity_id;
		const eventData = event.event;
		const escapeQuote = s => R.replace(/'/g, `''`, s);
		acc[acc.length] = `('${id}', '${ts.toISOString()}', '${entity_id}', '${escapeQuote(JSON.stringify(eventData))}')`;
		return acc;
	}, [], events);
	return 'INSERT INTO EVENTS (id, ts, entity_id, event) VALUES ' + R.join(', ',  eventList);
};

const replicate = co.wrap(function *(replicationClient) {
	try {
		_private.logger.info(`Replicating events greater than ${replicationClient.maximumEventId} to ${replicationClient.dbClient.database}`);
		const pooledEventSourceDbClient = yield dbUtils.createPooledClient(_private.eventSourceConnectionString);
		const eventSourceDbClientDatabase = pooledEventSourceDbClient.dbClient.database;
		const getListener = function(err) {
			_private.logger.error({err: err}, `Pooled Client Error for database ${eventSourceDbClientDatabase}`);
			throw err;
		};
		pooledEventSourceDbClient.dbClient.on('error', getListener);
		const selectStatement = `SELECT id, ts, entity_id, event FROM events
								WHERE id > ${replicationClient.maximumEventId} ORDER BY id LIMIT ${_private.maxEventsPerRead}`;
		// get object stream of rows from select statement
		const eventStream = dbUtils.createQueryStream(pooledEventSourceDbClient.dbClient, selectStatement);
		// pooledEventSourceDbClient events stream error handler
		eventStream.on('error', err => {
			_private.logger.error({err: err}, `Error detected in stream created for database ${eventSourceDbClientDatabase}`);
			throw err;
		});
		var rowsReplicated = 0;
		var eventsToCopy;
		while ((eventsToCopy = yield utils.getEventsFromStream(eventStream, _private.maxEventsPerWrite)).length > 0) {
			var insertStatement = formatInsertStatement(eventsToCopy);
			var result = yield dbUtils.executeSQLStatement(replicationClient.dbClient, insertStatement);
			if (eventsToCopy.length !== result.rowCount) {
				throw new Error('Program logic error  Event Count:', eventsToCopy.length, 'Rows Inserted:', result.rowCount);
			}
			// get maximum id replicated
			replicationClient.maximumEventId = utils.parseInteger(eventsToCopy[eventsToCopy.length - 1].id);
			rowsReplicated += eventsToCopy.length;
		}
		if (rowsReplicated === 0 ) {
			_private.logger.error(`Logic Error:  No rows replicated to ${replicationClient.dbClient.database}`);
			throw new Error(`No rows replicated to ${replicationClient.dbClient.database}`);
		}
		return rowsReplicated;
	}
	finally {
		if (eventStream) {
			eventStream.close();
		}
		// must remove events listener due to the way the connection pool works
		if (pooledEventSourceDbClient && getListener) {
			pooledEventSourceDbClient.dbClient.removeListener('error', getListener);
		}
		if (pooledEventSourceDbClient) {
			dbUtils.close(pooledEventSourceDbClient);
		}
	}
});

const processReplicationRequest = co.wrap(function *(replicationClient) {
	try {
		// replication client is not performing a copy
		if (!replicationClient.copyInProgress) {
			replicationClient.copyInProgress = true;
			// perform replication for replication client until equal with maximum event source id from NOTIFY
			while (replicationClient.maximumEventId < _private.replicationClients.maxEventSourceEventId) {
				var rowsReplicated = yield replicate(replicationClient);
				_private.logger.info(`After replicate to ${replicationClient.dbClient.database}`,
					{maxSourceId: _private.replicationClients.maxEventSourceEventId}, {maxDestId: replicationClient.maximumEventId}, {copyInProgress: replicationClient.copyInProgress},
					`Rows replicated:  ${utils.formatNumber(rowsReplicated)}`);
			}
			replicationClient.copyInProgress = false;
			_private.logger.info(`Replication up-to-date with latest event source id  ${replicationClient.dbClient.database}`,
				{maxSourceId: _private.replicationClients.maxEventSourceEventId}, {maxDestId: replicationClient.maximumEventId}, {copyInProgress: replicationClient.copyInProgress});
		}
	}
	catch(err) {
		_private.logger.error({err: err}, `Error detected processing replication request:`,
			{maxSourceId: _private.replicationClients.maxEventSourceEventId}, {maxDestId: replicationClient.maximumEventId}, {copyInProgress: replicationClient.copyInProgress});
		throw err;
	}
});

const processReplicationRequests = function (replicationClients) {
	// returning a map in case the caller wants to wait, i.e. yield on the array of Promises
	return R.map(function (replicationClient) {
		return processReplicationRequest(replicationClient);
	}, replicationClients.replicationClientsList);
};

const createNotificationListener = (eventSourceDbClient, replicationClients) => {
	eventSourceDbClient.on('notification', message => {
		// get message payload
		const payload = safeJSONParse(message.payload);
		if (!payload) {
			throw new Error('Could not parse message payload:  ' + message.payload);
		}
		const payloadId = utils.parseInteger(payload.id);
		// set new goal for the replicator
		if (payloadId > _private.replicationClients.maxEventSourceEventId) {
			_private.replicationClients.maxEventSourceEventId = payloadId;
		}
		// trigger replication AND do NOT wait
		// this would be problematic if the replicator didn't short circuit when copy in progress
		processReplicationRequests(replicationClients);
	});
};
const validateReplicationDatabase = co.wrap(function *(replicationClient, sourceEventWithMinId, eventSourceClientRowCount) {
	// get number of events
	const replicationClientRowCount = yield utils.getEventCount(replicationClient.dbClient);
	_private.logger.info(`Database:  ${replicationClient.dbClient.database}` +
		`    Event Source Maximum Event id:  ${_private.replicationClients.maxEventSourceEventId}    Replication Client Maximum EventId:  ${replicationClient.maximumEventId}` +
		`    Event Source Row Count:  ${utils.formatNumber(eventSourceClientRowCount)}` +
		`    Replication Client Row Count:  ${utils.formatNumber(replicationClientRowCount)}`);
	const destEventWithMinId = yield getEventWithMinimumEventId(replicationClient.dbClient);
	// replication database events table is not empty
	if (destEventWithMinId) {
		// event source database events table is not empty
		if (sourceEventWithMinId) {
			// wrong databases if event rows with minimum ids don't match
			if (!R.equals(sourceEventWithMinId, destEventWithMinId)) {
				throw new Error(`Row with minimum id in Event Source database (${_private.eventSourceDbClient.database}) events table: ${'\n' + JSON.stringify(sourceEventWithMinId)}` +
					`${'\n'}doesn't match row with minimum id in Replication database (${replicationClient.dbClient.database}) events table:  ${'\n' + JSON.stringify(destEventWithMinId)}`);
			}
		}
		// event source database events table is empty
		else {
			throw new Error(`Event Source database (${_private.eventSourceDbClient.database}) events table is empty but Replication database (${replicationClient.dbClient.database}) events table is not empty`);
		}
	}
	if (replicationClientRowCount > 0) {
		if (replicationClientRowCount > eventSourceClientRowCount) {
			throw new Error(`Replication database (${replicationClient.dbClient.database}) row count (${replicationClientRowCount})` +
				` is greater than Event Source database (${_private.eventSourceDbClient.database}) row count (${eventSourceClientRowCount})`);
		}
		if (replicationClient.maximumEventId > _private.replicationClients.maxEventSourceEventId) {
			throw new Error(`Replication database (${replicationClient.dbClient.database}) maximum event id (${replicationClient.maximumEventId})` +
				` is greater than Event Source database (${_private.eventSourceDbClient.database}) maximum event id (${_private.replicationClients.maxEventSourceEventId})`);
		}
	}
});

const validateReplicationDatabases = function (replicationClients, sourceEventWithMinId, eventSourceClientRowCount) {
	return R.map(function (replicationClient) {
		return validateReplicationDatabase(replicationClient, sourceEventWithMinId, eventSourceClientRowCount);
	}, replicationClients.replicationClientsList);
};

const getEventWithMinimumEventId = co.wrap(function *(dbClient) {
	// it is assumed that the minimum value for the id column is 1
	const selectStatement = `SELECT id, ts, entity_id, event FROM events
								WHERE id = (SELECT min(id) FROM events)`;
	const result = yield dbUtils.executeSQLStatement(dbClient, selectStatement);
	// no rows in table
	if (result.rowCount === 0) {
		return null;
	}
	// event with minimum event id found
	if (result.rowCount === 1) {
		const event = result.rows[0];
		const minId = utils.parseInteger(event.id);
		// minimum id is valid
		if (minId > 0) {
			return event;
		}
		// minimum id is invalid
		else {
			throw new Error(`Minimum events.id (${minId}) is invalid for database ${dbClient.database}`)
		}
	}
	// row count returned from select is invalid
	else {
		throw new Error(`Row count (${result.rowCount}) returned for SELECT statement is invalid for database ${dbClient.database}`);
	}
});

const initReplicationClients = co.wrap(function *(replicationConnectionParamsList) {
	yield R.map(co.wrap(function*(replicationConnectionParams) {
		const replicationClient = {};
		replicationClient.connectionString = dbUtils.createConnectionUrl(replicationConnectionParams);
		replicationClient.dbClient = yield dbUtils.createClient(replicationClient.connectionString);
		replicationClient.dbClient.on('error', function(err) {
			_private.logger.error({err: err}, `Replication Client Error for database ${replicationClient.dbClient.database}`);
			throw err;
		});
		// get maximum replication client event id
		replicationClient.maximumEventId = yield utils.getMaximumEventId(replicationClient.dbClient);
		replicationClient.copyInProgress = false;
		_private.logger.info(`Replication Client connected -  Database: ${replicationClient.dbClient.database}` +
			`    Host:  ${replicationClient.dbClient.host}    user:  ${replicationClient.dbClient.user}` + `    SSL Connection:  ${replicationClient.dbClient.ssl}`);
		_private.replicationClients.replicationClientsList = R.append(replicationClient, _private.replicationClients.replicationClientsList);
	}), replicationConnectionParamsList);
});

// Assumption:  function parameters have been validated
const replicator = co.wrap(function *(eventSourceConnectionParams, replicationConnectionParamsList, logger, maxEventsPerRead, maxEventsPerWrite, connectTimeout) {
	_private.logger = logger;
	dbUtils.setDefaultOptions({logger: logger, connectTimeout: connectTimeout});
	_private.maxEventsPerRead = maxEventsPerRead;
	_private.maxEventsPerWrite = maxEventsPerWrite;
	_private.eventSourceConnectionString = dbUtils.createConnectionUrl(eventSourceConnectionParams);
	_private.eventSourceDbClient = yield dbUtils.createClient(_private.eventSourceConnectionString);

	// event source client error handler
	_private.eventSourceDbClient.on('error', function(err) {
		_private.logger.error({err: err}, `Event Source Client Error for database ${_private.eventSourceDbClient.database}`);
		throw err;
	});

	// get number of events
	const eventSourceClientRowCount = yield utils.getEventCount(_private.eventSourceDbClient);
	// get maximum event source event id
	_private.replicationClients.maxEventSourceEventId = yield utils.getMaximumEventId(_private.eventSourceDbClient);
	_private.logger.info(`Event Source Client connected - Database: ${_private.eventSourceDbClient.database}` +
				`    Host:  ${_private.eventSourceDbClient.host}    user:  ${_private.eventSourceDbClient.user}` + `    SSL Connection:  ${_private.eventSourceDbClient.ssl}` +
				`    Row Count:  ${utils.formatNumber(eventSourceClientRowCount)}`);
	yield initReplicationClients(replicationConnectionParamsList);
	const sourceEventWithMinId = yield getEventWithMinimumEventId(_private.eventSourceDbClient);
	yield validateReplicationDatabases(_private.replicationClients, sourceEventWithMinId, eventSourceClientRowCount);
	// initialize event notification listener for notification source events
	createNotificationListener(_private.eventSourceDbClient, _private.replicationClients);
	const result = yield dbUtils.executeSQLStatement(_private.eventSourceDbClient, 'LISTEN eventsinsert');
	// perform initial replication AND do not wait
	processReplicationRequests(_private.replicationClients);
});

module.exports = replicator;

