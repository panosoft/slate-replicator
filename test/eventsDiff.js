const co = require('co');
const R = require('ramda');
var path = require('path');
const fs = require('fs');
var bunyan = require('bunyan');
const bformat = require('bunyan-format');
var program = require('commander');
const is = require('is_js');
const dbUtils = require('@panosoft/slate-db-utils');
const utils = require('../lib/utils');

const formatOut = bformat({ outputMode: 'long' });

const logger = bunyan.createLogger({
	name: 'eventsDiff',
	stream: formatOut,
	serializers: bunyan.stdSerializers
});

const exit = exitCode => setTimeout(_ => process.exit(exitCode), 1000);

process.on('uncaughtException', err => {
	logger.error({err: err}, `Uncaught exception:`);
	exit(1);
});
process.on('unhandledRejection', (reason, p) => {
	logger.error("Unhandled Rejection at: Promise ", p, " reason: ", reason);
	exit(1);
});
const handleSignal = signal => process.on(signal, _ => {
	logger.info(`${signal} received.`);
	exit(0);
});
R.forEach(handleSignal, ['SIGINT', 'SIGTERM']);

const eventsValidationString = fs.readFileSync('test/sql/loadPersonDataValidation.sql', 'utf8');

const logProgress = (delayInSecs) => {
	const start = new Date();
	return setInterval(() => logger.info(`--------> Operation Elapsed Time:  ${Math.round((new Date() - start) / 1000)} seconds`), delayInSecs * 1000);
};

const compareEvents = (events1, events2, diffIds) => {
	return new Promise((resolve, reject) => {
		var i = 0;
		R.forEach(function (event1) {
			if (!R.equals(event1, events2[i])) {
				diffIds[diffIds.length] = utils.parseInteger(event1.id);
			}
			i++;
		}, events1);
		resolve({countEvents: events1.length, thresholdEventId: utils.parseInteger(events1[events1.length - 1].id)});
	});
};

const getEvents = co.wrap(function *(connectionUrl, selectStatement, maxEventsPerRead) {
	let dbClient;
	let getListener;
	try {
		dbClient = yield dbUtils.createPooledClient(connectionUrl);
		const dbClientDatabase = dbClient.dbClient.database;
		getListener = err => {
			_private.logger.error({err: err}, `Pooled Client Error for database ${dbClientDatabase}`);
			throw err;
		};
		// dbClient error handler
		dbClient.dbClient.on('error', getListener);
		const eventStream = dbUtils.createQueryStream(dbClient.dbClient, selectStatement);
		// dbClient events stream error handler
		eventStream.on('error', err => {
			logger.error({err: err}, `Error detected in stream created for database ${dbClientDatabase}`);
			throw err;
		});
		return yield utils.getEventsFromStream(eventStream, maxEventsPerRead);
	}
	finally {
		// must remove events listener due to the way the connection pool works
		if (dbClient && getListener) {
			dbClient.dbClient.removeListener('error', getListener);
		}
		if (dbClient) {
			dbUtils.close(dbClient);
		}
	}
});

const processEvents = co.wrap(function *(events1ConnectionUrl, events2ConnectionUrl, maxEventsPerRead, thresholdEventId, diffIds) {
	const selectStatement = `SELECT id, ts, entity_id, event FROM events
							WHERE id > ${thresholdEventId} ORDER BY id LIMIT ${maxEventsPerRead}`;
	const events1 = yield getEvents(events1ConnectionUrl, selectStatement, maxEventsPerRead);
	const events2 = yield getEvents(events2ConnectionUrl, selectStatement, maxEventsPerRead);
	// events exist
	if (events1.events.length > 0) {
		return yield compareEvents(events1.events, events2.events, diffIds);
	}
	// unexpected no events found
	else {
		throw new Error('No More Events');
	}
});

const isEventsTableValid = co.wrap(function *(dbClient) {
	logger.info(`Checking events table validity for database ${dbClient.database}.  NOTE:  This operation may take several minutes.`);
	const intervalObject = logProgress(60);
	const result = yield dbUtils.executeSQLStatement(dbClient, eventsValidationString);
	clearInterval(intervalObject);
	if (result.rowCount === 1) {
		const tableStats = result.rows[0];
		if (R.test(/^VALID/, tableStats.events_table_status)) {
			logger.info(tableStats, `events table is valid for database ${dbClient.database}`);
			return true;
		}
		else {
			logger.error(tableStats, `events table is invalid for database ${dbClient.database}`);
			return false;
		}
	}
	else {
		logger.error(result, `result returned is not formatted properly`);
		return false;
	}
});

const eventsDiff = co.wrap(function *(events1ConnectionParams, events2ConnectionParams, logger, maxEventsPerRead, maxDiffs, connectTimeout, validateTables) {
	try {
		const diffIds = [];
		dbUtils.setDefaultOptions({logger: logger, connectTimeout: connectTimeout});
		var startDate = new Date();
		logger.info('\n### STARTED:  ' + startDate + ' ###\n');
		const events1ConnectionUrl = dbUtils.createConnectionUrl(events1ConnectionParams);
		const dbClient1 = yield dbUtils.createPooledClient(events1ConnectionUrl);
		const dbClient1Database = dbClient1.dbClient.database;

		// dbClient1 error handler
		const getListener1 = err => {
			logger.error({err: err}, `dbClient1 Error for database ${dbClient1Database}`);
		};
		dbClient1.dbClient.on('error', getListener1);

		// get maximum dbClient1 event id
		const maxDbClient1EventId = yield utils.getMaximumEventId(dbClient1.dbClient);
		const dbClient1RowCount = yield utils.getEventCount(dbClient1.dbClient);
		logger.info(`dbClient1 connected - Database: ${dbClient1Database}` +
			`    Host:  ${dbClient1.dbClient.host}    user:  ${dbClient1.dbClient.user}` +
			`    SSL Connection:  ${dbClient1.dbClient.ssl}    Events Table -- Maximum Event id:  ${maxDbClient1EventId}    Row Count:  ${utils.formatNumber(dbClient1RowCount)}`);
		const events2ConnectionUrl = dbUtils.createConnectionUrl(events2ConnectionParams);
		const dbClient2 = yield dbUtils.createPooledClient(events2ConnectionUrl);
		const dbClient2Database = dbClient2.dbClient.database;

		// dbClient2 error handler
		const getListener2 = err => {
			logger.error({err: err}, `dbClient2 Error for database ${dbClient2Database}`);
		};
		dbClient2.dbClient.on('error', getListener2);

		// get maximum dbClient2 event id
		const maxDbClient2EventId = yield utils.getMaximumEventId(dbClient2.dbClient);
		const dbClient2RowCount = yield utils.getEventCount(dbClient2.dbClient);
		logger.info(`dbClient2 connected - Database: ${dbClient2Database}` +
			`    Host:  ${dbClient2.dbClient.host}    user:  ${dbClient2.dbClient.user}` +
			`    SSL Connection:  ${dbClient2.dbClient.ssl}    Events Table -- Maximum Event id:  ${maxDbClient2EventId}    Row Count:  ${utils.formatNumber(dbClient2RowCount)}`);
		// max event id mismatch error
		if (maxDbClient1EventId !== maxDbClient2EventId) {
			throw new Error(`Maximum eventIds are different.  Client1 Maximum EventId (${dbClient1Database}):  ` +
				`${maxDbClient1EventId}  Client2 Maximum EventId (${dbClient2Database}):  ${maxDbClient2EventId}`);
		}
		// events table row count difference error
		if (dbClient1RowCount !== dbClient2RowCount) {
			throw new Error(`events table row counts are different.  Client1 Row Count (${dbClient1Database}):  ` +
				`${utils.formatNumber(dbClient1RowCount)}  Client2 Row Count (${dbClient2Database}):  ${utils.formatNumber(dbClient2RowCount)}`);
		}
		if (validateTables) {
			const isTable1Valid = yield isEventsTableValid(dbClient1.dbClient);
			// events table 1 is not valid
			if (!isTable1Valid) {
				throw new Error(`events table in ${dbClient1Database} is not valid`);
			}
			const isTable2Valid = yield isEventsTableValid(dbClient2.dbClient);
			// events table 2 is not valid
			if (!isTable2Valid) {
				throw new Error(`events table in ${dbClient2Database} is not valid`);
			}
		}
		var result;
		var thresholdEventId = 0;
		var countEvents = 0;
		var loopCtr = 0;
		// determine when to log progress messages given a maximum of 10 progress messages desired during program execution
		const logCycle = Math.ceil(Math.ceil(maxDbClient1EventId / maxEventsPerRead) / 10);
		// return connections to connection pools
		dbClient1.dbClient.removeListener('error', getListener1);
		dbClient2.dbClient.removeListener('error', getListener2);
		dbUtils.close(dbClient1);
		dbUtils.close(dbClient2);
		logger.info(`Log progress message every ${utils.formatNumber(logCycle * maxEventsPerRead)} events checked`);
		// compare maxEventsPerRead events per iteration
		while (true) {
			result = yield processEvents(events1ConnectionUrl, events2ConnectionUrl, maxEventsPerRead, thresholdEventId, diffIds);
			thresholdEventId = result.thresholdEventId;
			countEvents += result.countEvents;
			if (maxDiffs && diffIds.length >= maxDiffs) {
				logger.error(`Maximum Event differences (${utils.formatNumber(maxDiffs)}) reached.  Program ending.`);
				break;
			}
			// end of events
			if (thresholdEventId === maxDbClient1EventId) {
				break;
			}
			// should never happen
			else if (thresholdEventId > maxDbClient1EventId) {
				throw new Error(`Program Logic error.  eventId (${thresholdEventId}) of last set of events compared is greater than the maximum eventId (${maxDbClient1EventId})`);
			}
			loopCtr++;
			if (loopCtr % logCycle === 0) {
				logger.info(`Events up to event id ${thresholdEventId} have been compared.  Event Differences:  ${utils.formatNumber(diffIds.length)}`);
			}
		}
		logger.info(`${utils.formatNumber(countEvents)} events compared.  Maximum Event Id Compared:  ${thresholdEventId}`);
		// event differences occurred
		if (diffIds.length > 0) {
			logger.error(`Event content differs at the following events table row ids in databases ${dbClient1Database} and ${dbClient2Database}:  ${R.join(',', diffIds)}`);
		}
		// no event differences
		else {
			logger.info(`events table row content is equal in databases ${dbClient1Database} and ${dbClient2Database}`);
		}
		return yield {diffs: diffIds.length};
	}
	finally {
		var elapsed = Date.now() - startDate.getTime();
		if (logger) {
			logger.info('\n### ELAPSED TIME:  ' + elapsed / 1000 + ' ###\n');
		}
	}
});
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// initialize and run events comparison
(function() {
	program
		.option('-c, --config-filename <s>', 'configuration file name')
		.option('-v, --validate-tables', 'optional parameter.  if specified, validate "testingStats" in each events table.  must have created each event column jsonb object with "testingStats property".  see test/loadPersonData.js.')
		.option('--dry-run', 'if specified, display run parameters and end program without starting eventsDiff')
		.parse(process.argv);

	const validateArguments = arguments => {
		var errors = [];
		if (!arguments.configFilename || is.not.string(arguments.configFilename))
			errors = R.append('config-filename is invalid:  ' + JSON.stringify(arguments.configFilename), errors);
		if (!(arguments.dryRun === undefined || arguments.dryRun === true))
			errors = R.append('dry-run is invalid:  ' + JSON.stringify(arguments.dryRun), errors);
		if (arguments.args.length > 0)
			errors = R.append(`Some command arguments exist after processing command options.  There may be command options after " -- " in the command line.  Unprocessed Command Arguments:  ${JSON.stringify(program.args)}`, errors);
		return errors;
	};

	const logConfig = config => {
		logger.info(`Events1 Connection Params:`, R.pick(['host', 'databaseName', 'user'], config.events1Params));
		logger.info(`Events2 Connection Params:`, R.pick(['host', 'databaseName', 'user'], config.events2Params));
		if (config.maxEventsPerRead)
			logger.info(`Maximum Events Per Read:  ${config.maxEventsPerRead}`);
		if (config.maxDiffs)
			logger.info(`Stop program if ${config.maxDiffs} Event Difference(s) is reached`);
		if (config.connectTimeout)
			logger.info(`Database Connection Timeout (millisecs):`, config.connectTimeout);
		if (program.validateTables) {
			logger.info(`"testingStats" in each database events table will be validated`);
		}
		else {
			logger.info(`"testingStats" in each database events table will NOT be validated`);
		}
	};
	/////////////////////////////////////////////////////////////////////////////////////
	//  validate configuration
	/////////////////////////////////////////////////////////////////////////////////////
	const argumentErrors = validateArguments(program);

	if (argumentErrors.length > 0) {
		logger.error(`Invalid command line arguments:${'\n' + R.join('\n', argumentErrors)}`);
		program.help();
		process.exit(2);
	}
	// get absolute name so logs will display absolute path
	const configFilename = path.isAbsolute(program.configFilename) ? program.configFilename : path.resolve('.', program.configFilename);

	let config;
	try {
		logger.info(`${'\n'}Config File Name:  "${configFilename}"${'\n'}`);
		config = require(configFilename);
	}
	catch (err) {
		logger.error({err: err}, `Exception detected processing configuration file:`);
		process.exit(1);
	}

	var configErrors = [];

	configErrors = R.concat(utils.validateConnectionParameters(config.events1Params, 'config.event1Params'), configErrors);
	configErrors = R.concat(utils.validateConnectionParameters(config.events2Params, 'config.event2Params'), configErrors);
	if (configErrors.length === 0) {
		if (config.events1Params.host === config.events2Params.host && config.events1Params.databaseName === config.events2Params.databaseName) {
			configErrors = R.append(`config.events1Params and config.events2Params host and databaseName` +
				` parameters are the same  (${config.events1Params.host}, ${config.events1Params.databaseName})`, configErrors);
		}
	}
	config.maxEventsPerRead = config.maxEventsPerRead || 50000;
	if (utils.isPositiveInteger(config.maxEventsPerRead)) {
		if (config.maxEventsPerRead < 50000) {
			config.maxEventsPerRead = 50000;
			logger.info(`config.maxEventsPerRead set to minimum value of 50000`);
		}
	}
	else {
		configErrors = R.append(`config.maxEventsPerRead (${config.maxEventsPerRead}) is not a positive integer`, configErrors);
	}
	if (config.maxDiffs === 0) {
		configErrors = R.append(`config.maxDiffs (${config.maxDiffs}) is not a positive integer`, configErrors);
	}
	else if (config.maxDiffs) {
		if (!utils.isPositiveInteger(config.maxDiffs)) {
			configErrors = R.append(`config.maxDiffs (${config.maxDiffs}) is not a positive integer`, configErrors);
		}
	}
	if (config.connectTimeout) {
		if (!utils.isPositiveInteger(config.connectTimeout)) {
			configErrors = R.append(`config.connectTimeout is invalid:  ${config.connectTimeout}`, configErrors);
		}
	}
	if (configErrors.length > 0) {
		logger.error(`Invalid configuration parameters:${'\n' + R.join('\n', configErrors)}`);
		program.help();
		process.exit(2);
	}

	logConfig(config);

	if (program.dryRun) {
		logger.info(`--dry-run specified, ending program`);
		process.exit(2);
	}
	/////////////////////////////////////////////////////////////////////////////////////
	//  to reach this point, configuration must be valid and --dry-run was not specified
	/////////////////////////////////////////////////////////////////////////////////////
	eventsDiff(config.events1Params, config.events2Params, logger, config.maxEventsPerRead, config.maxDiffs, config.connectTimeout, program.validateTables)
	.then(result => {
		if (result.diffs > 0) {
			logger.error(`***** Processing Complete with the following number of event differences:  ${result.diffs} *****`);
		}
		else {
			logger.info(`Processing Complete with no event differences`);
		}
		exit(0);
	})
	.catch(err => {
		logger.error({err: err}, `error in eventsDiff`);
		exit(2);
	});
})();
