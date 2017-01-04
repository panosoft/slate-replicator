	const co = require('co');
	const faker = require('faker');
	const uuid = require('node-uuid');
	const bunyan = require('bunyan');
	const bformat = require('bunyan-format');
	const program = require('commander');
	const R = require('ramda');
	const is = require('is_js');
	const path = require('path');
	const dbUtils = require('@panosoft/slate-db-utils');
	const utils = require('../lib/utils');

	const startDate = new Date();

	const formatOut = bformat({ outputMode: 'long' });

	const logger = bunyan.createLogger({
		name: 'loadPerfTestData',
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

	program
		.option('-c, --config-filename <s>', 'configuration file name')
		.option('--filler-events-between <n>', 'number of Filler events between each User event.  Default 10000.', '10000')
		.option('--max-events-per-insert <n>', 'maximum number of events per SQL insert.  Default 25.', '25')
		.option('--dry-run', 'if specified, show run parameters and end without writing any events')
		.parse(process.argv);

	const validateArguments = arguments => {
		var errors = [];
		if (!arguments.configFilename || is.not.string(arguments.configFilename))
			errors = R.append('config-filename is invalid:  ' + JSON.stringify(arguments.configFilename), errors);
		if (!utils.isStringPositiveInteger(arguments.fillerEventsBetween))
			errors = R.append('filler-events-between is not a positive integer:  ' + JSON.stringify(arguments.fillerEventsBetween), errors);
		if (!utils.isStringPositiveInteger(arguments.maxEventsPerInsert))
			errors = R.append('max-events-per-insert is not a positive integer:  ' + JSON.stringify(arguments.maxEventsPerInsert), errors);
		if (!(arguments.dryRun === undefined || arguments.dryRun === true))
			errors = R.append('dry-run is invalid:  ' + JSON.stringify(arguments.dryRun), errors);
		if (arguments.args.length > 0)
			errors = R.append(`Some command arguments exist after processing command options.  There may be command options after " -- " in the command line.  Unprocessed Command Arguments:  ${program.args}`, errors);
		return errors;
	};

	const logConfig = config => {
		logger.info(`Event Source Connection Params:`, R.pick(['host', 'databaseName', 'user'], config.eventSource));
		if (config.connectTimeout)
			logger.info(`Database Connection Timeout (millisecs):`, config.connectTimeout);
	};
	/////////////////////////////////////////////////////////////////////////////////////
	//  validate configuration
	/////////////////////////////////////////////////////////////////////////////////////
	logger.info('\n### ' + startDate.toISOString() + ' ###\n');

	const errors = validateArguments(program);
	if (errors.length > 0) {
		logger.error('Invalid command line arguments:\n' + R.join('\n', errors));
		program.help();
		process.exit(1);
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

	var configErrors = utils.validateConnectionParameters(config.eventSource, 'config.eventsSource');
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

	// db connection url
	const connectionUrl = dbUtils.createConnectionUrl(config.eventSource);
	// number of filler events to create between each user event
	const fillerEventsBetween = Number(program.fillerEventsBetween);
	// max events per SQL insert statement
	const maxEventsPerInsert = Number(program.maxEventsPerInsert);

	const fillerEventsBetweenDivisor = fillerEventsBetween + 1;

	const userEventConfig = {
		createdEventCommand: {event: 'User Created', command: 'Create User'},
		eventCommands: [
			{event: 'User Phone Changed', command: 'Change User Phone'},
			{event: 'User Name Added', command: 'Add User Name'},
			{event: 'User Phone Added', command:  'Add User Phone'}
		]
	};

	const fillerEventConfig = {
		createdEventCommand: {event: 'Filler Created', command: 'Create Filler'},
		updatedEventCommand: {event: 'Filler Changed', command: 'Change Filler'}
	};

	const countUsers = 3;
	const countUserEventTypes = userEventConfig.eventCommands.length + 1;
	const countUserEventsToCreate = countUsers * countUserEventTypes;

	const minListValue =  integers => {
		return R.reduce(R.min, Infinity, integers)
	};

	const matchMinCountValue = entities => {
		const minCount = minListValue(R.map(R.prop('count'), entities));
		const eqMinCount = (o) => R.prop('count', o) === minCount || minCount === Infinity;
		return minCount === Infinity ? entities : R.filter(eqMinCount, entities);
	};

	const createEntity = entityId => ({entityId: entityId, count: 0});

	const createEntityIds = count => R.map(() => uuid.v4(), new Array(count));

	const initiatorIdList = createEntityIds(30);
	const userEntityIdList = R.map(createEntity, createEntityIds(countUsers));
	const fillerEntityIdList = R.map(createEntity, createEntityIds(1000));


	// return random integer between min (inclusive) and max (exclusive)
	const getRandomInt = (min, max) => {
		return Math.floor(Math.random() * (max - min)) + min;
	};

	const getRandomInitiatorId = () => {
		return initiatorIdList[getRandomInt(0, initiatorIdList.length)];
	};

	const addEventValues = event => {
		if (event.name === 'User Name Added') {
			event.data.value = {first: faker.name.firstName(), middle: '', last: faker.name.lastName()};
		}
		else if (event.name === 'User Phone Added' || event.name === 'User Phone Changed') {
			event.data.value = {phone: faker.phone.phoneNumber()};

		}
		else if (event.name === 'Filler Changed') {
			event.data.value = {date: faker.date.past()};
		}
		return event;
	};

	const getNextUserEvent = () => {
		const candidates = matchMinCountValue(userEntityIdList);
		const candidate = candidates [getRandomInt(0, candidates.length)];
		const nextEventCommand = candidate.count === 0 ?
			userEventConfig.createdEventCommand : userEventConfig.eventCommands[candidate.count % userEventConfig.eventCommands.length];
		candidate.count += 1;
		return addEventValues({name: nextEventCommand.event,
			data: {entityId: candidate.entityId},
			metadata: {command: nextEventCommand.command, initiatorId: getRandomInitiatorId()}});
	};

	const getNextFillerEvent = () => {
		const candidates = matchMinCountValue(fillerEntityIdList);
		const candidate = candidates [getRandomInt(0, candidates.length)];
		const nextEventCommand = candidate.count === 0 ? fillerEventConfig.createdEventCommand : fillerEventConfig.updatedEventCommand;
		candidate.count += 1;
		return addEventValues({name: nextEventCommand.event,
			data: {entityId: candidate.entityId},
			metadata: {command: nextEventCommand.command, initiatorId: getRandomInitiatorId()}});
	};



	logConfig(config);

	logger.info('\nNumber of Filler Events between each User Event:  ' + fillerEventsBetween +
		'\nMaximum Number of Events per SQL Insert:  ' + maxEventsPerInsert +
		'\nEstimated Number of Events that will be created:  ' + utils.formatNumber((countUserEventsToCreate - 1) * fillerEventsBetween + (countUserEventsToCreate + 1)) + '\n');
	
	if (program.dryRun) {
		logger.info('dry-run specified, ending program');
		process.exit(0);
	}
	/////////////////////////////////////////////////////////////////////////////////////
	//  to reach this point, configuration must be valid and --dry-run was not specified
	/////////////////////////////////////////////////////////////////////////////////////

	const testingStats = {
		programId: uuid.v4(),
		dbOperationNum: 0,
		dbOperationId: uuid.v4(),
		dbOperationEventNum: 0,
		dbOperationEventCount: 0,
		dbOperationTS: new Date().toISOString()
	};

	const initTestingStatsForDbOperation = (eventCount) => {
		testingStats.dbOperationNum++;
		testingStats.dbOperationId = uuid.v4();
		testingStats.dbOperationEventNum = 0;
		testingStats.dbOperationEventCount = eventCount;
		testingStats.dbOperationTS = new Date().toISOString();
	};

	const addTestingStatsToEvent = (event, idx) => {
		event.testingStats = R.clone(testingStats);
		event.testingStats.dbOperationEventNum = idx;
	};

	const addTestingStats = (events) => {
		initTestingStatsForDbOperation(events.length);
		var idx = 1;
		events = R.forEach((event) => {
			addTestingStatsToEvent(event, idx);
			idx++;
			return event;
		}, events);
		return events;
	};

	const logCounts = (countEventsCreated) => {
		logger.info('Total Events Created:  ' + countEventsCreated + '\n');
	};

	// processing is complete when all possible events for all user entities have be been written
	const processingComplete = () => {
		const usersComplete = R.filter(userEntityId => userEntityId.count === countUserEventTypes, userEntityIdList);
		return usersComplete.length === userEntityIdList.length;
	};

	const progressMessage = (eventsCreated) => {
		if (eventsCreated % 500000 === 0) {
			logger.info(`${utils.formatNumber(eventsCreated)} Events created`);
		}
	};

	// create a set of events
	const createEvents = (countToCreate, totalEventsCreated) => {
		var events = [];
		var done = false;
		while (events.length < countToCreate && !done) {
			if ((events.length + totalEventsCreated) % fillerEventsBetweenDivisor === 0) {
				events[events.length] = getNextUserEvent();
				done = processingComplete();
				logger.info(`User Event created at event number ${utils.formatNumber(events.length + totalEventsCreated)}`);
			}
			progressMessage(events.length + totalEventsCreated);
			events[events.length] = getNextFillerEvent();
		}
		return {events: events, done: done};
	};

	const createAndInsertEvents = co.wrap(function *(dbClient) {
		var totalEventsCreated = 0;
		var totalInsertStatementsCreated = 0;
		var errorMessage = '';
		var done = false;
		while (!done) {
			var eventsResult = createEvents(maxEventsPerInsert, totalEventsCreated);
			// var events = addTestingStats(R.concat(personEvents.events, fillerEvents.events));
			var insertStatement = utils.createInsertEventsSQLStatement(eventsResult.events);
			done = eventsResult.done;
			var countEventsCreated = eventsResult.events.length;
			var result = yield dbUtils.executeSQLStatement(dbClient, insertStatement);
			if (result.rowCount === 1) {
				var row1 = result.rows[0];
				if (!(row1['insert_events'] && row1['insert_events'] === countEventsCreated)) {
					errorMessage = `Program logic error.  Event count doesn't match rows inserted.  Event Count:  ${countEventsCreated}  Rows Inserted:  ${row1['insert_events']}`;
					logger.error(`${errorMessage}  SQL Statement:  ${insertStatement.substr(0, 4000)}...`);
					throw new Error(errorMessage);
				}
			}
			else {
				errorMessage = `Program logic error.  Expected result array of one object to be returned.  Result:  ${JSON.stringify(result)}`;
				logger.error(`${errorMessage}  SQL Statement:  ${insertStatement.substr(0, 4000)}...`);
				throw new Error(errorMessage);
			}
			totalEventsCreated += countEventsCreated;
			totalInsertStatementsCreated++;
		}
		logCounts(totalEventsCreated);
	});

	const logDbStatus = co.wrap(function *(dbClient) {
		const rowCount = yield utils.getEventCount(dbClient);
		// get maximum event source event id
		const maxEventId = yield utils.getMaximumEventId(dbClient);
		logger.info(`Event Source Client status - Database: ${dbClient.database}` +
			`    Host:  ${dbClient.host}    user:  ${dbClient.user}` + `    SSL Connection:  ${dbClient.ssl}` +
			`    Row Count:  ${utils.formatNumber(rowCount)}    Maximum Event Id:  ${maxEventId}`);
	});

	const main = co.wrap(function *(connectionUrl, connectTimeout) {
		let pooledDbClient;
		let getListener;
		try {
			dbUtils.setDefaultOptions({logger: logger, connectTimeout: connectTimeout});
			pooledDbClient = yield dbUtils.createPooledClient(connectionUrl);
			const dbClientDatabase = pooledDbClient.dbClient.database;
			getListener = err => {
				logger.error({err: err}, `Error for database ${dbClientDatabase}`);
				throw err;
			};
			pooledDbClient.dbClient.on('error', getListener);
			yield logDbStatus(pooledDbClient.dbClient);
			yield createAndInsertEvents(pooledDbClient.dbClient);
			yield logDbStatus(pooledDbClient.dbClient);
		}
		finally {
			// must remove events listener due to the way the connection pool works
			if (pooledDbClient && getListener) {
				pooledDbClient.dbClient.removeListener('error', getListener);
			}
			if (pooledDbClient) {
				dbUtils.close(pooledDbClient);
			}
		}
	});

	main(connectionUrl, config.connectTimeout)
		.then(() =>  {
			const elapsed = Date.now() - startDate.getTime();
			logger.info('Processing complete\n### ELAPSED TIME:  ' + (elapsed / 1000) + ' SECs ###\n');
			exit(0);
		})
		.catch(err => {
			logger.error({err: err}, `Exception in loadPersonEvents:`);
			exit(1);
		});
