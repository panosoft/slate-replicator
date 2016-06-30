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
	name: 'loadPersonData',
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
	.option('--count-person <n>', 'number of different person event series to create.  Default 1000.', '1000')
	.option('--count-filler <n>', 'number of filler events to create.  Default 10000.', '10000')
	.option('--count-person-delete <n>', 'maximum number of person delete events to create.  Default 50.', '50')
	.option('--dry-run', 'if specified, show run parameters and end without writing any events')
	.parse(process.argv);

const validateArguments = arguments => {
	var errors = [];
	if (!arguments.configFilename || is.not.string(arguments.configFilename))
		errors = R.append('config-filename is invalid:  ' + JSON.stringify(arguments.configFilename), errors);
	if (!utils.isStringPositiveInteger(arguments.countPerson))
		errors = R.append('count-person is not a positive integer:  ' + JSON.stringify(arguments.countPerson), errors);
	if (!utils.isStringPositiveInteger(arguments.countFiller))
		errors = R.append('count-filler is not a positive integer:  ' + JSON.stringify(arguments.countFiller), errors);
	if (!utils.isStringPositiveInteger(arguments.countPersonDelete))
		errors = R.append('count-person-delete is not a positive integer:  ' + JSON.stringify(arguments.countPersonDelete), errors);
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
// total number of insert statements to create
const numberOfPersonsToCreate = Number(program.countPerson);
// total number of filler events to create
const numberOfFillerEvents = Number(program.countFiller);
// maximum number of person delete events to create
const numberPersonDeletes = Number(program.countPersonDelete);
// number of person events created before person deleted
var numberPersonEventsToCreateBeforeDelete = Math.ceil(numberOfPersonsToCreate / numberPersonDeletes);
numberPersonEventsToCreateBeforeDelete = numberPersonEventsToCreateBeforeDelete >= 1 ? numberPersonEventsToCreateBeforeDelete : 1;
// uniqueIds
const initiatorIdList = [uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4()];
const entityIdList = [uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4()];

// number of filler events to create per insert statement
var fillerEventsPerStatement = Math.ceil(numberOfFillerEvents / numberOfPersonsToCreate);
fillerEventsPerStatement = fillerEventsPerStatement >= 1 ? fillerEventsPerStatement : 1;

logConfig(config);

logger.info('\nNumber of Persons to Create:  ' + numberOfPersonsToCreate + '    Number of Filler Events to Create:  ' + numberOfFillerEvents +
	'    Maximum number of Persons to Delete:  ' + numberPersonDeletes +
	'\nNumber of Filler Events per Insert:  ' + fillerEventsPerStatement +
	'    Number of Persons to Create Before Person Delete:  ' + numberPersonEventsToCreateBeforeDelete + '\n');

if (program.dryRun) {
	logger.info('dry-run specified, ending program');
	process.exit(0);
}
/////////////////////////////////////////////////////////////////////////////////////
//  to reach this point, configuration must be valid and --dry-run was not specified
/////////////////////////////////////////////////////////////////////////////////////
var personIds = {};

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

const logCounts = (countPersonEventsCreated, countFillerEventsCreated, countEventsCreated, countPersonCreated, countPersonDeleted) => {
	logger.info('Total Events Created:  ' + countEventsCreated + '  Persons Created:  ' + countPersonCreated +
					'  Persons Deleted:  ' + countPersonDeleted +
					'  Person Events Created:  ' + countPersonEventsCreated + '  Filler Events Created:  ' + countFillerEventsCreated + '\n');
};

// return random integer between min (inclusive) and max (exclusive)
const getRandomInt = (min, max) => {
  return Math.floor(Math.random() * (max - min)) + min;
};

const getRandomInitiatorId = () => {
	return initiatorIdList[getRandomInt(0, initiatorIdList.length)];
};

const getRandomEntityId = () => {
	return entityIdList[getRandomInt(0, entityIdList.length)];
};

// return personId to delete from list of personIds created
const getPersonToDelete = currentPersonCount => {
	const personIdList = Object.keys(personIds);
	const personId = personIdList[getRandomInt(0, personIdList.length)];
	logger.info('Person Deletion Stats:  Current Person Count:  ' + currentPersonCount + '  personId Deleted:  ' + personId + '\n');
	// remove personId to delete from list of personIds created
	delete personIds[personId];
	return personId;
};

// create a set of person events
const createPersonEvents = totalPersonCreated => {
	var events = [];
	var countPersonDeletedEvents = 0;
	const personId = uuid.v4();
	personIds[personId] = true;
	events[events.length] = {
		name: 'PersonCreated',
		data: {
			id: personId
		},
		metadata: {
			initiatorId: getRandomInitiatorId(),
			command: 'Create person'
		}
	};
	events[events.length] = {
		name: 'PersonNameAdded',
		data: {
			id: personId,
			lastName: faker.name.lastName(),
			firstName: faker.name.firstName(),
			mi: ''
		},
		metadata: {
			initiatorId: getRandomInitiatorId(),
			command: 'Add person name'
		}
	};
	events[events.length] = {
		name: 'PersonSSNAdded',
		data: {
			id: personId,
			ssn : uuid.v4()
		},
		metadata: {
			initiatorId: getRandomInitiatorId(),
			command: 'Add person ssn'
		}
	};
	if ((totalPersonCreated + 1) % numberPersonEventsToCreateBeforeDelete === 0) {
		
		events[events.length] = {
			name: 'PersonDeleted',
			data: {
				id: getPersonToDelete(totalPersonCreated + 1)
			},
			metadata: {
				initiatorId: getRandomInitiatorId(),
				command: 'Delete person'
			}
		};
		countPersonDeletedEvents++;
	}
	return {events: events, countPersonDeletedEvents: countPersonDeletedEvents};
};

// create a set of filler events
const createFillerEvents = countToCreate =>{
	var events = [];
	for (var i = 0; i < countToCreate; i++) {
		events[events.length] = {
			name: 'FillerEvent' + getRandomInt(0, 9),
			data: {
				id: getRandomEntityId(),
				category: 'Category' + getRandomInt(0, 4),
				a: getRandomInt(1, 2000),
				b: getRandomInt(100, 200)
			},
			metadata: {
				initiatorId: getRandomInitiatorId(),
				command: 'Add filler event'
			}
		};
	}
	return {events: events};
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

const createAndInsertEvents = co.wrap(function *(dbClient) {
	var totalPersonEventsCreated = 0;
	var totalFillerEventsCreated = 0;
	var totalEventsCreated = 0;
	var totalPersonCreated = 0;
	var totalPersonDeleted = 0;
	var totalInsertStatementsCreated = 0;
	var errorMessage = '';
	while (totalPersonCreated < numberOfPersonsToCreate) {
		var personEvents = createPersonEvents(totalPersonCreated);
		var fillerEvents = createFillerEvents(fillerEventsPerStatement);
		var events = addTestingStats(R.concat(personEvents.events, fillerEvents.events));
		var insertStatement = utils.createInsertEventsSQLStatement(events);
		var countEventsCreated = personEvents.events.length + fillerEvents.events.length;
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
		totalPersonEventsCreated += personEvents.events.length;
		totalFillerEventsCreated += fillerEvents.events.length;
		totalEventsCreated += countEventsCreated;
		totalPersonCreated++;
		totalPersonDeleted += personEvents.countPersonDeletedEvents;
		totalInsertStatementsCreated++;
	}
	logCounts(totalPersonEventsCreated, totalFillerEventsCreated, totalEventsCreated, totalPersonCreated, totalPersonDeleted);
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
		logger.info(`Processing complete`);
		exit(0);
	})
	.catch(err => {
		logger.error({err: err}, `Exception in loadPersonEvents:`);
		exit(1);
	});
