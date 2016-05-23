const co = require('co');
const faker = require('faker');
const uuid = require('node-uuid');
const bunyan = require('bunyan');
const program = require('commander');
const R = require('ramda');
const is = require('is_js');
const path = require('path');
const dbUtils = require('@panosoft/slate-db-utils');
const utils = require('../lib/utils');

const startDate = new Date();

const logger = bunyan.createLogger({
	name: 'loadPersonData',
	serializers: bunyan.stdSerializers
});

process.on('uncaughtException', err => {
	logger.error({err: err}, `Uncaught exception:`);
	process.exit(1);
});
process.on('unhandledRejection', (reason, p) => {
	logger.error("Unhandled Rejection at: Promise ", p, " reason: ", reason);
	process.exit(1);
});
process.on('SIGINT', () => {
	logger.info(`SIGINT received.`);
	process.exit(0);
});
process.on('SIGTERM', () => {
	logger.info(`SIGTERM received.`);
	process.exit(0);
});
process.on('exit', function () {
	const elapsed = Date.now() - startDate.getTime();
	logger.info('\n### Elapsed: ' + elapsed / 1000 + ' ###\n');
});

program
	.option('-c, --config-filename <s>', 'configuration file name')
	.option('--count-person <n>', 'number of different person event series to create.  Default 1000.', '1000')
	.option('--count-filler <n>', 'number of filler events to create.  Default 10000.', '10000')
	.option('--count-person-delete <n>', 'maximum number of person delete events to create.  Default 50.', '50')
	.option('--dry-run', 'if specified, show run parameters and end without writing any events')
	.parse(process.argv);

const validateArguments = function(arguments) {
	var errors = [];
	if (!arguments.configFilename || is.not.string(arguments.configFilename))
		errors = R.append('config-filename is invalid:  ' + arguments.configFilename, errors);
	if (!utils.isStringPositiveInteger(arguments.countPerson))
		errors = R.append('count-person is not a positive integer:  ' + arguments.countPerson, errors);
	if (!utils.isStringPositiveInteger(arguments.countFiller))
		errors = R.append('count-filler is not a positive integer:  ' + arguments.countFiller, errors);
	if (!utils.isStringPositiveInteger(arguments.countPersonDelete))
		errors = R.append('count-person-delete is not a positive integer:  ' + arguments.countPersonDelete, errors);
	return errors;
};

const logConfig = config => {
	logger.info(`Event Source Connection Params:`, R.pick(['host', 'databaseName', 'user'], config.eventSource));
	if (config.connectTimeout)
		logger.info(`Database Connection Timeout (millisecs):`, config.connectTimeout);
};
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
logger.info('\n### ' + startDate.toISOString() + ' ###\n');

const errors = validateArguments(program);
if (errors.length > 0) {
	logger.error('Invalid command line arguments:\n' + R.join('\n', errors));
	program.help();
	process.exit(1);
}
// get absolute name so logs will display absolute path
const configFilename = path.isAbsolute(program.configFilename) ? program.configFilename : path.resolve('.', program.configFilename);

try {
	logger.info(`${'\n'}Config File Name:  "${configFilename}"${'\n'}`);
	const config = require(configFilename);
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
const numberOfInserts = parseInt(program.countPerson, 10);
// total number of filler events to create
const numberOfFillerEvents = parseInt(program.countFiller, 10);
// maximum number of person delete events to create
const numberPersonDeletes = parseInt(program.countPersonDelete, 10);
// number of person events created before person deleted
var numberPersonEventsToCreateBeforeDelete = Math.ceil(numberOfInserts / numberPersonDeletes);
numberPersonEventsToCreateBeforeDelete = numberPersonEventsToCreateBeforeDelete >= 1 ? numberPersonEventsToCreateBeforeDelete : 1;
// uniqueIds
const initiatorIdList = [uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4()];
const entityIdList = [uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4()];

// number of filler events to create per insert statement
var fillerEventsPerStatement = Math.ceil(numberOfFillerEvents / numberOfInserts);
fillerEventsPerStatement = fillerEventsPerStatement >= 1 ? fillerEventsPerStatement : 1;

logConfig(config);

logger.info('\nNumber of Persons to Create:  ' + numberOfInserts + '    Number of Filler Events to Create:  ' + numberOfFillerEvents +
	'    Maximum number of Persons to Delete:  ' + numberPersonDeletes +
	'\nNumber of Filler Events per Insert:  ' + fillerEventsPerStatement +
	'    Number of Persons to Create Before Person Delete:  ' + numberPersonEventsToCreateBeforeDelete + '\n');

if (program.dryRun) {
	logger.info('dry-run specified, ending program');
	process.exit(0);
}

var personIds = {};
// amount to increment eventTimestamp for every person row
const personEventDiffInMillisecs = parseInt(program.personEventTimestampDiff, 10);
// amount to increment eventTimestamp for every filler row
const fillerEventDiffInMillisecs = parseInt(program.fillerEventTimestampDiff, 10);

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

const addTestingStatsToEvent = (event) => {
	event.testingStats = testingStats;
	event.testingStats.dbOperationEventNum++;
};

const logCounts = function(countPersonEventsCreated, countFillerEventsCreated, countEventsCreated, countPersonCreated, countPersonDeleted) {
	logger.info('Total Events Created:  ' + countEventsCreated + '  Persons Created:  ' + countPersonCreated +
					'  Persons Deleted:  ' + countPersonDeleted +
					'  Person Events Created:  ' + countPersonEventsCreated + '  Filler Events Created:  ' + countFillerEventsCreated + '\n');
};

// return random integer between min (inclusive) and max (exclusive)
const getRandomInt = function(min, max) {
  return Math.floor(Math.random() * (max - min)) + min;
};

const getRandomInitiatorId = function() {
	return initiatorIdList[getRandomInt(0, initiatorIdList.length)];
};

const getRandomEntityId = function() {
	return entityIdList[getRandomInt(0, initiatorIdList.length)];
};

// return personId to delete from list of personIds created
const getPersonToDelete = function(currentPersonCount) {
	const personIdList = Object.keys(personIds);
	const personId = personIdList[getRandomInt(0, personIdList.length)];
	logger.info('Person Deletion Stats:  Current Person Count:  ' + currentPersonCount + '  personId Deleted:  ' + personId + '\n');
	// remove personId to delete from list of personIds created
	delete personIds[personId];
	return personId;
};

// create a set of person events
const createPersonEvents = function(countPersonCreated) {
	var events = [];
	var countPersonDeleted = 0;
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
	if ((countPersonCreated + 1) % numberPersonEventsToCreateBeforeDelete === 0) {
		
		events[events.length] = {
			name: 'PersonDeleted',
			data: {
				id: getPersonToDelete(countPersonCreated + 1)
			},
			metadata: {
				initiatorId: getRandomInitiatorId(),
				command: 'Delete person'
			}
		};
		countPersonDeleted++;
	}
	return {events: events, countPersonDeleted: countPersonDeleted};
};

// create a set of filler events
const createFillerEvents = function(countToCreate) {
	var events = [];
	for (var i = 0; i < countToCreate; i++) {
		events[events.length] = {
			name: 'FillerEvent' + getRandomInt(0, 9),
			data: {
				id: uuid.v4(),
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
	return events;
};

const createPersonEventValues = function(countPersonCreated) {
	var eventValues = [];
	var result = createPersonEvents(countPersonCreated);
	initTestingStatsForDbOperation(result.events.length + fillerEventsPerStatement);
	result.events.forEach(function(event) {
		addTestingStatsToEvent(event);
		// id[idx] represent the parameter value for the id column where idx is a 1-based index starting at 1
		eventValues[eventValues.length] = `($1[${eventValues.length + 1}], $2, '${getRandomEntityId()}', ` +
											`'${JSON.stringify(event).replace(/'/g, '\'\'')}')`;
	});
	return {events: eventValues, countPersonDeleted: result.countPersonDeleted};
};

const createFillerEventValues = function(idx) {
	var eventValues = [];
	createFillerEvents(fillerEventsPerStatement).forEach(function(event) {
		addTestingStatsToEvent(event);
		// id[idx] represent the parameter value for the id column where idx is a 1-based
		eventValues[eventValues.length] = `($1[${idx}], $2, '${getRandomEntityId()}', ` +
											`'${JSON.stringify(event).replace(/'/g, '\'\'')}')`;
		idx++;
	});
	return eventValues;
};
const logDbStatus = co.wrap(function *(dbClient) {
	const rowCount = yield utils.getEventCount(dbClient);
	// get maximum event source event id
	const maxEventId = yield utils.getMaximumEventId(dbClient);
	logger.info(`Event Source Client status - Database: ${dbClient.database}` +
		`    Host:  ${dbClient.host}    user:  ${dbClient.user}` + `    SSL Connection:  ${dbClient.ssl}` +
		`    Row Count:  ${utils.formatNumber(rowCount)}    Maximum Event Id:  ${maxEventId}`);
});

const createEvents = (totalPersonCreated) => {
	const createPersonResult = createPersonEventValues(totalPersonCreated);
	var personEvents = createPersonResult.events;
	// pass the idx to be used for the first filler event which is 1 + the last idx used for the person events
	const fillerEvents = createFillerEventValues(createPersonResult.events.length + 1);
	return {events: personEvents.concat(fillerEvents), countPersonCreated: personEvents.length, countPersonDeleted: createPersonResult.countPersonDeleted, countFiller: fillerEvents.length};
};

const createAndInsertEvents = co.wrap(function *(dbClient) {
	var totalPersonEventsCreated = 0;
	var totalFillerEventsCreated = 0;
	var totalEventsCreated = 0;
	var totalPersonCreated = 0;
	var totalPersonDeleted = 0;
	var totalInsertStatementsCreated = 0;
	while (totalInsertStatementsCreated < numberOfInserts) {
		var createdEvents = createEvents(totalPersonCreated);
		var insertStatement = `SELECT insert_events(ARRAY[$$${createdEvents.events.join(', ')}$$])`;
		var result = yield dbUtils.executeSQLStatement(dbClient, insertStatement);
		if (result.rowCount === 1) {
			var row1 = result.rows[0];
			if (!(row1['insert_events'] && row1['insert_events'] === createdEvents.events.length)) {
				throw new Error(`Program logic error.  Event count doesn't match rows inserted.  Event Count:  ${createdEvents.events.length}  Rows Inserted:  ${row1['insert_events']}`);
			}
		}
		else {
			throw new Error(`Program logic error.  Expected result array of one object to be returned.  Result:  ${result}`);
		}
		totalPersonEventsCreated += createdEvents.countPersonCreated;
		totalFillerEventsCreated += createdEvents.countFiller;
		totalEventsCreated += createdEvents.events.length;
		totalPersonCreated++;
		totalPersonDeleted += createdEvents.countPersonDeleted;
		totalInsertStatementsCreated++;
	}
	logCounts(totalPersonEventsCreated, totalFillerEventsCreated, totalEventsCreated, totalPersonCreated, totalPersonDeleted);
});

const main = co.wrap(function *(connectionUrl, connectTimeout) {
	try {
		dbUtils.setDefaultOptions({logger: logger, connectTimeout: connectTimeout});
		const pooledDbClient = yield dbUtils.createPooledClient(connectionUrl);
		const dbClientDatabase = pooledDbClient.dbClient.database;
		const getListener = function(err) {
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
	})
	.catch(err => {
		logger.error({err: err}, `Exception in loadPersonEvents:`);
		process.exit(1);
	});
