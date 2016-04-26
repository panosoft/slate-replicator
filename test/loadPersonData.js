const co = require('co');
var faker = require('faker');
var uuid = require('node-uuid');
var bunyan = require('bunyan');
var program = require('commander');
var R = require('ramda');
var is = require('is_js');
var path = require('path');
var dbUtils = require('@panosoft/slate-db-utils');
var utils = require('../lib/utils');

var logger = bunyan.createLogger({
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
	var elapsed = Date.now() - startDate.getTime();
	logger.info('\n### Elapsed: ' + elapsed / 1000 + ' ###\n');
});

program
	.option('-c, --config-filename <s>', 'configuration file name')
	.option('--count-person <n>', 'number of different person event series to create.  Default 1000.', '1000')
	.option('--count-filler <n>', 'number of filler events to create.  Default 10000.', '10000')
	.option('--count-person-delete <n>', 'maximum number of person delete events to create.  Default 50.', '50')
	.option('--event-start-date <datetime>', 'starting timestamp for created events.  Default 2006-02-08T17:48:59.158Z.', '2006-02-08T17:48:59.158Z')
	.option('--person-event-timestamp-diff <n>', 'timestamp increase between each person event in millisec.  Default: 1803321', '1803321')
	.option('--filler-event-timestamp-diff <n>', 'timestamp increase between each filler event in millisec.  Default: 60996', '60996')
	.option('--dry-run', 'if specified, show run parameters and end without writing any events')
	.parse(process.argv);

var validateArguments = function(arguments) {
	var errors = [];
	if (!arguments.configFilename || is.not.string(arguments.configFilename))
		errors = R.append('config-filename is invalid:  ' + arguments.configFilename, errors);
	if (!utils.isStringPositiveInteger(arguments.countPerson))
		errors = R.append('count-person is not a positive integer:  ' + arguments.countPerson, errors);
	if (!utils.isStringPositiveInteger(arguments.countFiller))
		errors = R.append('count-filler is not a positive integer:  ' + arguments.countFiller, errors);
	if (!utils.isStringPositiveInteger(arguments.countPersonDelete))
		errors = R.append('count-person-delete is not a positive integer:  ' + arguments.countPersonDelete, errors);
	if (!utils.isStringPositiveInteger(arguments.personEventTimestampDiff))
		errors = R.append('person-event-timestamp-diff is not a positive integer:  ' + arguments.personEventTimestampDiff, errors);
	if (!utils.isStringPositiveInteger(arguments.fillerEventTimestampDiff))
		errors = R.append('filler-event-timestamp-diff is not a positive integer:  ' + arguments.fillerEventTimestampDiff, errors);
	if (isNaN(new Date(arguments.eventStartDate)))
		errors = R.append('event-start-date is not a valid date', errors);
	return errors;
};

const logConfig = config => {
	logger.info(`Event Source Connection Params:`, R.pick(['host', 'databaseName', 'user'], config.eventSource));
	if (config.connectTimeout)
		logger.info(`Database Connection Timeout (millisecs):`, config.connectTimeout);
};
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var startDate = new Date();
logger.info('\n### ' + startDate + ' ###\n');

var errors = validateArguments(program);
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

// number of filler events to create per insert statement
var fillerEventsPerStatement = Math.ceil(numberOfFillerEvents / numberOfInserts);
fillerEventsPerStatement = fillerEventsPerStatement >= 1 ? fillerEventsPerStatement : 1;
// Event timestamp is increased for every event created
var eventTimestamp = new Date(program.eventStartDate);
// most recent Event timestamp of all events created
var maxEventTimestamp = null;

logConfig(config);

logger.info('\nNumber of Persons to Create:  ' + numberOfInserts + '    Number of Filler Events to Create:  ' + numberOfFillerEvents +
	'    Maximum number of Persons to Delete:  ' + numberPersonDeletes +
	'\nNumber of Filler Events per Insert:  ' + fillerEventsPerStatement +
	'    Number of Persons to Create Before Person Delete:  ' + numberPersonEventsToCreateBeforeDelete +
	'\nEvent Start Date:  ' + eventTimestamp.toISOString() + '\n');

if (program.dryRun) {
	logger.info('dry-run specified, ending program');
	process.exit(0);
}

var personIds = {};
// amount to increment eventTimestamp for every person row
var personEventDiffInMillisecs = parseInt(program.personEventTimestampDiff, 10);
// amount to increment eventTimestamp for every filler row
var fillerEventDiffInMillisecs = parseInt(program.fillerEventTimestampDiff, 10);

var logCounts = function(countPersonEventsCreated, countFillerEventsCreated, countEventsCreated, countPersonCreated, countPersonDeleted, maxEventTimestamp) {
	logger.info('Total Events Created:  ' + countEventsCreated + '  Persons Created:  ' + countPersonCreated +
					'  Persons Deleted:  ' + countPersonDeleted +
					'  Person Events Created:  ' + countPersonEventsCreated + '  Filler Events Created:  ' + countFillerEventsCreated +
					'\nMost recent Event Date for Created Events:  ' + maxEventTimestamp.toISOString() + '\n');
};

// return random integer between min (inclusive) and max (exclusive)
function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min)) + min;
}

function getRandomInitiatorId() {
	return initiatorIdList[getRandomInt(0, initiatorIdList.length)];
}

// return personId to delete from list of personIds created
var getPersonToDelete = function(currentPersonCount) {
	var personIdList = Object.keys(personIds);
	var personId = personIdList[getRandomInt(0, personIdList.length)];
	logger.info('Person Deletion Stats:  Current Person Count:  ' + currentPersonCount + '  personId Deleted:  ' + personId + '\n');
	// remove personId to delete from list of personIds created
	delete personIds[personId];
	return personId;
};

// create a set of person events
var createPersonEvents = function(countPersonCreated) {
	var events = [];
	var countPersonDeleted = 0;
	var personId = uuid.v4();
	personIds[personId] = true;
	events[events.length] = {
		name: 'PersonCreated',
		initiatorId: getRandomInitiatorId(),
		data: {
			personId: personId
		}
	};
	events[events.length] = {
		name: 'PersonNameAdded',
		initiatorId: getRandomInitiatorId(),
		data: {
			personId: personId,
			lastName: faker.name.lastName(),
			firstName: faker.name.firstName(),
			mi: ''
		}
	};
	events[events.length] = {
		name: 'PersonSSNAdded',
		initiatorId: getRandomInitiatorId(),
		data: {
			personId: personId,
			ssn : uuid.v4()
		}
	};
	if ((countPersonCreated + 1) % numberPersonEventsToCreateBeforeDelete === 0) {
		
		events[events.length] = {
			name: 'PersonDeleted',
			initiatorId: getRandomInitiatorId(),
			data: {
				personId: getPersonToDelete(countPersonCreated + 1)
			}
		};
		countPersonDeleted++;
	}
	return {events: events, countPersonDeleted: countPersonDeleted};
};

// create a set of filler events
var createFillerEvents = function(countToCreate) {
	var events = [];
	for (var i = 0; i < countToCreate; i++) {
		events[events.length] = {
			name: 'FillerEvent' + getRandomInt(0, 9),
			initiatorId: getRandomInitiatorId(),
			data: {
				guidId: uuid.v4,
				category: 'Category' + getRandomInt(0, 4),
				a: getRandomInt(1, 2000),
				b: getRandomInt(100, 200)
			}
		};
	}
	return events;
};

var createPersonEventValues = function(countPersonCreated) {
	var eventValues = [];
	var result = createPersonEvents(countPersonCreated);
	result.events.forEach(function(event) {
		eventValues[eventValues.length] = '(\'' + eventTimestamp.toISOString() + '\',\'' + JSON.stringify(event).replace(/'/g, '\'\'') + '\')';
		eventTimestamp = new Date(eventTimestamp.valueOf() + (personEventDiffInMillisecs));
	});
	return {events: eventValues, countPersonDeleted: result.countPersonDeleted};
};

var createFillerEventValues = function() {
	var eventValues = [];
	createFillerEvents(fillerEventsPerStatement).forEach(function(event) {
		eventValues[eventValues.length] = '(\'' + eventTimestamp.toISOString() + '\',\'' + JSON.stringify(event).replace(/'/g, '\'\'') + '\')';
		maxEventTimestamp = eventTimestamp;
		eventTimestamp = new Date(eventTimestamp.valueOf() + (fillerEventDiffInMillisecs));
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
	var createPersonResult = createPersonEventValues(totalPersonCreated);
	var personEvents = createPersonResult.events;
	var fillerEvents = createFillerEventValues();
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
		var insertStatement = 'INSERT INTO events (eventTimestamp, event) VALUES ' + createdEvents.events.join(', ') + ';';
		var result = yield dbUtils.executeSQLStatement(dbClient, insertStatement);
		if (createdEvents.events.length !== result.rowCount) {
			throw new Error('Program logic error  Event Count:', createdEvents.events.length, 'Rows Inserted:', result.rowCount);
		}
		totalPersonEventsCreated += createdEvents.countPersonCreated;
		totalFillerEventsCreated += createdEvents.countFiller;
		totalEventsCreated += createdEvents.events.length;
		totalPersonCreated++;
		totalPersonDeleted += createdEvents.countPersonDeleted;
		totalInsertStatementsCreated++;
	}
	logCounts(totalPersonEventsCreated, totalFillerEventsCreated, totalEventsCreated, totalPersonCreated, totalPersonDeleted, maxEventTimestamp);
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
