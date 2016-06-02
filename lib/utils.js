const is = require('is_js');
const co = require('co');
const coread = require('co-read');
const R = require('ramda');
const dbUtils = require('@panosoft/slate-db-utils');

const utils = {
	formatNumber: n => n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ','),
	isStringPositiveInteger: s => utils.isPositiveInteger(parseInt(s, 10)),
	isPositiveInteger: n => is.integer(n) && is.positive(n),
	parseInteger: s => {
		const n = parseInt(s, 10);
		if (isNaN(n)) {
			throw new Error(`${JSON.stringify(s)} is not an integer`);
		}
		return n;
	},
	getEventsFromStream: co.wrap(function *(eventStream, maxEvents) {
		var events = [];
		var endOfStream = false;
		while (events.length < maxEvents && !endOfStream) {
			var event = yield coread(eventStream);
			// event returned
			if (event) {
				events[events.length] = event;
			}
			// end of stream
			else {
				endOfStream = true;
			}
		}
		return {events: events, endOfStream: endOfStream};
	}),
	validateConnectionParameters: (parameters, parametersName) => {
		var errors = [];
		if (parameters) {
			if (!parameters.host || is.not.string(parameters.host)) {
				errors = R.append(`${parametersName}.host is missing or invalid:  ${parameters.host}`, errors);
			}
			if (!parameters.databaseName || is.not.string(parameters.databaseName)) {
				errors = R.append(`${parametersName}.databaseName is missing or invalid:  ${parameters.databaseName}`, errors);
			}
			if (parameters.userName && is.not.string(parameters.userName)) {
				errors = R.append(`${parametersName}.userName is invalid:  ${parameters.userName}`, errors);
			}
			if (parameters.password && is.not.string(parameters.password)) {
				errors = R.append(`${parametersName}.password is invalid:  ${parameters.password}`, errors);
			}
		}
		else {
			errors = R.append(`connection parameters for ${parametersName} are missing or invalid`, errors);
		}
		return errors;
	},
	getMaximumEventId: co.wrap(function *(dbClient) {
		// it is assumed that the minimum value for the id column is 1
		const result = yield dbUtils.executeSQLStatement(dbClient, 'SELECT max(id) AS "maxId" FROM events');
		// no rows in table
		if (result.rowCount === 0) {
			return 0;
		}
		// maximum id found
		else if (result.rowCount === 1) {
			var maxId = result.rows[0].maxId;
			if (maxId === null) {
				return 0;
			}
			else {
				maxId = utils.parseInteger(maxId);
			}
			// maximum id is valid
			if (maxId > 0) {
				return maxId;
			}
			// maximum id is invalid
			else {
				throw new Error(`Maximum events.id (${maxId}) is invalid for database ${dbClient.database}`)
			}
		}
		// row count returned from select is invalid
		else {
			throw new Error(`Row count (${result.rowCount}) returned for SELECT statement is invalid for database ${dbClient.database}`);
		}
	}),
	getEventCount: co.wrap(function *(dbClient) {
		const result = yield dbUtils.executeSQLStatement(dbClient, 'SELECT count(*) AS "count" FROM events');
		// no rows in table
		if (result.rowCount === 0) {
			return 0;
		}
		else if (result.rowCount === 1) {
			var count = result.rows[0].count;
			if (count === null) {
				return 0;
			}
			else {
				return utils.parseInteger(count);
			}
		}
		// row count returned from select is invalid
		else {
			throw new Error(`Row count (${result.rowCount}) returned for SELECT statement is invalid for database ${dbClient.database}`);
		}
	}),
	createInsertEventsSQLStatement: events => {
		const eventsToInsertValues = event => {
			// $1[idx] represents the parameter value for the id column where idx is a 1-based index starting at 1 needed by the insert_events SQL function.
			// $2 represents the parameter value for the ts column.
			insertValues[insertValues.length] = `($1[${insertValues.length + 1}], $2, '${event.data.id}', ` + `'${JSON.stringify(event).replace(/'/g, '\'\'')}')`;
		};
		const insertValues = [];
		R.forEach(eventsToInsertValues, events);
		return `SELECT insert_events($$${R.join(',', insertValues)}$$)`;
	},
	yieldToEventLoop: () => {
		return new Promise((resolve) => {
			setImmediate(() => resolve());
		});
	}
};

module.exports = utils;
