
const _ = require('lodash')
const util = require('util')
const {BigQuery} = require('@google-cloud/bigquery');

function DeduceColumns(rows)
{
	return _.union(...rows.map(row => Object.keys(row)));
}

function PrintData(options, tableId, rows)
{	
	let columns = DeduceColumns(rows);
	
	console.log("[" + tableId + "]");
	let columnWidths = [];

	let truncatedRows = rows.slice(1, 12);
	
	for(let column of columns)
	{		
		columnWidths.push(Math.max(column.length, ...truncatedRows.map(row => (row[column] || "null").toString().length)));
	}
	
	let headers = "| ";
	let seperator = "+-"
	for(let i in columns)
	{
		headers += " " + columns[i].padEnd(columnWidths[i] + 1) + "|";
		seperator += "-".padEnd(columnWidths[i] + 2, "-") + "+"
	}
	console.log(headers);
	console.log(seperator);
	
	for(let row of truncatedRows)
	{
		let rowText = "| ";
		for(let i in columns)
		{
			rowText += " " + (row[columns[i]] || "null").toString().padEnd(columnWidths[i] + 1) + "|";
		}
		console.log(rowText);
	}
	
	if (truncatedRows.length < rows.length)
		console.log(`... ${rows.length - truncatedRows.length} more rows ...`);
	
	console.log("");
}

function DeduceTypeAndMode(options, name, values)
{
	let types = _.uniq(values.map(v => typeof v));
	
	if (types.length != 1)
		throw new Error("Unable to deduce type: multiple types in same column " + types);
	
	if (name === "time") return [ "TIMESTAMP", "NULLABLE" ];
	
	let jsType = types[0];
	
	switch (jsType)
	{
		case "string": 	return [ "STRING", "NULLABLE" ];
		case "boolean": return [ "BOOLEAN", "NULLABLE" ];
		case "number": 	return [ "FLOAT", "NULLABLE" ];
		
		case "object":
			let [ subtype, submode ] = DeduceTypeAndMode(options, name, _.union(...values));
			
			if (submode === "REPEATED") throw new Error("Arrays can only have a nesting level of one");
			return  [ subtype, "REPEATED" ];
		
		default:
			throw new Error("Unable to deduce bigquery type of " + jsType);
	}
}

function DeduceSchema(options, rows)
{
	let columns = DeduceColumns(rows);
	
	let fields = [];

	for(let column of columns)
	{
		
		let [ type, mode ] = DeduceTypeAndMode(options, column, rows.map(row => row[column]));
		fields.push(
		{
			name: column,
			type: type,
			mode: mode
		})
	}
	
	return { fields: fields };
}

async function CreateTable(options, dataset, tableId, rows)
{
	let columns = DeduceColumns(rows);
	let clustering = _.filter(columns, column => column.startsWith("_"));
	
	const tableOptions = 
	{
		clustering: 		options.clusterOnUnderscore && clustering.length > 0
								? { fields: clustering }
								: null ,
		timePartitioning: 	
		{ 
			type: "DAY", 
			field: "time",
			expirationMs: 	options.partionExpiryDays != 0
								? options.partionExpiryDays * 1000 * 60 * 60 * 24
								: null
		},
		schema: DeduceSchema(options, rows)
	};
	
	if(options.printToConsole)
	{
		console.log("Creating table [" + tableId + "] with options:")
		console.log(util.inspect(tableOptions, {showHidden: false, depth: null, colors: true}));
	}
	
	return await dataset.createTable(tableId, tableOptions);
}

async function WriteToTable(options, dataset, tableId, rows)
{	
	PrintData(options, tableId, rows);
	
	let table;
	try
	{
		table = await dataset.table(tableId);
		await table.get();
	}
	catch (e)
	{
		console.log("table [" + tableId + "] does not exists: " + e.message);
		
		if (!e.message.startsWith("Not found"))
			throw new Error("Unkown error db: " + e.message);
		
		if (options.createMissingTables) 
		{
			console.log("Creating table: " + tableId);
			[ table ] = await CreateTable(options, dataset, tableId, rows);
		}
		else
			throw new Error("Missing table: " + tableId);
	}

	try 
	{	
		if (await table.insert(rows))
			console.log("Insert successfull");
		else
			console.log("Insert failure");
	}
	catch(e)
	{
		console.log("Failed to insert rows");
		
		if (!options.createMissingColumns)
			throw e;
		
		if (!e.response)												throw e;
		if (!e.response.kind)											throw e;
		if (e.response.kind !== "bigquery#tableDataInsertAllResponse") 	throw e;
		if (!e.response.insertErrors) 									throw e;
		
		let rowsToReinsert = [];
		let [ metadata ] = await table.getMetadata();
		let schema = metadata.schema;
		let addedColumns = [];
		
		//console.log(util.inspect(e.response.insertErrors, {showHidden: false, depth: null, colors: true}))
		
		for(let rowError of e.response.insertErrors)
		{
			rowsToReinsert.push(rows[rowError.index]);
			
			for (let err of rowError.errors)
			{
				if (err.reason !== "invalid") throw new Error("unkown error");
				if (!err.message.startsWith("no such field")) throw new Error("unkown error");
				
				if (addedColumns.includes(err.location))
					continue;
				
				let [ type, mode ] = DeduceTypeAndMode(options, err.location, _.map(rows, (row) => row[err.location]));
				
				if (options.printToConsole) console.log("Adding column '" + err.location + "' of type '" + type + "' to '" + table.id + "'");
				
				addedColumns.push(err.location);
				
				schema.fields.push(
					{
						name: err.location,
						type: type,
						mode: mode
					});
			}
		}
		
		console.log("new schema: ", util.inspect(schema, {showHidden: false, depth: null, colors: true}))
		metadata.schema = schema;
		
		const [ result ] = await table.setMetadata(metadata);
		
		return rowsToReinsert
	}
	
	return [];
}

exports.BQInterface = class 
{
	constructor(options)
	{
		if(options.printToConsole) console.log("Writing into [" + options.dataset + "]");
		
		this.Dataset = new BigQuery().dataset(options.dataset);
		
		this.Buffer = {};
	}
	
	async WriteToDatabase (options, stats)
	{
		let time = stats.time;
		
		for(let table of Object.keys(stats.tables))
		{
			if (!this.Buffer[table])
			{
				this.Buffer[table] = 
				{
					last_sent: Date.now(),
					queue: []
				}
			}
			
			stats.tables[table].forEach(r => r.time = time);
			
			this.Buffer[table].queue = this.Buffer[table].queue.concat(stats.tables[table]);
		}
		
		let now = Date.now();
		
		for(let table of Object.keys(this.Buffer))
		{
			let shouldFlush = false;
			let buffer = this.Buffer[table];
			
			if (buffer.queue.length > options.rowsToBuffer)
			{
				console.log(`${table} has ${buffer.queue.length} rows queued, flushing`);
				shouldFlush = true;
			}
			
			if (buffer.last_sent + options.maxBufferTime * 1000 * 60 < now)
			{
				console.log(`Flushing ${table} due to time`);
				shouldFlush = true;
			}
		
			if (!shouldFlush)
				continue;
			
			buffer.last_sent = now;
			buffer.queue = await WriteToTable(options, this.Dataset, table, buffer.queue);
		}
		
		if (options.printToConsole)
			console.log(`${_.sumBy(Object.values(this.Buffer), (b) => b.queue.length)} rows buffered in ${Object.keys(this.Buffer).length} tables`);
	}
}