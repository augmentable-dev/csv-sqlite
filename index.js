const md5File = require('md5-file/promise')
const csv = require('csvtojson')
const fs = require('fs')
const readline = require('readline')
const Database = require('better-sqlite3')
const _ = require('lodash')
const os = require('os')

const knex = require('knex')({
    client: 'sqlite',
    useNullAsDefault: true,
    connection: false
})
// return an MD5 hash of the provided file
module.exports.md5File = md5File;

// count the number of lines in the provided file
module.exports.countLines = function(filePath) {
    return new Promise((resolve, reject) => {
        let count = 0;
        fs.createReadStream(filePath)
            .on('data', function(chunk) {
                for (let i = 0; i < chunk.length; ++i)
                    if (chunk[i] == 10) count++;
            })
            .on('end', function() {
                resolve(count);
            });
    })
}

// get info about the provided file
module.exports.fileStat = function(filePath) {
    const stats = fs.statSync(filePath);
    return stats;
}

// pull out a specific line from a file
module.exports.getLine = function(lineIndex, filePath) {
    return new Promise((resolve, reject) => {
        let counter = 0;
        let foundLine = null;
        const rl = readline.createInterface({
            input: fs.createReadStream(filePath),
          });
        rl.on('line', line => {
            if (counter === lineIndex) {
                foundLine = line
                rl.close()
            }
            counter++
        })
        rl.on('close', () => {
            resolve(foundLine)
        })
        rl.on('error', reject)
    })
}

// pull out a range of lines from a file
module.exports.getLines = function(startLineIndex, numLines, filePath) {
    return new Promise((resolve, reject) => {
        let counter = 0;
        let lines = [];
        const rl = readline.createInterface({
            input: fs.createReadStream(filePath),
          });
        rl.on('line', line => {
            if (counter >= startLineIndex && counter <= startLineIndex + numLines) {
                lines.push(line)
            }
            else if (counter > startLineIndex + numLines) rl.close()
            counter++
        })
        rl.on('close', () => {
            resolve(lines)
        })
        rl.on('error', reject)
    })
}

// create a db file at a specified path
module.exports.createDB = function(dbPath) {
    return new Database(dbPath)
}

// remove a db file at a specified path
module.exports.removeDB = function(dbPath) {
    return new Promise((resolve, reject) => {
        if (fs.existsSync(dbPath)) fs.unlink(dbPath, error => error ? reject(error): resolve())
        else resolve()
    })
}

// create a table in a db file, with a column defintion specified as an array of strings
module.exports.createTable = function(dbPath, tableName, columns) {
    const db = new Database(dbPath, {fileMustExist: true})
    const sql = knex.schema.createTable(tableName, table => {
        columns.forEach(c => {
            table.string(c)
        })
    }).toSQL()[0]
    return db.prepare(sql.sql).run(sql.bindings)
}

// check if a table already exists
module.exports.tableExists = function(dbPath, tableName) {
    const db = new Database(dbPath, {fileMustExist: true})
    const sql = knex.schema.hasTable(tableName).toSQL()[0]
    return db.prepare(sql.sql).get(sql.bindings)
}

// drop a table from a db file
module.exports.removeTable = function(dbPath, tableName) {
    const db = new Database(dbPath, {fileMustExist: true})
    const sql = knex.schema.dropTableIfExists(tableName).toString()
    return db.prepare(sql).run()
}

// execute a query and return all results on a db file
module.exports.prepare = function(dbPath, query, bindings = []) {
    const db = new Database(dbPath, {fileMustExist: true})
    return db.prepare(query).all(bindings)
}

// execute a query and return the first row
module.exports.get = function(dbPath, query, bindings = []) {
    const db = new Database(dbPath, {fileMustExist: true})
    return db.prepare(query).get(bindings)
}

// execute a query and return an iterator
module.exports.iterate = function(dbPath, query, bindings = []) {
    const db = new Database(dbPath, {fileMustExist: true})
    return db.prepare(query).iterate(bindings)
}

// run a query
module.exports.run = function(dbPath, query, bindings = []) {
    const db = new Database(dbPath, {fileMustExist: true})
    return db.prepare(query).run(bindings)
}

module.exports.getColumnsFromHeaderSpecification = async function(filePath, headerSpecification = 0, delimiter = 'auto') {
    let columns
    if (Array.isArray(headerSpecification)) columns = headerSpecification
    if (Number.isInteger(headerSpecification)) {
        const line = await module.exports.getLine(headerSpecification, filePath)
        columns = await new Promise((resolve, reject) => {
            csv({noheader: true, delimiter}).fromString(line).on('csv', resolve).on('error', reject)
        })
    }
    if (!columns) throw new Error("Invalid header specification, must either be an integer or an array of strings")

    columns = columns.map(_.snakeCase)
    return columns
}

module.exports.getParsedLines = async function(filePath, startLineIndex, numLines, headerSpecification = 0, delimiter = 'auto') {
    const columns = await module.exports.getColumnsFromHeaderSpecification(filePath, headerSpecification, delimiter)
    return new Promise((resolve, reject) => {
        let rows = []
        let counter = 0
        const parsing = csv({headers: columns, workerNum: os.cpus().length}) // TODO: only use workers if file size is large (or row count high)
            .fromFile(filePath)
            .on('csv', row => {
                if (counter >= startLineIndex && counter <= startLineIndex + numLines) {
                    rows.push(row)
                }
                else if (counter > startLineIndex + numLines) {
                    parsing.emit('end', {done: true})
                }
                counter++
            })
            .on('error', error => {
                reject(error)
            })
            .on('end', () => {
                resolve(rows)
            })
    })
}

// import a file into a new or existing table
// if a table with tableName doesn't exist, a new table will be created
// if a table with tableName exists, try to import into that table
// headerSpecification can either be an integer (index of headerRow in file) or an array of column names (string) to use
module.exports.importFromFile = async function(dbPath, filePath, tableName = path.basename(filePath), headerSpecification = 0, delimiter = 'auto') {
    const columns = await module.exports.getColumnsFromHeaderSpecification(filePath, headerSpecification, delimiter)
    const tableExists = module.exports.tableExists(dbPath, tableName)
    if (!tableExists) await module.exports.createTable(dbPath, tableName, columns)

    return new Promise((resolve, reject) => {
        const db = new Database(dbPath, {fileMustExist: true})
        db.prepare('BEGIN').run()
        let batch = []

        const batchSize = Math.floor(999 / columns.length)
        let statement = null
        csv({headers: columns, workerNum: os.cpus().length, flatKeys:true}) // TODO: only use workers if file size is large (or row count high)
        .fromFile(filePath)
        .on('csv', row => {
            if (batch.length && ((batch.length % batchSize) === 0)) {
                const batchObj = batch.map( csvRow => {
                    return _.zipObject(columns, csvRow)
                })
                const sql = knex.insert(batchObj).into(tableName).toSQL().toNative()
                if (!statement) statement = db.prepare(sql.sql)
                if (statement) statement.run(sql.bindings)
                batch = []
            }
            batch.push(row)
        })
        .on('error', error => {
            db.prepare('ROLLBACK').run()
            reject(error)
        })
        .on('end', () => {
            const batchObj = batch.map( csvRow => {
                return _.zipObject(columns, csvRow)
            })
            const sql = knex.insert(batchObj).into(tableName).toSQL().toNative()
            db.prepare(sql.sql).run(sql.bindings)
            db.prepare('COMMIT').run()
            resolve()
        })
    })

}