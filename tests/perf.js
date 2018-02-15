const { removeDB, createDB, importFromFile, get, countLines, readFile, query } = require('../index')
const path = require('path')

const dbFilePath = path.join(__dirname, 'data/test-perf.sqlite')
// const csvFilePath = path.join(__dirname, 'data/500000_sales.csv')
// const csvFilePath = path.join(__dirname, 'data/CivilWarMemorials.csv')

const run = async () => {
    await removeDB(dbFilePath)
    await createDB(dbFilePath)

    const lineCount = await countLines(csvFilePath)
    console.log(lineCount, 'lines in file')


    // let interval = setInterval(() => {
    //     console.log(process.memoryUsage().heapUsed / (.00000001), 'mb of heap in use')
    // }, 1000)

    const tableName = 'testTable'
    const imported = await importFromFile(dbFilePath, csvFilePath, tableName, 0)

    const q = await get(dbFilePath, `SELECT COUNT(*) as c FROM ${tableName}`)
    console.log(q.c, 'lines imported')
}

const start = new Date()
run().then(async () => {
    const end = new Date()
    console.log((end - start) / 1000, 'seconds to import')

    await removeDB(dbFilePath)
})