const { removeDB, createDB, importFromFile, get } = require('../index')
const path = require('path')

jest.setTimeout(300*1000);

describe('importing csv files into sqlite dbs', () => {
    const dbFilePath = path.join(__dirname, 'data/test-import.sqlite')
    const csvFilePath = path.join(__dirname, 'data/CivilWarMemorials.csv')

    beforeAll(async () => {
        await removeDB(dbFilePath)
        await createDB(dbFilePath)
    })

    test('can import a csv file with the headerRowIndex supplied and no header supplied', async () => {
        const tableName = 'testTable'
        const imported = await importFromFile(dbFilePath, csvFilePath, tableName, 0)

        const q = await get(dbFilePath, `SELECT COUNT(*) as c FROM ${tableName}`)
        expect(q.c).toBe(1528)
    })

    afterAll(async () => {
        await removeDB(dbFilePath)
    })
})