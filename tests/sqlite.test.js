const { createDB, removeDB, createTable, prepare, removeTable, run } = require('../index')
const path = require('path')
const fs = require('fs')


describe('sqlite related functionality', () => {

    describe('sqlite db file related functionality', () => {

        const dbFilePath = path.join(__dirname, 'data/test-db.sqlite')
    
        test('can create a new sqlite db file', async () => {
            await createDB(dbFilePath)
            expect(fs.existsSync(dbFilePath)).toBe(true)
        })
    
        test('can delete a sqlite db file', async () => {
            await createDB(dbFilePath)
            expect(fs.existsSync(dbFilePath)).toBe(true)
            await removeDB(dbFilePath)
            expect(fs.existsSync(dbFilePath)).toBe(false)
        })
    
    })
    
    
    describe('sqlite db table related functionality', () => {
    
        const dbFilePath = path.join(__dirname, 'data/test-db-tables.sqlite')

        beforeAll(() => {
            return removeDB(dbFilePath).then(() => createDB(dbFilePath))
        })

        const tableName = 'testTable'
        const columns = ['column_1', 'column_2', 'column_3']
    
        test('can create a new table with an array of strings as schema definition with no type information', async () => {
            const table = await createTable(dbFilePath, tableName, columns)
            const tableInfo = await prepare(dbFilePath, `PRAGMA table_info(${tableName});`)

            tableInfo.forEach((col, i) => {
                if (i === 0) return // ignore primary key column
                expect(col.name).toBe(columns[i-1])
            })
        })

        test('can copy a table', async () => {
            const r = run(dbFilePath, `CREATE TABLE copied AS SELECT * FROM ${tableName} WHERE 0`)
            const tableInfo = await prepare(dbFilePath, `PRAGMA table_info(copied);`)
            tableInfo.forEach((col, i) => {
                if (i === 0) return // ignore primary key column
                expect(col.name).toBe(columns[i-1])
            })
        })

        test('can remove a table by name', async () => {
            const remove = removeTable(dbFilePath, tableName)
            const tableInfo = await prepare(dbFilePath, `PRAGMA table_info(${tableName});`)
            
            expect(tableInfo).toEqual([])
        })

        afterAll(() => {
            return removeDB(dbFilePath)
        })
    
    })
})