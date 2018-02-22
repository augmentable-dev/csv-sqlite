const { getParsedLines } = require('../index')
const path = require('path')


describe('handling csvs', () => {
    const csvFilePath = path.join(__dirname, 'data/CivilWarMemorials.csv')

    test('can get the correctly parsed lines of a csv', async () => {
        const parsedLines = await getParsedLines(csvFilePath, 0, 10, 0)
        const expectedParsing = 
`Alabama,Anniston,Major John Pelham Monument,Monument,1905,Confederate State
Alabama,Ashville,Confederate Monument,Monument/Courthouse,1923,Confederate State
Alabama,Athens,Confederate Soldier Monument,Monument/Courthouse,1909,Confederate State
Alabama,Beauregard,Unincorporated Area Beauregard,Municipality,,Confederate State
Alabama,Birmingham,Confederate Soldiers and Sailors Monument,Monument,1905,Confederate State
Alabama,Brewton,Jefferson Davis Community College,School,1965,Confederate State
Alabama,Butler,Confederate Monument,Monument/Courthouse,1937,Confederate State
Alabama,Carrollton,Confederate Memorial,Monument/Courthouse,1927,Confederate State
Alabama,Cedar Bluff,General N.B. Forrest Captured Colonel A.D. Straight Monument,Monument,1939,Confederate State
Alabama,Centre,Confederate Memorial,Monument/Courthouse,1988,Confederate State
Alabama,Centreville,Confederate Monument,Monument/Courthouse,1910,Confederate State`
        .split('\n').map(l => l.split(','))
        expect(parsedLines).toEqual(expectedParsing)
    })

})