const { fileStat, countLines, getLine, getLines } = require('../index')

describe('file related functions', () => {

    const sales_500k = require.resolve('./data/500000_sales.csv')

    test('returns some stats for a file', async () => {
        const s = fileStat(sales_500k)
        expect(s).toEqual(expect.anything())
    })

    test('count the lines in a file', async () => {
        const lineCount = await countLines(sales_500k)
        expect(lineCount).toBe(500000 + 1)
    })

    test('get the correct line from a file', async () => {
        const line = await getLine(9, sales_500k)
        const lineValue = 'Sub-Saharan Africa,Ghana,Office Supplies,Online,L,3/23/2017,601245963,4/15/2017,896,651.21,524.96,583484.16,470364.16,113120.00'
        expect(line).toBe(lineValue)

        const lastLine = await getLine(500000, sales_500k)
        const lastlLineValue = 'Europe,Slovakia,Household,Online,H,5/13/2015,984919011,6/12/2015,8277,668.27,502.54,5531270.79,4159523.58,1371747.21'
        expect(lastLine).toBe(lastlLineValue)
    })
    
    
    test('get the correct range of lines from a file', async () => {
        const lines = await getLines(9, 10, sales_500k)
        const lineValues = [
            'Sub-Saharan Africa,Ghana,Office Supplies,Online,L,3/23/2017,601245963,4/15/2017,896,651.21,524.96,583484.16,470364.16,113120.00',
            'Sub-Saharan Africa,Tanzania,Cosmetics,Offline,L,5/23/2016,739008080,5/24/2016,7768,437.20,263.33,3396169.60,2045547.44,1350622.16',
            'Asia,Taiwan,Fruits,Offline,M,2/9/2014,732588374,2/23/2014,8034,9.33,6.92,74957.22,55595.28,19361.94',
            'Middle East and North Africa,Algeria,Cosmetics,Online,M,2/18/2011,761723172,2/24/2011,9669,437.20,263.33,4227286.80,2546137.77,1681149.03',
            'Asia,Singapore,Snacks,Online,C,1/28/2013,176461303,2/7/2013,7676,152.58,97.44,1171204.08,747949.44,423254.64',
            'Australia and Oceania,Papua New Guinea,Clothes,Offline,L,6/20/2011,647164094,7/14/2011,9092,109.28,35.84,993573.76,325857.28,667716.48',
            'Asia,Vietnam,Personal Care,Online,M,4/4/2010,314505374,5/6/2010,7984,81.73,56.67,652532.32,452453.28,200079.04',
            'Sub-Saharan Africa,Uganda,Personal Care,Online,M,6/19/2014,539471471,7/21/2014,451,81.73,56.67,36860.23,25558.17,11302.06',
            'Sub-Saharan Africa,Zimbabwe,Office Supplies,Offline,C,3/28/2011,953361213,4/8/2011,9623,651.21,524.96,6266593.83,5051690.08,1214903.75',
            'Sub-Saharan Africa,Ethiopia,Cosmetics,Online,M,7/7/2011,807785928,7/25/2011,662,437.20,263.33,289426.40,174324.46,115101.94',
            'Europe,France,Cosmetics,Online,M,12/7/2015,324669444,1/18/2016,5758,437.20,263.33,2517397.60,1516254.14,1001143.46'
        ]
        expect(lines).toEqual(lineValues)
    })
})