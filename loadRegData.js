const csvFilePath='./csvFiles/JupiterFaceBookleads.csv';
const csv=require('csvtojson');
const async = require('async');
const sql = require('mssql');
const sqlConfig = {
    user: 'ceo',
    password: '123',
    database: 'buddha',
    server: 'localhost',
    options: {
      trustServerCertificate: true // change to true for local dev / self-signed certs
    }
  };
const tableName = 'jupiterRegdata';

csv()
.fromFile(csvFilePath)
.then(async (jsonObj)=>{
    // console.log('1');
    let pool = await sql.connect(sqlConfig);
    // console.log('2');

    const request = pool.request();
            
    const insertToDb = (eachRow) => {
        // console.log(eachRow['First Name'], eachRow['Last Name'], eachRow['Email'],eachRow['Phone']);
        return new Promise((resolve, reject) => {
            const request = pool.request();
            request.input('firstName', sql.VarChar, eachRow['First Name']);
            request.input('lastName', sql.VarChar, eachRow['Last Name']);
            request.input('email', sql.VarChar, eachRow['Email']);
            request.input('mobile', sql.VarChar, eachRow['Phone']);
            request.input('attendance_count', sql.Int, 0);
            request.input('is_duplicate', sql.VarChar, 'N');
            request.input('comments', sql.VarChar, '');
            // request.input('uuid', sql.Int, eachRow.UIID);
            request.input('updatedTime', sql.DateTime, new Date());
            request.input('lastPresentDay', sql.VarChar, '');
            request.input('presentDates', sql.VarChar, '');

            request.query(`INSERT INTO ${tableName}(firstName, lastName, email, mobile,attendance_count, updatedTime, lastPresentDay, presentDates) values (@firstName, @lastName, @email, @mobile,@attendance_count, @updatedTime, @lastPresentDay, @presentDates)`, (err, result) => {
                if (err) {
                    console.log(err);
                    reject(err);
                }
                console.log(result);
                resolve(result);
            });
        });
    }
    async.each(jsonObj, (eachRow, cb) => {
        const operation = async () => {
            // console.log(jsonObj.length);
            let insertResult = await insertToDb(eachRow);
            console.log(insertResult.rowsAffected);
        }
        operation().then(() => cb()).catch((e) => cb());
    },
    function(err) {
      console.log('all done!!!');
    });
});