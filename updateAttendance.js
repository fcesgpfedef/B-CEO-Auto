const csvFilePath='./csvFiles/JupiterDay-1.csv';
const csv=require('csvtojson');
const async = require('async');
const sql = require('mssql');
const fs = require('fs');
const sqlConfig = {
  user: 'ceo',
  password: '123',
  database: 'buddha',
  server: 'localhost',
  options: {
    trustServerCertificate: true // change to true for local dev / self-signed certs
  }
};
const tableName = 'jupiterRegData';

const dirName = './csvFiles/zoomReports';

fs.readdir(dirName, function(err, filenames) {
    if (err) {
      console.log(err);
      return;
    }
    async.eachSeries(filenames, (fileName, cbSeries) => {
      console.log(fileName);
      csv()
      .fromFile(`${dirName}/${fileName}`)
      .then(async (jsonObj)=>{
        let pool = await sql.connect(sqlConfig);
        const updateAttendanceByEmail = (eachRow) => {
          return new Promise((resolve, reject) => {
            checkWithEmail(eachRow).then(resultemail => {
              // check for the lastName 
              resolve(resultemail);
            }).catch((e) => reject(e));
          });
        }

        const updateAttendanceByName = (eachRow) => {
          const name  = eachRow['Name (Original Name)'];
          const nameArr = name.split(' ');
          return new Promise((resolve, reject) => {
            checkWithFirstName(nameArr).then(resultFirstName => {
              if (resultFirstName.recordset.length > 1) {
                return checkWithLastName(nameArr);
              } else {
                return resolve(resultFirstName);
              }
            }).then((resultLastName) => resolve(resultLastName)).catch((e) => reject(e));
          });
        }

        const checkWithFirstName = (nameArr) => {
          return new Promise((resolve, reject) => {
            const request = pool.request();
            request.input('firstName', sql.VarChar, nameArr[0]);
            request.query(`select * from ${tableName} where firstName=@firstName`, (err, result) => {
              if (err) {
                console.log(err);
                reject(err);
              }
              // console.log('result of firstName length : ', result.recordset.length);
              resolve(result);
            });
          });
        };

        const checkWithLastName = (nameArr) => {
          return new Promise((resolve, reject) => {
            const requestTwo = pool.request();
            requestTwo.input('lastName', sql.VarChar, nameArr[1]);
            requestTwo.query(`select * from ${tableName} where lastName=@lastName`, (err, resultLastName) => {
              if (err) {
                console.log(err);
                reject(err);
              }
              // console.log('resultLastName length : ', resultLastName.recordset.length);
              resolve(resultLastName);
            });
          });
        };

        const checkWithEmail = (eachRow) => {
          return new Promise((resolve, reject) => {
            // console.log('HIE');
            const requestThree = pool.request();
            // console.log(eachRow['User Email']);
            requestThree.input('email', sql.VarChar, eachRow['User Email']);
            requestThree.query(`select * from ${tableName} where email=@email`, (err, resultemail) => {
              if (err) {
                console.log(err);
                reject(err);
              }
              // console.log(eachRow['User Email']);
              // console.log('resultemail length : ', resultemail.recordset.length);
              resolve (resultemail);
            });
          });
        };

        async.eachSeries(jsonObj, (eachRow, cb) => {
          // console.log('..........................', eachRow);
          const operation = async () => {
            let attendanceRes = await updateAttendanceByEmail(eachRow);
            let attendanceResByName = await updateAttendanceByName(eachRow);
            // console.log('attendanceRes.recordset.length...', attendanceRes.recordset.length);
            try {
              if (attendanceRes.recordset.length === 1) {
                // Increase the attendance count by 1
                await updateAttendanceCount(attendanceRes, 'byEmail')
                return;
              }
              if (attendanceResByName.recordset.length === 1) {
                // Increase the attendance count by 1
                await updateAttendanceCount(attendanceResByName, 'byName');
                return;
              }
            } catch(e) {
              console.log(e);
            }
          }
          operation().then(() => cb()).catch((e) => cb());
        },
        function(err) {
          console.log('all done!!!');
          cbSeries();
        });

        const updateAttendanceCount = (data, dataFlag) => {
          return new Promise((resolve, reject) => {
            const requestUpdate = pool.request();
            let updateQuery = '';
            const dayNo = fileName.split('.');
            let attendObj;
            try {
              attendObj = data.recordset[0].presentDates;
              attendObj = attendObj.concat(";", dayNo[0])
            }catch (erMsg) {
              // console.log(erMsg);
              attendObj = attendObj = dayNo[0];
            }
            // console.log(attendObj)
            if (dataFlag === 'byEmail') {
              updateQuery = `update ${tableName} set attendance_count=@attendance_count,presentDates=@presentDates, lastPresentDay=@lastPresentDay,updatedTime=@updatedTime where email=@email`;
              requestUpdate.input('email', sql.VarChar, data.recordset[0].email);
              requestUpdate.input('attendance_count', sql.VarChar, data.recordset[0].attendance_count + 1);
              requestUpdate.input('updatedTime', sql.DateTime, new Date());
              requestUpdate.input('lastPresentDay', sql.VarChar, dayNo[0]);
              requestUpdate.input('presentDates', sql.VarChar, attendObj);
            } else {
              updateQuery = `update ${tableName} set attendance_count=@attendance_count,presentDates=@presentDates,lastPresentDay=@lastPresentDay, updatedTime=@updatedTime where firstName=@firstName and lastName=@lastName`;
              requestUpdate.input('firstName', sql.VarChar, data. recordset[0].firstName);
              requestUpdate.input('lastName', sql.VarChar, data.recordset[0].lastName);
              requestUpdate.input('attendance_count', sql.VarChar, data.recordset[0].attendance_count + 1);
              requestUpdate.input('updatedTime', sql.DateTime, new Date());
              requestUpdate.input('lastPresentDay', sql.VarChar, dayNo[0]);
              requestUpdate.input('presentDates', sql.VarChar, attendObj);
            }
            requestUpdate.query(updateQuery, (err, resultUpdate) => {
              if (err) {
                console.log(err);
                reject(err);
              }
              resolve (resultUpdate);
            });
          });
        }
      });
      
    }, function(err) {
      console.log('Series all done');
    })
});
