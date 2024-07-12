const express = require('express');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const db = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'Chinnu@2024',
    database: 'project2'
});

db.connect(err => {
    if (err) {
        console.error('Error connecting to database:', err);
        return;
    }
    console.log('Connected to database');
});

function getIdField(table) {
    const idFields = {
        authors: 'auID',
        customer: 'custID',
        publishers: 'pubID',
        titles: 'titleID',
        titleauthors: 'titleID,auID',
        subjects: 'subID'
    };
    return idFields[table];
}

function formatDates(data) {
    const dateFields = ['birthDate', 'hireDate', 'pubDate'];
    dateFields.forEach(field => {
        if (data[field]) {
            data[field] = new Date(data[field]).toISOString().slice(0, 10);
        }
    });
    return data;
}

app.get('/api/tables', (req, res) => {
    const sql = 'SHOW TABLES';
    db.query(sql, (err, results) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        const tables = results.map(row => Object.values(row)[0]);
        res.json(tables);
    });
});

app.get('/api/:table', (req, res) => {
    const table = req.params.table;
    const sql = `SELECT * FROM ${table}`;
    db.query(sql, (err, results) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        res.json(results);
    });
});

app.post('/api/:table', (req, res) => {
    const table = req.params.table;
    let data = req.body;

    data = formatDates(data);

    const sql = `INSERT INTO ${table} SET ?`;
    db.query(sql, data, (err, result) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        res.json({ id: result.insertId });
    });
});

app.put('/api/:table/:id', (req, res) => {
    const table = req.params.table;
    const idField = getIdField(table);
    let data = req.body;

    data = formatDates(data);

    let sql;

    if (table === 'titleauthors') {
        const [titleID, auID] = req.params.id.split(',').map(Number);
        sql = `UPDATE ${table} SET ? WHERE titleID = ? AND auID = ?`;
        db.query(sql, [data, titleID, auID], (err, result) => {
            if (err) {
                if (err.code === 'ER_ROW_IS_REFERENCED_2') {
                    res.status(400).json({ error: 'Updating not possible due to constraints' });
                } else {
                    res.status(500).json({ error: err.message });
                }
                return;
            }
            res.send('Record updated...');
        });
    } else {
        sql = `UPDATE ${table} SET ? WHERE ${idField} = ?`;
        db.query(sql, [data, req.params.id], (err, result) => {
            if (err) {
                if (err.code === 'ER_ROW_IS_REFERENCED_2') {
                    res.status(400).json({ error: 'Updating not possible due to constraints' });
                } else {
                    res.status(500).json({ error: err.message });
                }
                return;
            }
            res.send('Record updated...');
        });
    }
});
/*
app.delete('/api/:table/:id', (req, res) => {
    const table = req.params.table;
    const idField = getIdField(table);
    let sql;

    if (table === 'titleauthors') {
        const [titleID, auID] = req.params.id.split(',').map(Number);
        sql = `DELETE FROM ${table} WHERE titleID = ? AND auID = ?`;
        db.query(sql, [titleID, auID], (err, result) => {
            if (err) {
                if (err.code === 'ER_ROW_IS_REFERENCED_2') { // Handle foreign key constraint error
                    res.status(400).json({
                        error: 'Deletion not possible due to constraints'
                    });
                } else {
                    console.error(`Failed to delete record from ${table} with titleID=${titleID} and auID=${auID}: ${err.message}`);
                    res.status(500).json({
                        error: err.message
                    });
                }
            } else {
                res.send('Record deleted...');
            }
        });
    } else {
        sql = `DELETE FROM ${table} WHERE ${idField} = ?`;
         
        db.query(sql, [req.params.id], (err, result) => {
            if (err) {
                if (err.code === 'ER_ROW_IS_REFERENCED_2') { // Handle foreign key constraint error
                    res.status(400).json({
                        error: 'Deletion not possible due to constraints'
                    });
                } else {
                    console.error(`Failed to delete record from ${table} with ${idField}=${req.params.id}: ${err.message}`);
                    res.status(500).json({
                        error: err.message
                    });
                }
            } else {
                res.send('Record deleted...');
            }
        });
    }
});*/
/*async function deleteRecord(table, id) {
            console.log(`Deleting record ${id} from ${table}`); // Debugging
            try {
                let apiUrlDelete;
                if (table === 'titles') {
                    alert("Cannot delete a child row: a foreign key constraint fails");
                    return;
                }
                if (table === 'titleauthors') {
                    const [titleID, auID] = id.split(',');
                    apiUrlDelete = `${apiUrl}/${table}/${titleID},${auID}`;
                } else {
                    apiUrlDelete = `${apiUrl}/${table}/${id}`;
                }
        
                const response = await fetch(apiUrlDelete, { method: 'DELETE' });
                if (!response.ok) {
                    throw new Error(`Error: ${response.statusText}`);
                } else {
                    fetchTableData(table);
                }
            } catch (error) {
                alert(`Failed to delete record: ${error.message}`);
            }
        }*/
/*app.delete('/api/:table/:id', (req, res) => {
    const table = req.params.table;
    const idField = getIdField(table);
    let sql;

    if (table === 'titleauthors') {
        const [titleID, auID] = req.params.id.split(',').map(Number);
        sql = `DELETE FROM ${table} WHERE titleID = ? AND auID = ?`;
        db.query(sql, [titleID, auID], (err, result) => {
            if (err) {
                if (err.code === 'ER_ROW_IS_REFERENCED_2') {
                    res.status(400).json({
                        error: 'Deletion not possible due to constraints'
                    });
                } else {
                    console.error(`Failed to delete record from ${table} with titleID=${titleID} and auID=${auID}: ${err.message}`);
                    res.status(500).json({
                        error: err.message
                    });
                }
                return;
            }
            res.send('Record deleted...');
        });
    } else {
        sql = `DELETE FROM ${table} WHERE ${idField} = ?`;
        db.query(sql, [req.params.id], (err, result) => {
            if (err) {
                if (err.code === 'ER_ROW_IS_REFERENCED_2') {
                    res.status(400).json({
                        error: 'Deletion not possible due to constraints'
                    });
                } else {
                    console.error(`Failed to delete record from ${table} with ${idField}=${req.params.id}: ${err.message}`);
                    res.status(500).json({
                        error: err.message
                    });
                }
                return;
            }
            res.send('Record deleted...');
        });
    }
});
*/
/*
 app.delete('/api/:table/:id', (req, res) => {
    const table = req.params.table;
    const idField = getIdField(table);
    let sql;

    if (table === 'titleauthors') {
        const [titleID, auID] = req.params.id.split(',').map(Number);
        sql = `DELETE FROM ${table} WHERE titleID = ? AND auID = ?`;
        db.query(sql, [titleID, auID], (err, result) => {
            if (err) {
                if (err.code === 'ER_ROW_IS_REFERENCED_2') {
                    res.status(400).json({
                        error: 'Deletion not possible due to constraints'
                    });
                } else {
                    console.error(`Failed to delete record from ${table} with titleID=${titleID} and auID=${auID}: ${err.message}`);
                    res.status(500).json({
                        error: err.message
                    });
                }
                return;
            }
            res.send('Record deleted...');
        });
    } else {
        sql = `DELETE FROM ${table} WHERE ${idField} = ?`;
        db.query(sql, [req.params.id], (err, result) => {
            if (err) {
                if (err.code === 'ER_ROW_IS_REFERENCED_2') {
                    res.status(400).json({
                        error: 'Deletion not possible due to constraints'
                    });
                } else {
                    console.error(`Failed to delete record from ${table} with ${idField}=${req.params.id}: ${err.message}`);
                    res.status(500).json({
                        error: err.message
                    });
                }
                return;
            }
            res.send('Record deleted...');
        });
    }
});*/
app.delete('/api/:table/:id', (req, res) => {
    const table = req.params.table;
    const idField = getIdField(table);
    let sql;

    if (table === 'titleauthors') {
        const [titleID, auID] = req.params.id.split(',').map(Number);
        sql = `DELETE FROM ${table} WHERE titleID = ? AND auID = ?`;
        db.query(sql, [titleID, auID], (err, result) => {
            if (err) {
                if (err.code === 'ER_ROW_IS_REFERENCED_2') {
                    res.status(400).json({
                        error: 'Deletion not possible due to constraints'
                    });
                } else {
                    console.error(`Failed to delete record from ${table} with titleID=${titleID} and auID=${auID}: ${err.message}`);
                    res.status(500).json({
                        error: err.message
                    });
                }
                return;
            }
            res.send('Record deleted...');
        });
    } else {
        sql = `DELETE FROM ${table} WHERE ${idField} = ?`;
        db.query(sql, [req.params.id], (err, result) => {
            if (err) {
                if (err.code === 'ER_ROW_IS_REFERENCED_2') {
                    res.status(400).json({
                        error: 'Deletion not possible due to constraints'
                    });
                } else {
                    console.error(`Failed to delete record from ${table} with ${idField}=${req.params.id}: ${err.message}`);
                    res.status(500).json({
                        error: err.message
                    });
                }
                return;
            }
            res.send('Record deleted...');
        });
    }
});

       

const port = 3001;
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
