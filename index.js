const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    try {
        await migrationUp();

        //Busca os dados da API no formato json com axios
        const { data } = await axios.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population');

        //deleta os dados da tabela api_data
        await db[DATABASE_SCHEMA].api_data.destroy({});

        //insere os dados no banco na coluna doc_record
        await db[DATABASE_SCHEMA].api_data.insert({
            api_name: data.source[0].name,
            doc_id: data.source[0].annotations.table_id,
            doc_name: data.source[0].annotations.dataset_name,
            doc_record: JSON.stringify(data.data),
        });

        //realizando o calculo de populacao em memoria
        const calculatePopulationFunction = () => {
            const filteredData = data.data.filter(item => item.Year >= 2018 && item.Year <= 2020);
            const population = filteredData.reduce((acc, item) => acc + item.Population, 0);
            return population;
        }

        //realizando o calculo de populacao atraves de select
        const calculatePopulationSelect = async () => {
            const result = await db.query(`
                WITH pop_year AS (
                    SELECT 
                        (jsonb_array_elements(doc_record)->>'Population')::INTEGER AS population,
                        (jsonb_array_elements(doc_record)->>'Year')::INTEGER AS year
                    FROM 
                        ${DATABASE_SCHEMA}.api_data
                )
                SELECT 
                    SUM(population) AS total_population
                FROM 
                    pop_year
                WHERE 
                    year IN (2018, 2019, 2020)
            `);
            return result[0].total_population;
        }

        //realizando o calculo usando a view
        const calculatePopulationView = async () => {
            const result = await db[DATABASE_SCHEMA].vw_population_sum.find();
            return result[0].total_population;
        }

        console.log('caculando a populacao dos anos de 2018, 2019 e 2020');
        console.log('calculatePopulationFunction', calculatePopulationFunction());
        console.log('calculatePopulationSelect', await calculatePopulationSelect());
        console.log('calculatePopulationView', await calculatePopulationView());

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();