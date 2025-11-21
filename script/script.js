const { MongoClient } = require("mongodb");
const fs = require("fs");

// CONFIG
const uri = "mongodb://localhost:27017";
const dbName = "finac_crm";
const collectionName = "jobs";

// LOG FILES
const updatedLog = "updated_ids.log";
const skippedLog = "skipped_ids.log";
const errorLog = "error_ids.log";
const summaryLog = "summary.log";
const cursorErrorLog = "cursor_error.log";

// Clear old logs
fs.writeFileSync(updatedLog, "");
fs.writeFileSync(skippedLog, "");
fs.writeFileSync(errorLog, "");
fs.writeFileSync(summaryLog, "");
fs.writeFileSync(cursorErrorLog, "");

// FILTER
const filter = {
    $or: [
        // num_search invalid
        { num_search: { $exists: false } },
        { num_search: null },
        { num_search: "" },
        { num_search: { $type: "null" } },
        { num_search: /^\s*$/ },
        { num_search: { $not: /^j\d+c\d+$/ } },

        // job_id invalid
        { job_id: { $exists: false } },
        { job_id: null },
        { job_id: "" },
        { job_id: /^\s*$/ },

        // client_code invalid
        { client_code: { $exists: false } },
        { client_code: null },
        { client_code: "" },
        { client_code: /^\s*$/ }
    ]
};

// UPDATE PIPELINE
const updatePipeline = [
    {
        $set: {
            num_search: {
                $cond: {
                    if: {
                        $and: [
                            { $ifNull: ["$job_id", false] },
                            { $ifNull: ["$client_code", false] }
                        ]
                    },
                    then: {
                        $concat: [
                            "j",
                            { $toString: "$job_id" },
                            "c",
                            { $toString: "$client_code" }
                        ]
                    },
                    else: "$num_search"
                }
            }
        }
    }
];

async function runScript() {
    const client = new MongoClient(uri);

    try {
        await client.connect();
        console.log("Connected to MongoDB");

        const col = client.db(dbName).collection(collectionName);

        const total = await col.countDocuments(filter);
        console.log("Total invalid documents:", total);

        if (total === 0) {
            console.log("No invalid documents found.");
            fs.writeFileSync(summaryLog, "No invalid documents found.");
            return;
        }

        let scanned = 0;
        let updated = 0;
        let skipped = 0;

        // const cursor = col.find(filter)
        const cursor = col.find(filter, { noCursorTimeout: true , batchSize: 1 });

        try {
            for await (const doc of cursor) {
                scanned++;

                try {
                    const result = await col.updateOne({ _id: doc._id }, updatePipeline);

                    if (result.modifiedCount === 1) {
                        updated++;
                        fs.appendFileSync(updatedLog, doc._id + "\n");
                    } else {
                        skipped++;
                        fs.appendFileSync(skippedLog, doc._id + "\n");
                    }
                } catch (err) {
                    fs.appendFileSync(errorLog, `${doc._id} - ${err}\n`);
                }

                if (scanned % 5 === 0) {
                    console.log(`Processed ${scanned}/${total}...`);
                }
            }
        } catch (cursorErr) {
            fs.appendFileSync(cursorErrorLog, `Cursor failed at document #${scanned}: ${cursorErr}\n`);
            console.error("Cursor error occurred. Check cursor_error.log for details.");
        } finally {
            await cursor.close();
        }


        const summary =
            `SUMMARY\n` +
            `-----------------\n` +
            `Total Invalid: ${total}\n` +
            `Scanned: ${scanned}\n` +
            `Updated: ${updated}\n` +
            `Skipped: ${skipped}\n`;

        fs.writeFileSync(summaryLog, summary);
        console.log(summary);

    } catch (err) {
        console.error("Fatal error:", err);
        fs.appendFileSync(errorLog, "Fatal: " + err + "\n");
    } finally {
        await client.close();
    }
}

runScript();
