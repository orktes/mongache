const { MongoClient } = require("mongodb");

const uri =
    "mongodb://localhost:6666";


const client = new MongoClient(uri);
async function run() {
    try {
        await client.connect();
        const database = client.db("sample_mflix");
        const collection = database.collection("movies");
        // Query for a movie that has the title 'The Room'
        const query = { title: "The Room" };
        const options = {
            // sort matched documents in descending order by rating
            sort: { rating: -1 },
            // Include only the `title` and `imdb` fields in the returned document
            projection: { _id: 0, title: 1, imdb: 1 },
        };
        const movie = await collection.findOne(query, options);
        // since this method returns the matched document, not a cursor, print it directly
        console.log(movie);


        const all = await collection.find({}, { batchSize: 50 }).toArray();
        console.log(all);
        console.log(all[0], all.pop())

        const cur = collection.find({}, { batchSize: 50 });

        for (var i = 0; i < 100; i++) {
            const item = await cur.next()
            console.log(item);
        }

        await cur.close()
        console.log("Closed")

    } finally {
        await client.close(true);
    }
}
run().catch(console.dir);