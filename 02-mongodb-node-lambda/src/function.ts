import { MongoClient } from "mongodb";

const client = new MongoClient(process.env['MONGODB_CONNECTION_STRING'] as string);

export const handler = async(event: any) => {
    const db = await client.db("sample_mflix");
    const collection = await db.collection("movies");
    const body = await collection.find().limit(10).toArray();
    const response = {
        statusCode: 200,
        body
    };
    return response;
};