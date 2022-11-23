const express = require("express");
 
// recordRoutes is an instance of the express router.
// We use it to define our routes.
// The router will be added as a middleware and will take control of requests starting with path /record.
const movieRoutes = express.Router();
 
// This will help us connect to the database
const dbo = require("../db/conn");
 
// This help convert the id from string to ObjectId for the _id.
const ObjectId = require("mongodb").ObjectId;
 
let movies;
 
// This section will help you get a list of all the records.
movieRoutes.route("/movie").get((req, res) => {
 let db_connect = dbo.getDb("movie");
 db_connect
   .collection("movies")
   .find({})
   .toArray(function (err, result) {
     if (err) throw err;
     movies = result
     res.send(movies);
   });
});
 
// This section will help you get a single record by id
movieRoutes.route("/movie/:id").get((req, res) => {
 let db_connect = dbo.getDb();
 let myquery = { _id: ObjectId(req.params.id) };
 db_connect
   .collection("movies")
   .findOne(myquery, function (err, result) {
     if (err) throw err;
     res.json(result);
   });
});

movieRoutes.route("/search").get( async (req, res) => {
  let db_connect = dbo.getDb("movie");
  try {
      let result = await db_connect.collection("movies").aggregate([
          {
              "$search": {
                  "index": "default",
                  "autocomplete": {
                      "query": `${req.query.query}`,
                      "path": "title",
                      "fuzzy": {
                          "maxEdits": 2,
                          "prefixLength": 3,
                      }
                  }
              }
          }
      ]).toArray();
      res.send(result);
  } catch (e) {
      res.status(500).send({ message: e.message });
  }
});
 
 
module.exports = movieRoutes;