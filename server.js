const express = require("express");
const fs = require("fs");
const app = express();

app.use(express.json());

app.post("/location", (req, res) => {
  const { lat, lon, time } = req.body;

  const entry = `${time} - Lat: ${lat}, Lon: ${lon}\n`;
  fs.appendFileSync("locations.log", entry);

  console.log("Location received:", req.body);
  res.send({ status: "OK" });
});

app.get("/", (req, res) => {
  res.send("Location server is running!");
});

app.listen(3000, () => console.log("Server running on port 3000"));
