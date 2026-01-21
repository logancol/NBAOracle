# NBA Oracle

Natural language querying web tool for basketball enthusiasts to derive exploratory insights from NBA play-by-play data.

## What does it do?

At the moment, NBAOracle exposes one endpoint (outside of user auth) that allows a client to ask NBA statistical questions that can be answered through some transformation of NBA play-by-play data. It relies on OpenAI's GPT 5.2 to generate efficient SQL queries from natural language user questions, validates and runs these queries against a database of nba data (updated nightly), then provides an interpretted result. This project is still in its relatively early stages and, due to the fallible nature of LLMs, it will be liable to make mistakes for the forseeable future. That being said, the end goal is for NBAOracle to be another tool in the belt of statistically-inclined sports enthusiasts to perform exploratory analysis and derive neat insights.

## Current limitations and best use cases

As of now, the API will only have access to play-by-play data from the current season (updated nightly at 3am EST) and will likely experience significant downtime as I continue to build the database out and work on a frontend. The goal of hosting the api and making it accessible at the moment is to catch bugs and limitations early from some test users. 

Some examples of questions that the api is currently suited to answer include: 
- How many points per game is Paolo Banchero averaging on shots from in the paint this season?
- What is the median points scored by opponents on the Pistons this season?
- Which 5 players have had the best fg% in home games this season (> 100 attempts)?
- Who has Alperen Sengun assisted the most this season?
- And any other question that can reasonably be answered via transformation or aggregation of play by play data.

Currently, the API can't do much to answer questions relating to defensive matchups, granular positional data, and some advanced stats, but this is on the way! 

## Example Usage

### Register your account

```
$ curl -X POST "https://nbaoracle.onrender.com/auth/register" \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"yourpassword!","full_name":"Test User"}'
```

### Login to retrieve Bearer Token
```
curl -X POST "https://nbaoracle.onrender.com/auth/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "username=you@example.com" \
  --data-urlencode "password=yourpassword!"
```

### Hit question endpoint
```
curl -X POST "https://nbaoracle.onrender.com/question" \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"question":"Who has Ryan Rollins assisted the most this season?"}'
```

### Response example:
```
{
  "answer": "This season, Ryan Rollins has assisted Giannis Antetokounmpo the most, with 44 assists leading directly to Giannis made shots."
}
```
