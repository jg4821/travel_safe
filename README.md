# Travel Safe

## Business Case
- Travel agency needs to assure present or near-present destination safety before planning and marketing their itinerary. 
- Safety score based on events happened in different areas around the globe gives direct and general indication of area safety level. 

## Idea
- Incorporate information from GDELT events, eventmentions, global knowledge graph to provide safety score of an area. 
- Allow travel agency to look for past 3 month daily safety score of an area, and give top 3 negative-impact events when looking into a certain day. 

## Dataset
- GDELT dataset: ~ 2.5TB per year

## Engineering Challenge
- Big data joins and query

## Tech Stack
- S3 => Spark => PostgreSQL => Flask
