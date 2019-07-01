# Travel Informed

## Background
In 2018, the global Travel & Tourism sector grew at 3.9% to contribute a record $8.8 trillion and 319 million jobs to the world economy, and generated 10.4% of all global economic activity, according to the World Travel & Tourism Council’s (WTTC) annual research. The safety level of a country directly affects the travel business. For instance, in Colombia, decrease in crime level saw 10.7% increase of travelers. Conversely, in Venezuela, increase of crimes led to 7.2% drop of travelers. Travel agents, as well as travelers, must assure destination safety before making recommendations to clients or planning their next wonderful trip. 

## Project Overview
This project provides a platform for travel agents and travelers to get up-to-date information on global cities. It incorporates real-time news information from Global Database of Events, Language and Tone (GDELT), aggregates every news mention about a event to provide a direct and inclusive representation of safety level of a city. 

## Dataset
[GDELT dataset](https://www.gdeltproject.org/data.html#rawdatafiles): ~ 2.5TB per year

## Tech Stack
![tech_stack](https://user-images.githubusercontent.com/32504091/60420177-eb370b00-9b9b-11e9-9ae1-bc683f39efcc.png)

## Engineering Challenge
- Large table joins with O(n<sup>3</sup>) complexity
- New coming and backfill data handling
