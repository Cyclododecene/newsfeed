---
name: newsfeed
description: Comprehensive news aggregator that fetches, filter, and deeply analyze real-time/historical content from the GDELT Project (Global Database of Events, Language, and Tone) databases for news articles, events, and entity data. Use when user are working with global news, historical news report, global event extraction, or GDELT database access.
version: 0.1.7.3
---

# Newsfeed - GDELT Data Access

## Overview 

Fetch real-time/historical news from the **GDELT Project** (Global Database of Events, Language, and Tone) through CLI tools and Python APIs.

## When to Use

Use this skill when you need to:
- Analyse global news by keywords, themes, actors, or locations
- Query global news articles by date range
- Access GDELT Events, Mentions, and Global Knowledge Graph (GKG) databases
- Perform timeline analysis of news events
- Download full-text articles from URLs
- Analyze global events, themes, and relationships

## Prerequisites

To use this skill, set up your environment with the following commands:

1. Create and activate a new or existing Python environment (Python 3.10+ recommended)
2. Install `newsfeed` package:
   ```bash
   pip install newsfeed==0.1.7.3
   ```

Or just download from source code:
   ```bash
    git clone https://github.com/Cyclododecene/newsfeed.git
    cd newsfeed
    git checkout dev
    pip install -e .
   ```

## Common Patterns

1. Scenario 1: Search news by keywords/themes, e.g. `python script/script.py --scenario search-topics --days 1 --keywords "AI,GPT,LLM"` Query news about AI and LLM in the past 1 day and summarize the top themes, locations, and actors.
2. Scenario 2: View bialteral events between two countries, e.g. `python script/script.py --scenario bilateral-events --days 7 --country1 USA --country2 CHN` Query events between USA and China in the past 7 days and summarize the event types, sentiment, and main actors.
3. Scenario 3: View event media mentions and sentiment: e.g. `python skills/script.py --scenario event-mentions --event-id 12345` Query media mentions of event with ID 12345 and summarize the sentiment and source distribution.

## Tools for Scenario (Recommended)

```python
# Senario 1: Search news by keywords/themes
 
python script/script.py --scenario search-topics --days 1 --keywords "{keywords}"

# Scenario 2: View bialteral events between two countries
python script/script.py --scenario bilateral-events --days 7 --country1 {country1} --country2 {country2}

# Scenario 3: View event media mentions and sentiment
python script/script.py --scenario event-mentions --event-id {event_id}
```


## Additional Capabilities
- If the user is requiring for additional data analysis: You can use the Python API to perform more customized analysis, such as sentiment analysis, network analysis, or geospatial analysis. You can check the details in [newsfeed.md](./references/newsfeed.md) for more information on how to use the Python API.
- If the user has specific question about the GDELT CAMEO codes, You can refer to the official GDELT documentation: 
   - CAMEO type: https://www.gdeltproject.org/data/lookups/CAMEO.type.txt
   - CAMEO religion: https://www.gdeltproject.org/data/lookups/CAMEO.religion.txt
   - CAMEO known group: https://www.gdeltproject.org/data/lookups/CAMEO.knowngroup.txt
   - CAMEO goldstein scale: https://www.gdeltproject.org/data/lookups/CAMEO.goldsteinscale.txt
   - CAMEO event code: https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt
   - CAMEO ethnic code: https://www.gdeltproject.org/data/lookups/CAMEO.ethnic.txt
   - CAMEO country code: https://www.gdeltproject.org/data/lookups/CAMEO.country.txt