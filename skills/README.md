# SKILL for Agent

## ONE MORE THING

Please note that now we support the [`SKILLS`](https://github.com/agentskills/agentskills). You can check the details in [SKILL.md](skills/newsfeed/SKILL.md) and [SKILL.zh.md](skills/newsfeed/SKILL.zh.md).

Here is an example of how to use the SKILL with OpenClaw (GLM-4.7) with querying news events related to China and the UK from GDELT Event database:

```
User: Help me find news reports related to China and the UK from the GDELT Event since 2026.

OpenClaw [Feb 14, 2026 at 17:40:39]: 
I am downloading all event data from the GDELT database from January to February 14, 2026. This query involves files of 4320 time slices, and the download and processing will take some time (estimated 15-20 minutes).


OpenClaw [Feb 14, 2026 at 17:51:51]: 
Query complete!
...📊 Summary of China-UK Related News Events

Total Events: 1,599 China-UK related events
Data Size: Full dataset 440MB, filtered down to 672KB
Time Range: January 31 - February 14, 2026

📅 Date Distribution (Partial)
| Date       | Event Count      |
| ---------- | ---------------- |
| 2026-01-01 | 2 events         |
| 2026-01-02 | 8 events         |
| 2026-01-05 | 19 events        |
| 2026-01-10 | 23 events        |
| 2026-01-12 | 38 events (peak) |
🎯 Top Event Types (Top 5)

1. Code 42: 252 events
2. Code 43: 217 events
3. Code 36: 181 events
4. Code 40: 92 events
5. Code 20: 79 events
💭 Sentiment Analysis

• Average Tone Score: -1.27 (overall slightly negative)
• Goldstein Scale: Average 2.03 (moderate impact level)
• Most Negative Event: Tone score -10.42
• **Most Positive Event**: Tone score 12.56

🏛️ Main Actors

China Side:

• CHINA: 482 occurrences
• CHINESE: 159 occurrences
• BEIJING: 108 occurrences
UK Side:

• UNITED KINGDOM: 449 occurrences
• BRITISH: 176 occurrences
• LONDON: 59 occurrences

📁 File Locations

• Filtered Data: /tmp/china_uk_events.csv (1,599 events)
• Full Raw Data: /tmp/events_full.csv (440MB)
Would you like me to dig deeper? For example:

• Analyze events from specific dates
• Download full text of certain articles
• Look at specific event types in detail
``` 

Another example of the SKILL with Claude Code: 

<center>
    <img src="https://32cf906.webp.li/2026/02/example_with_skills.png" alt="Example of using SKILL with Claude Code" width="600"/>
</center>