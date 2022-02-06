from newsfeed.news.database.events import *
from newsfeed.utils import fulltext as ft

events = EventV2(start_date="2022-02-06-00-00-00", end_date="2022-02-06-12-00-00")
events_list = events.query_nowtime()


mentions = EventV2(start_date="2022-02-06-00-00-00", end_date="2022-02-06-12-00-00", table="mentions")
mentions_list = mentions.query_nowtime()

intersection_list = list(set(mentions_list["GLOBALEVENTID"]).intersection(set(events_list["GLOBALEVENTID"])))


'''
NumSources. (integer) This is the total number of information sources containing one or more 
mentions of this event during the 15 minute update in which it was first seen. This can be used 
as a method of assessing the “importance” of an event: the more discussion of that event, the 
more likely it is to be significant.

'''
events_list["NumSources"]

news_fulltext = []
news_list = events_list[(events_list["Actor1CountryCode"] == "CHN") & (events_list["Actor2CountryCode"]=="CHN")]
for i in range(0, len(news_list)):
    news_fulltext.append(ft.download(url=list(news_list["SOURCEURL"])[i]))

