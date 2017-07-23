# Google-Finance-Stock-Data-Analysis
Developed a high performance data processing platform using Apache Kafka, Apache Cassandra, and Apache Spark to analyze stock price and related stock tweets sentiment.

## tweets_producer.py
### This program is used to retrieve stocks data from Google Finance and tweets associated with the stocks through tweepy API. The stock symbol is from user's input. The supported RESTFUL request type is POST and DELETE, so user can retrive stock or delete stock.
### After fetching stocks data and tweets the program will send the stock data to Kafka using their respective topic.

## worker.py
### This is the major program that is used to process the streaming data sent via Kafka. There are two incoming data stream, stocks and tweets. Two direct spark stream are created to consume the Kafka data. The stock stream is processed to calculate the average stock price and the daily stock trend, and the tweet stream is processed to get the twitter user and their tweets (sentiment) towards the stock. Finally the results are stored into Cassandra.

# Steps to run the program:
### Start stock and tweet producer:
~~~   
export ENV_CONFIG_FILE=/<path>/config.cfg
~~~
```
python tweets_producer.py
```

### Start spark streaming process:
```
spark-submit  worker.py
```

# Output: 
### Producer that produces stock data and tweets:
![kafka_producer](https://github.com/hpnhxxwn/cs502-1702/blob/master/Isabella/assignment6/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-05-11%20%E4%B8%8B%E5%8D%8810.37.44.png?raw=true)

### Consumer that consumes tweet data:
![kafka_consumer1.1](https://github.com/hpnhxxwn/cs502-1702/blob/master/Isabella/assignment6/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-05-11%20%E4%B8%8B%E5%8D%8810.36.06.png?raw=true)
![kafka_consumer1.2](https://github.com/hpnhxxwn/cs502-1702/blob/master/Isabella/assignment6/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-05-11%20%E4%B8%8B%E5%8D%8810.36.15.png?raw=true)

### Consumer that consumes stock trend data:
![kafka_consumer2](https://github.com/hpnhxxwn/cs502-1702/blob/master/Isabella/assignment6/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-05-11%20%E4%B8%8B%E5%8D%8810.36.42.png?raw=true)

### Consumer that consumes stock average price:
![kafka_consumer3](https://github.com/hpnhxxwn/cs502-1702/blob/master/Isabella/assignment6/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-05-11%20%E4%B8%8B%E5%8D%8811.00.57.png?raw=true)

### Zipkin output:
![zipkin_UI_1](https://github.com/hpnhxxwn/cs502-1702/blob/master/Isabella/assignment6/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-05-11%20%E4%B8%8B%E5%8D%8811.07.40.png?raw=true)
![zipkin_UI_1](https://github.com/hpnhxxwn/cs502-1702/blob/master/Isabella/assignment6/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-05-11%20%E4%B8%8B%E5%8D%8811.07.51.png?raw=true)

# Cassandra output:
### Stock table:
~~~
 stock_symbol | trade_time                      | trade_price
--------------+---------------------------------+-------------
         SNAP | 2017-05-10 09:04:45.000000-0700 |       22.98
         SNAP | 2017-05-11 09:00:10.000000-0700 |       18.05
         AAPL | 2017-05-11 09:00:01.000000-0700 |      153.95
         GOOG | 2017-05-10 09:00:06.000000-0700 |   928.78003
         GOOG | 2017-05-11 09:00:01.000000-0700 |   930.59998
~~~

### Stock average price table:
~~~
 stock_symbol | record_time      | average_price
--------------+------------------+---------------
         SNAP | 1494484751321514 |         18.05
	 AAPL | 1494567290257799 |        153.95
         GOOG | 1494477596796821 |     928.78003
~~~

### Stock trend table:
~~~
 stock_symbol | trade_time                      | stock_price_difference | time_elapsed | trend
--------------+---------------------------------+------------------------+--------------+-------------
         SNAP | 2017-05-11 16:00:10.000000-0700 |                  1e-06 |            0 | not changed
         AAPL | 2017-05-11 16:00:01.000000-0700 |                  3e-06 |            0 | not changed
         GOOG | 2017-05-11 16:00:01.000000-0700 |                1.81997 |            1 |    up
~~~

### Stock tweet table:
~~~
 stock_symbol | user_id            | tweet                                                                                                                                                      | tweet_create_time               | user_screen_name
--------------+--------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------+------------------
         SNAP |          109053346 |                Oh, Snap. Parent company of Snapchat posts staggering loss in its first earnings report since going public in Marc… https://t.co/8VzrGMhacf | 2017-05-11 05:48:16.000000-0700 |         Mr_AlVil
         SNAP |         1571714749 |                                                                                                                                      New snap update sucks | 2017-05-11 05:55:30.000000-0700 |    bobbyjones_23
         SNAP |          186019376 |                               RIP Snap: Investors pound stock down 25% after big earnings miss and weak user growth https://t.co/kKxKWmWr7r by VentureBeat | 2017-05-11 06:02:11.000000-0700 |      ClubSignage
         SNAP |           20402945 |                                         Snap CEO Evan Spiegel and his co-founder just lost more than $1 billion each https://t.co/quha7CYPcQ via @cnbctech | 2017-05-11 05:53:04.000000-0700 |             CNBC
         SNAP |         2359362634 |                    #VentureBeat: RIP Snap: Investors pound stock down 25% after big earnings miss and weak user growth https://t.co/QG9zruD8Dq lordcedric… | 2017-05-11 06:02:19.000000-0700 |   lordcedric_com
         SNAP |         2373206354 |                                                                                             is apologize for my annoying snap story tonight but C BREEZY😻 | 2017-05-11 06:02:20.000000-0700 |  Regan_Russell14
         SNAP |          252550136 |                             When today made you snap back and realise who the fuck you are...a fuckin bomb ass African Queen bihhh https://t.co/mz1XwSei6h | 2017-05-11 06:44:42.000000-0700 |      MelaninGawd
         SNAP |         2525805978 |                                  Rand Swiss morning note now out!\n\nWe look at $SNAP, $JSEEQU, $JSEILU and a whole lot more...\n\nhttps://t.co/sGs8E9oiyq | 2017-05-11 06:44:54.000000-0700 |        RandSwiss
         SNAP |         2761498751 |                                                                                                                                  This snap update annoying | 2017-05-11 06:02:17.000000-0700 |  itsAlexLamadrid
         SNAP |          284877526 |                                                              when snap says @BallinBallard12 is typing but never sends anything 😳😳😳😳😳😳😳😳😳😂😂😂😂 | 2017-05-11 06:44:54.000000-0700 |       MiniMartar
         SNAP |         3061152311 |                                                                                                        I actually love the video I posted to my snap story | 2017-05-11 06:02:19.000000-0700 |      Bohr_EDream
         SNAP |          344594172 |                                                   Snap Blows First Earnings—But That’s Not the Whole Story https://t.co/uByzRQfExd https://t.co/YIkgK7Dzh6 | 2017-05-11 06:02:11.000000-0700 |         Jules_T_
         SNAP |         3673062019 |                         business: Snap tumbled Wednesday, after trailing estimates for new users and sales https://t.co/yAPsDyIXWP https://t.co/uQXDbmXKEX | 2017-05-11 06:02:09.000000-0700 |  serge_poznanski
         SNAP |           39383125 |                                                                                                                                                  Oh $snap! | 2017-05-11 05:55:32.000000-0700 |         acmurthy
         SNAP |         4100556028 |                                           #ukog shares 6.5m being rumored to be placed for 1.05p a share #twtr growth shows #snap overvalued and stagnated | 2017-05-11 05:55:25.000000-0700 |         PSVR2016
         SNAP |         4552172843 |                                                                                                  be w/ my dog on the snap all day any way. life is good 💛 | 2017-05-11 06:02:18.000000-0700 |       tiinytee__
         SNAP |         4843262945 |                                                      so either his name was always scar or you break into my house and snap my fucking neck it’s your call | 2017-05-11 06:44:44.000000-0700 |         tori_txt
         SNAP |          557943742 |                                                                                       Snap shares slide as growth slows - BBC News https://t.co/sILr0b0LxE | 2017-05-11 05:55:30.000000-0700 |      BeaumontBee
         SNAP |           56942347 |                                                                                                                                              Snap me )))): | 2017-05-11 05:54:44.000000-0700 |      Jeff_Tatman
         SNAP |          587155302 | AndWeCare WHY??\nTheyWERE&amp;ARE\n#PaperBillionaires\nSoWhat?\n:@evanspiegel\n@Snap\n@Snapchat\n@Spectacles\n\nMission&amp;Cause… https://t.co/d0MxNYID7t | 2017-05-11 06:04:51.000000-0700 |      NASCHartInc
         SNAP | 702422966747983873 |                                                  Snap Shares Tumble After First Post-IPO Earnings Miss Expectations https://t.co/DmOB4MAU7S via @TheStreet | 2017-05-11 06:44:38.000000-0700 |      tst_chennai
         SNAP | 721582027728793601 |            SEO friendly auto post for #WordPress.com &amp; self-hosted WordPress https://t.co/7ytIQsreTE | SEO Tips | SEO blog | Snap Inc. loses $2.2B in… | 2017-05-11 06:02:11.000000-0700 |   davidy_filemon
         SNAP |          722011406 |                                                 Snapchat shares tumble as results miss Wall Street targets https://t.co/JiCrIrHrby https://t.co/m6JGa1kaAT | 2017-05-11 05:51:31.000000-0700 |      SblendedKT1
         SNAP | 774341031143870464 |               To both @lamb_chloe3 @chlomoney is it possible for me to meet @courtneyoc13 God she is so fricken pretty and awesome from what I see on snap | 2017-05-11 05:51:33.000000-0700 |   teambigwill518
         SNAP | 795624239541915649 |                        Trade with the best indicators https://t.co/lEGQ2cVnV5 📈 $IMO $ADM $FIS $SNAP $ROST $EIX $DFS $CAH $HCN $K https://t.co/IFuxj0Tyra | 2017-05-11 05:55:23.000000-0700 |      MNLST_TRDNG
         SNAP | 815274158988914688 |                                                                                                        $SNAP via /r/wallstreetbets https://t.co/rFyKX50WJI | 2017-05-11 05:48:15.000000-0700 |    Penny_Stockin
         SNAP | 834132795710660613 |                                                     (Snapchat lost $2.2 billion last quarter) \n https://t.co/rZxUO6Yqqk\nSnap;... https://t.co/xroC9A8Fa3 | 2017-05-11 05:53:03.000000-0700 |  gleefulwhispers
         SNAP | 850439088809287680 |                                                                                                                        I fw snap🥀 https://t.co/jRj5d1NPbY | 2017-05-11 06:44:54.000000-0700 |       xokaiileee
~~~

# Visualization of Stock trend, price graph, and tweets associate with the stock
### How to run:
### We first launch web server

```
python redis_publisher.py stock tweet test localhost:9092 localhost 6379 data tweet trend
node index.js --port=3000 --redis_host=localhost --redis_port=6379 --subscribe_channel=data --subscribe_tweet_channel=tweet --subscribe_trend_channel=trend
```

### The data is already processed by worker. We can visualize stock trend within a few days (time elapsed) and tweets along with stock price.

!['nodejs_1'](https://github.com/hpnhxxwn/cs502-1702/blob/master/Isabella/assignment8/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-05-19%20%E4%B8%8B%E5%8D%886.54.07.png?raw=true)

!['nodejs_2'](https://github.com/hpnhxxwn/cs502-1702/blob/master/Isabella/assignment8/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202017-05-19%20%E4%B8%8B%E5%8D%886.54.21.png?raw=true)
