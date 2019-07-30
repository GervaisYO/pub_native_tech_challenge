# PubNative tech challenge

## Business Model
When advertisement bannners are displayed to users in mobile application(app_id) in country(country code) from advertiser(advertiser_id) impression events was recorded and stored. After that user could click to banner and in this case click event is recorded.

## Problem to solve
Given a list of files containing impressions (**app_id**, **country_code**, **id**, **advertiser_id**) and clicks (**impression_id**, **revenue**), the application has to generate metrics and recommendations grouped by the dimensions **app_id** and **country_code**

## Solution
The amount of data to process can be really huge and impossible to make it fit in memory to compute the required metrics and recommendations. To mitigate that problem, i came up with a solution that keep the memory footprint low but at the same time tries to be fast.
  - create partitions of data by relevant dimensions (**app_id** and **country_code**): before starting to compute metrics and recommendations the application needs to partition data by **app_id** and **country_code**. This allow subsequent steps to only load/read into memory data needed to compute metrics
  - use akka streams: the solution is using akka streams to read and write data, because it is not that hard to increase the number of concurrent processes when working with akka streams.

The code base is made of the following packages:
- domain package: Here are all domain related classes defined. Impression and Click
- data writer/loader: Here are all classes responsible to load/write data from/to the file system defined.
- metrics: Here is the class responsible to generate metrics. This class depends on functionalities offered by domain, writer and loader classes to achieve the computation of metrics.
- recommendation: Here is the class responsible to generate recommendations. This class depends on functionalities offered by domain, writer and loader classes to achieve the computation of recommendations.

## How to run the application
To run the application one must execute the following steps:
- clone the repository to your local machine
- navigate to the root of the project
- execute 'sbt assembly': this will generate a jar file in the folder **pub-native-jar**. The repository already contains a jar file.
- navigate to the folder where the jar file was generated and run the application: 
```
java -jar pub_native_tech_challenge.jar --impressions-dir XXX --clicks-dir XXX --output-dir XXX --streams-parallelism 50 --source-group-size 1000
``` 

The application accepts three mandatory parameters and three optional parameters:
- impressions-dir (*mandatory*): this is the directory where the impressions files are stored
- clicks-dir (*mandatory*): this is the directory where the clicks files are stored
- output-dir (*mandatory*): this is the directory where the output files (metrics.json and recommendations.json) will be stored 
- streams-parallelism (*default value is (numOfCores * 2 - 2)*): this parameter controls the parallelism level for the stream processing
- source-group-size (*default value is 1000*): this parameter controls the number of items to group before writing them to the proper partition
- top-advertiser-count (*default value is 5*): this parameter controls the number of top advertiser per **app_id** and **country_code** to generate for each recommendation

## Assumptions made during the development process
- Number of advertiser per app_id and country_code is less then Int.MAX_VALUE

## Could be done better
- Error handling
