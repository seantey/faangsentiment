
### EMR Script notes:
1. Remember to upload the latest spark script to S3. In the future maybe use AWS CodePipeline to automatically update the script with a github web hook.
2. Make sure to use at least m5.xlarge instances with 16GB of RAM as 8GB doesn't seem to be enough.
3. Even though `maximizeResourceAllocation` was set to `true`, there should be another test to check everything is really fully utilized and some parameters might need to be manually set as noted by: https://stackoverflow.com/questions/34003759/

### DynamoDB notes:

Make sure that there are the following 3 tables in the DynamoDB:
1. news_data
    * Primary Key: analysis_window -> string
    * Sort Key: symb_counter_source -> string
2. news_results
    * Primary Key: analysis_window -> string
    * Sort Key: t_symb -> string
    * Global Secondary Index: 
        * analysis_date_index -> string
        * Optional: Read Capacity = 5 units, Write capacity = 1 units. (Can probably be less to save a little bit more money)
3. results_json
    * analysis_window -> string

(Note for that the tables above, default settings work fine, but might want to tweak read and write capacity depending on workload to save a little bit more money. Defaults should fall under free tier usage for first year.)

### Lambda / Website JSON notes:
1. Make sure to run the news collection api for at least 7 days before starting the Ariflow DAGs so that there is enough data for the website which is processed by the results_to_json lambda function.