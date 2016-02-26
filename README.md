# spotify-data-challenge
Inferring new information from anonymized user listening data.

## How to run
When loading in the **SparkContext** textFiles:
```python
sc.textFile('s3://aws-logs-308764694576-us-west-2/end_song_sample.csv')
```
You need to change the location to either an *S3* bucket on *AWS*, local directory or *HDFS* directory.

**Run command:**
```bash
spark-submit SpotifyData.py
```
