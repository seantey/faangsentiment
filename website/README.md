## FAANG Sentiment Website

1. The website is hosted on AWS S3 and distributed with AWS CloudFront.
2. To update website code, just use the awscli tool with a command such as `aws s3 sync ./website/ s3://bucket-name` then remember to invalidate the CloudFront cache for the update files so that the changes are quickly reflected, otherwise CloudFront will keep on serving whatever is on the cache of the edge nodes. 
3. Invalidating all files with commands such as `aws cloudfront create-invalidation --distribution-id <DistributionID> --paths "/*"` is not recommended because cache invalidation is charged per path, so depending on how many files are there in the folder, it may become expensive. However, in most cases it should still be pretty cheap.
4. The website pulls data from a DynamoDB table through an AWS API Gateway endpoint which invokes an AWS Lambda function to get the latest sentiment analysis scores.
5. The website code is a modified version of the [Phantom](https://html5up.net/phantom) template by [HTML5UP](https://html5up.net/) which is licensed under the Creative Commons Attribution 3.0 License.