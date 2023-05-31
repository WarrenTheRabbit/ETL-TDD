# 1 - AccessDeniedException
## Error Message
> Exception in User Class: amzn.shaded.com.amazonaws.services.glue.model.AccessDeniedException : User: arn:aws:sts::618572314333:assumed-role/data-quality-lf/GlueJobRunnerSession is not authorized to perform: glue:GetDataQualityRuleRecommendationRun on resource: arn:aws:glue:ap-southeast-2:618572314333:dataQualityRuleset/* because no identity-based policy allows the glue:GetDataQualityRuleRecommendationRun action (Service: AWSGlue; Status Code: 400; Error Code: AccessDeniedException; Request ID: d54104a5-e108-445f-abac-a9b0fd532ccb; Proxy: null)

An access denied exception.

## Version at time of error.
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueRuleRecommendationRunActions",
            "Effect": "Allow",
            "Action": [
                "glue:GetDataQualityRuleRecommendationRun",
                "glue:PublishDataQuality",
                "glue:CreateDataQualityRuleset"
            ],
            "Resource": "arn:aws:glue:us-east-1:618572314333:dataQualityRuleset/*"
        },
        {
            "Sid": "AllowS3GetObjectToRunRuleRecommendationTask",
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": "arn:aws:s3:::*"
        }
    ]
}
```
## Action Taken
Changed region from `us-east-1` to `ap-southeast-2`.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueRuleRecommendationRunActions",
            "Effect": "Allow",
            "Action": [
                "glue:GetDataQualityRuleRecommendationRun",
                "glue:PublishDataQuality",
                "glue:CreateDataQualityRuleset"
            ],
            "Resource": "arn:aws:glue:us-east-1:618572314333:dataQualityRuleset/*"
        },
        {
            "Sid": "AllowS3GetObjectToRunRuleRecommendationTask",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::project-lf-wm"
        }
    ]
}
```

# 2 - AccessDeniedException

## Error Message
> Exception in User Class: amzn.shaded.com.amazonaws.services.glue.model.AccessDeniedException : User: arn:aws:sts::618572314333:assumed-role/data-quality-lf/GlueJobRunnerSession is not authorized to perform: glue:GetTable on resource: arn:aws:glue:ap-southeast-2:618572314333:catalog because no identity-based policy allows the glue:GetTable action (Service: AWSGlue; Status Code: 400; Error Code: AccessDeniedException; Request ID: 5e58b8da-c4bf-41c8-9beb-2a8ac43ace86; Proxy: null)

An access denied exception.

## Version at time of error.
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueRuleRecommendationRunActions",
            "Effect": "Allow",
            "Action": [
                "glue:GetDataQualityRuleRecommendationRun",
                "glue:PublishDataQuality",
                "glue:CreateDataQualityRuleset"
            ],
            "Resource": "arn:aws:glue:us-east-1:618572314333:dataQualityRuleset/*"
        },
        {
            "Sid": "AllowS3GetObjectToRunRuleRecommendationTask",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::project-lf-wm"
        }
    ]
}
```
## Fixed version?
Added `glue:GetTable` action for `catalog` resource.
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueRuleRecommendationRunActions",
            "Effect": "Allow",
            "Action": [
                "glue:GetDataQualityRuleRecommendationRun",
                "glue:PublishDataQuality",
                "glue:CreateDataQualityRuleset"
            ],
            "Resource": "arn:aws:glue:us-east-1:618572314333:dataQualityRuleset/*"
        },
        {
            "Sid": "AllowS3GetObjectToRunRuleRecommendationTask",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::project-lf-wm"
        },
        {
            "Sid": "AllowGlueGetTable",
            "Effect": "Allow",
            "Action": [
                "glue:GetTable"
            ],
            "Resource": "arn:aws:glue:ap-southeast-2:618572314333:catalog"
        }
    ]
}
```

# 3 

## Error Message
> LAUNCH ERROR | Error downloading from S3 for bucket: aws-glue-ml-data-quality-assets-ap-southeast-2, key: jars/aws-glue-ml-data-quality-etl.jar.Access Denied (Service: Amazon S3; Status Code: 403; Please refer logs for details.

*LAUNCH ERROR | Error downloading from S3 . . . Access denied.*

## Version at time of error.
Somehow 
```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowGlueRuleRecommendationRunActions",
			"Effect": "Allow",
			"Action": [
				"glue:GetDataQualityRuleRecommendationRun",
				"glue:PublishDataQuality",
				"glue:CreateDataQualityRuleset"
			],
			"Resource": "arn:aws:glue:us-east-1:618572314333:dataQualityRuleset/*"
		},
		{
			"Sid": "AllowS3GetObjectToRunRuleRecommendationTask",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:PutObject"
			],
			"Resource": "arn:aws:s3:::project-lf-wm"
		},
		{
			"Sid": "AllowGlueGetTable",
			"Effect": "Allow",
			"Action": [
				"glue:GetTable"
			],
			"Resource": "arn:aws:glue:ap-southeast-2:618572314333:catalog"
		}
	]
}
```
## Fixed version?

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueRuleRecommendationRunActions",
            "Effect": "Allow",
            "Action": [
                "glue:GetDataQualityRuleRecommendationRun",
                "glue:PublishDataQuality",
                "glue:CreateDataQualityRuleset"
            ],
            "Resource": "arn:aws:glue:us-east-1:618572314333:dataQualityRuleset/*"
        },
        {
            "Sid": "AllowGlueGetTable",
            "Effect": "Allow",
            "Action": [
                "glue:GetTable"
            ],
            "Resource": "arn:aws:glue:ap-southeast-2:618572314333:catalog"
        },
        {
            "Sid": "AllowS3GetObjectToRunRuleRecommendationTask",
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": "arn:aws:s3:::*"
        }
    ]
}
```

# 4 

## Error Message
> Exception in User Class: amzn.shaded.com.amazonaws.services.glue.model.AccessDeniedException : User: arn:aws:sts::618572314333:assumed-role/data-quality-lf/GlueJobRunnerSession is not authorized to perform: glue:GetDataQualityRuleRecommendationRun on resource: arn:aws:glue:ap-southeast-2:618572314333:dataQualityRuleset/* because no identity-based policy allows the glue:GetDataQualityRuleRecommendationRun action (Service: AWSGlue; Status Code: 400; Error Code: AccessDeniedException; Request ID: 3f564955-1a9a-4653-8020-8f3e6fd1cb09; Proxy: null)

## Lesson Learned
Check the region on all resource arns after receiving an error. It is likely there are others that need to be changed too.
## Version at time of error.
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueRuleRecommendationRunActions",
            "Effect": "Allow",
            "Action": [
                "glue:GetDataQualityRuleRecommendationRun",
                "glue:PublishDataQuality",
                "glue:CreateDataQualityRuleset"
            ],
            "Resource": "arn:aws:glue:us-east-1:618572314333:dataQualityRuleset/*"
        },
        {
            "Sid": "AllowGlueGetTable",
            "Effect": "Allow",
            "Action": [
                "glue:GetTable"
            ],
            "Resource": "arn:aws:glue:ap-southeast-2:618572314333:catalog"
        },
        {
            "Sid": "AllowS3GetObjectToRunRuleRecommendationTask",
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": "arn:aws:s3:::*"
        }
    ]
}
```
## Fixed version?

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueRuleRecommendationRunActions",
            "Effect": "Allow",
            "Action": [
                "glue:GetDataQualityRuleRecommendationRun",
                "glue:PublishDataQuality",
                "glue:CreateDataQualityRuleset"
            ],
            "Resource": "arn:aws:glue:ap-southeast-2:618572314333:dataQualityRuleset/*"
        },
        {
            "Sid": "AllowGlueGetTable",
            "Effect": "Allow",
            "Action": [
                "glue:GetTable"
            ],
            "Resource": "arn:aws:glue:ap-southeast-2:618572314333:catalog"
        },
        {
            "Sid": "AllowS3GetObjectToRunRuleRecommendationTask",
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": "arn:aws:s3:::*"
        }
    ]
}
```


# 5 

## Error Message
> Exception in User Class: amzn.shaded.com.amazonaws.services.glue.model.AccessDeniedException : User: arn:aws:sts::618572314333:assumed-role/data-quality-lf/GlueJobRunnerSession is not authorized to perform: glue:GetTable on resource: arn:aws:glue:ap-southeast-2:618572314333:database/lf-release2 because no identity-based policy allows the glue:GetTable action (Service: AWSGlue; Status Code: 400; Error Code: AccessDeniedException; Request ID: e6fd8412-a60a-443d-a6b2-15ce67fb0ab8; Proxy: null)

An access denied exception.

## Version at time of error.
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueRuleRecommendationRunActions",
            "Effect": "Allow",
            "Action": [
                "glue:GetDataQualityRuleRecommendationRun",
                "glue:PublishDataQuality",
                "glue:CreateDataQualityRuleset"
            ],
            "Resource": "arn:aws:glue:ap-southeast-2:618572314333:dataQualityRuleset/*"
        },
        {
            "Sid": "AllowGlueGetTable",
            "Effect": "Allow",
            "Action": [
                "glue:GetTable"
            ],
            "Resource": "arn:aws:glue:ap-southeast-2:618572314333:catalog"
        },
        {
            "Sid": "AllowS3GetObjectToRunRuleRecommendationTask",
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": "arn:aws:s3:::*"
        }
    ]
}
```
## Fixed version?

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGlueRuleRecommendationRunActions",
            "Effect": "Allow",
            "Action": [
                "glue:GetDataQualityRuleRecommendationRun",
                "glue:PublishDataQuality",
                "glue:CreateDataQualityRuleset"
            ],
            "Resource": "arn:aws:glue:ap-southeast-2:618572314333:dataQualityRuleset/*"
        },
        {
            "Sid": "AllowGlueGetTable",
            "Effect": "Allow",
            "Action": [
                "glue:GetTable"
            ],
            "Resource": "arn:aws:glue:ap-southeast-2:618572314333:*"
        },
        {
            "Sid": "AllowS3GetObjectToRunRuleRecommendationTask",
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": "arn:aws:s3:::*"
        }
    ]
}
```

# 6

## Error Message
> 

An access denied exception.

## Version at time of error.
```json

```
## Fixed version?

```json

```