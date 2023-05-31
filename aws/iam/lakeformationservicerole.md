```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::databucket-us-east-1-1273234499067545/data/*",
                "arn:aws:s3:::databucket-us-east-1-1273234499067545/results/*"
            ],
            "Effect": "Allow"
        },
        {
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::databucket-us-east-1-1273234499067545"
            ],
            "Effect": "Allow"
        }
    ]
}
```