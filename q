AWS()                                                                    AWS()



[1mNAME[0m
       aws -

[1mDESCRIPTION[0m
       The  AWS  Command  Line  Interface is a unified tool to manage your AWS
       services.

[1mSYNOPSIS[0m
          aws [options] <command> <subcommand> [parameters]

       Use [4maws[24m [4mcommand[24m [4mhelp[24m for information on a  specific  command.  Use  [4maws[0m
       [4mhelp[24m  [4mtopics[24m  to view a list of available help topics. The synopsis for
       each command shows its parameters and their usage. Optional  parameters
       are shown in square brackets.

[1mGLOBAL OPTIONS[0m
       [1m--debug [22m(boolean)

       Turn on debug logging.

       [1m--endpoint-url [22m(string)

       Override command's default URL with the given URL.

       [1m--no-verify-ssl [22m(boolean)

       By  default, the AWS CLI uses SSL when communicating with AWS services.
       For each SSL connection, the AWS CLI will verify SSL certificates. This
       option overrides the default behavior of verifying SSL certificates.

       [1m--no-paginate [22m(boolean)

       Disable automatic pagination.

       [1m--output [22m(string)

       The formatting style for command output.

       +o json

       +o text

       +o table

       +o yaml

       +o yaml-stream

       [1m--query [22m(string)

       A JMESPath query to use in filtering the response data.

       [1m--profile [22m(string)

       Use a specific profile from your credential file.

       [1m--region [22m(string)

       The region to use. Overrides config/env settings.

       [1m--version [22m(string)

       Display the version of this tool.

       [1m--color [22m(string)

       Turn on/off color output.

       +o on

       +o off

       +o auto

       [1m--no-sign-request [22m(boolean)

       Do  not  sign requests. Credentials will not be loaded if this argument
       is provided.

       [1m--ca-bundle [22m(string)

       The CA certificate bundle to use when verifying SSL certificates. Over-
       rides config/env settings.

       [1m--cli-read-timeout [22m(int)

       The  maximum socket read time in seconds. If the value is set to 0, the
       socket read will be blocking and not timeout. The default value  is  60
       seconds.

       [1m--cli-connect-timeout [22m(int)

       The  maximum  socket connect time in seconds. If the value is set to 0,
       the socket connect will be blocking and not timeout. The default  value
       is 60 seconds.

       [1m--cli-binary-format [22m(string)

       The formatting style to be used for binary blobs. The default format is
       base64. The base64 format expects binary blobs  to  be  provided  as  a
       base64  encoded string. The raw-in-base64-out format preserves compati-
       bility with AWS CLI V1 behavior and binary values must be passed liter-
       ally.  When  providing  contents  from a file that map to a binary blob
       [1mfileb:// [22mwill always be treated as binary and  use  the  file  contents
       directly  regardless  of  the  [1mcli-binary-format  [22msetting.  When  using
       [1mfile:// [22mthe file contents will need to properly formatted for the  con-
       figured [1mcli-binary-format[22m.

       +o base64

       +o raw-in-base64-out

       [1m--no-cli-pager [22m(boolean)

       Disable cli pager for output.

       [1m--cli-auto-prompt [22m(boolean)

       Automatically prompt for CLI input parameters.

       [1m--no-cli-auto-prompt [22m(boolean)

       Disable automatically prompt for CLI input parameters.

[1mAVAILABLE SERVICES[0m
       +o accessanalyzer

       +o account

       +o acm

       +o acm-pca

       +o alexaforbusiness

       +o amp

       +o amplify

       +o amplifybackend

       +o amplifyuibuilder

       +o apigateway

       +o apigatewaymanagementapi

       +o apigatewayv2

       +o appconfig

       +o appconfigdata

       +o appflow

       +o appintegrations

       +o application-autoscaling

       +o application-insights

       +o applicationcostprofiler

       +o appmesh

       +o apprunner

       +o appstream

       +o appsync

       +o arc-zonal-shift

       +o athena

       +o auditmanager

       +o autoscaling

       +o autoscaling-plans

       +o backup

       +o backup-gateway

       +o backupstorage

       +o batch

       +o billingconductor

       +o braket

       +o budgets

       +o ce

       +o chime

       +o chime-sdk-identity

       +o chime-sdk-media-pipelines

       +o chime-sdk-meetings

       +o chime-sdk-messaging

       +o chime-sdk-voice

       +o cleanrooms

       +o cli-dev

       +o cloud9

       +o cloudcontrol

       +o clouddirectory

       +o cloudformation

       +o cloudfront

       +o cloudhsm

       +o cloudhsmv2

       +o cloudsearch

       +o cloudsearchdomain

       +o cloudtrail

       +o cloudtrail-data

       +o cloudwatch

       +o codeartifact

       +o codebuild

       +o codecatalyst

       +o codecommit

       +o codeguru-reviewer

       +o codeguruprofiler

       +o codepipeline

       +o codestar

       +o codestar-connections

       +o codestar-notifications

       +o cognito-identity

       +o cognito-idp

       +o cognito-sync

       +o comprehend

       +o comprehendmedical

       +o compute-optimizer

       +o configservice

       +o configure

       +o connect

       +o connect-contact-lens

       +o connectcampaigns

       +o connectcases

       +o connectparticipant

       +o controltower

       +o cur

       +o customer-profiles

       +o databrew

       +o dataexchange

       +o datapipeline

       +o datasync

       +o dax

       +o ddb

       +o deploy

       +o detective

       +o devicefarm

       +o devops-guru

       +o directconnect

       +o discovery

       +o dlm

       +o dms

       +o docdb

       +o docdb-elastic

       +o drs

       +o ds

       +o dynamodb

       +o dynamodbstreams

       +o ebs

       +o ec2

       +o ec2-instance-connect

       +o ecr

       +o ecr-public

       +o ecs

       +o efs

       +o eks

       +o elastic-inference

       +o elasticache

       +o elasticbeanstalk

       +o elastictranscoder

       +o elb

       +o elbv2

       +o emr

       +o emr-containers

       +o emr-serverless

       +o es

       +o events

       +o evidently

       +o finspace

       +o finspace-data

       +o firehose

       +o fis

       +o fms

       +o forecast

       +o forecastquery

       +o frauddetector

       +o fsx

       +o gamelift

       +o gamesparks

       +o glacier

       +o globalaccelerator

       +o glue

       +o grafana

       +o greengrass

       +o greengrassv2

       +o groundstation

       +o guardduty

       +o health

       +o healthlake

       +o help

       +o history

       +o honeycode

       +o iam

       +o identitystore

       +o imagebuilder

       +o importexport

       +o inspector

       +o inspector2

       +o internetmonitor

       +o iot

       +o iot-data

       +o iot-jobs-data

       +o iot-roborunner

       +o iot1click-devices

       +o iot1click-projects

       +o iotanalytics

       +o iotdeviceadvisor

       +o iotevents

       +o iotevents-data

       +o iotfleethub

       +o iotfleetwise

       +o iotsecuretunneling

       +o iotsitewise

       +o iotthingsgraph

       +o iottwinmaker

       +o iotwireless

       +o ivs

       +o ivs-realtime

       +o ivschat

       +o kafka

       +o kafkaconnect

       +o kendra

       +o kendra-ranking

       +o keyspaces

       +o kinesis

       +o kinesis-video-archived-media

       +o kinesis-video-media

       +o kinesis-video-signaling

       +o kinesis-video-webrtc-storage

       +o kinesisanalytics

       +o kinesisanalyticsv2

       +o kinesisvideo

       +o kms

       +o lakeformation

       +o lambda

       +o lex-models

       +o lex-runtime

       +o lexv2-models

       +o lexv2-runtime

       +o license-manager

       +o license-manager-linux-subscriptions

       +o license-manager-user-subscriptions

       +o lightsail

       +o location

       +o logs

       +o lookoutequipment

       +o lookoutmetrics

       +o lookoutvision

       +o m2

       +o machinelearning

       +o macie

       +o macie2

       +o managedblockchain

       +o marketplace-catalog

       +o marketplace-entitlement

       +o marketplacecommerceanalytics

       +o mediaconnect

       +o mediaconvert

       +o medialive

       +o mediapackage

       +o mediapackage-vod

       +o mediapackagev2

       +o mediastore

       +o mediastore-data

       +o mediatailor

       +o memorydb

       +o meteringmarketplace

       +o mgh

       +o mgn

       +o migration-hub-refactor-spaces

       +o migrationhub-config

       +o migrationhuborchestrator

       +o migrationhubstrategy

       +o mobile

       +o mq

       +o mturk

       +o mwaa

       +o neptune

       +o network-firewall

       +o networkmanager

       +o nimble

       +o oam

       +o omics

       +o opensearch

       +o opensearchserverless

       +o opsworks

       +o opsworks-cm

       +o organizations

       +o osis

       +o outposts

       +o panorama

       +o personalize

       +o personalize-events

       +o personalize-runtime

       +o pi

       +o pinpoint

       +o pinpoint-email

       +o pinpoint-sms-voice

       +o pinpoint-sms-voice-v2

       +o pipes

       +o polly

       +o pricing

       +o privatenetworks

       +o proton

       +o qldb

       +o qldb-session

       +o quicksight

       +o ram

       +o rbin

       +o rds

       +o rds-data

       +o redshift

       +o redshift-data

       +o redshift-serverless

       +o rekognition

       +o resiliencehub

       +o resource-explorer-2

       +o resource-groups

       +o resourcegroupstaggingapi

       +o robomaker

       +o rolesanywhere

       +o route53

       +o route53-recovery-cluster

       +o route53-recovery-control-config

       +o route53-recovery-readiness

       +o route53domains

       +o route53resolver

       +o rum

       +o s3

       +o s3api

       +o s3control

       +o s3outposts

       +o sagemaker

       +o sagemaker-a2i-runtime

       +o sagemaker-edge

       +o sagemaker-featurestore-runtime

       +o sagemaker-geospatial

       +o sagemaker-metrics

       +o sagemaker-runtime

       +o savingsplans

       +o scheduler

       +o schemas

       +o sdb

       +o secretsmanager

       +o securityhub

       +o securitylake

       +o serverlessrepo

       +o service-quotas

       +o servicecatalog

       +o servicecatalog-appregistry

       +o servicediscovery

       +o ses

       +o sesv2

       +o shield

       +o signer

       +o simspaceweaver

       +o sms

       +o snow-device-management

       +o snowball

       +o sns

       +o sqs

       +o ssm

       +o ssm-contacts

       +o ssm-incidents

       +o ssm-sap

       +o sso

       +o sso-admin

       +o sso-oidc

       +o stepfunctions

       +o storagegateway

       +o sts

       +o support

       +o support-app

       +o swf

       +o synthetics

       +o textract

       +o timestream-query

       +o timestream-write

       +o tnb

       +o transcribe

       +o transfer

       +o translate

       +o voice-id

       +o vpc-lattice

       +o waf

       +o waf-regional

       +o wafv2

       +o wellarchitected

       +o wisdom

       +o workdocs

       +o worklink

       +o workmail

       +o workmailmessageflow

       +o workspaces

       +o workspaces-web

       +o xray

[1mSEE ALSO[0m
       +o aws help topics



                                                                         AWS()
