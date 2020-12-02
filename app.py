#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


from aws_cdk import core

from my_fits_datalake.my_fits_datalake_stack import MyFitsDatalakeStack

##### Begin customization
# This is where your source FITS files are stored or will be stored. The bucket must exist already and must be unique
source_bucket_name = "<changeme>"

# This is the name of the database for the data catalog 
glue_database_name = "fits_datalake"

# stack id
stack_id = "my-fits-datalake"
###### End customization

if source_bucket_name.startswith('<changeme>'): 
    raise Exception('Please set the source_bucket_name in app.py')

app = core.App()

MyFitsDatalakeStack(app, stack_id, source_bucket_name=source_bucket_name, glue_database_name=glue_database_name)

app.synth()
