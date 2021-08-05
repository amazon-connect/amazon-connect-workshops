#!/bin/bash

source ../common/parameters.ini

python3 deploy.py $Prefix \
    $AthenaCatalog $AthenaDatabaseName $AthenaTableName $AthenaS3Output \
    $QuickSightTemplateDatasetPlaceholder $QuickSightTemplateArn