ADMW - AWS Data Migration Workflow 📦
======================================
- A data migration pipeline built on several AWS services.
- From **flat file** to **relational database**.
- Data is processed in parallel using AWS Glue and Spark to decrease the time needed to extract, load, and transform data.

Two-Stage Migration Process ⏩
-------------------------------
**Preliminary Stage**

#. Reads data from a flat file and perform any needed data transformations on the data. 
#. Loads the transformed data into a preliminary table (contains additional columns for status tracking and data integrity checks).
#. Generates a report of the data migration process.

.. image:: https://github.com/maximus-lee-678/ADMW/blob/master/screenshots_diagrams/preliminary_workflow.png
  :alt: Preliminary Stage Visual Workflow
  :width: 50%

**Final Stage**

#. Moves data from the preliminary table to the final table (finalised schema with the addition of a source file name column).
#. Generates a report of the data migration process.
#. Compresses and stores source file.

.. image:: https://github.com/maximus-lee-678/ADMW/blob/master/screenshots_diagrams/final_workflow.png
  :alt: Final Stage Visual Workflow
  :width: 50%

**Global Features**

#. A tracking table in RDS instance is continually updated to aid in monitoring of a data migration process.
#. Report summaries are sent via email using SNS.
#. Users are notified of any errors occurring during a migration process via email.
#. A Jupyter notebook is provided to allow developers to create and test both transformation and reconcilation logic easily.

AWS Services Used ⭯
--------------------
#. **AWS S3** - storage service
#. **AWS Lambda** - serverless computing service
#. **AWS Glue** - data integration service
#. **AWS RDS** - relational database service
#. **AWS SNS** - messaging service
#. **AWS Step Functions** - orchestration service
#. **AWS CloudWatch** - monitoring service
#. **AWS IAM** - access control service

Video Demonstration 🎥
----------------------
.. image:: https://img.youtube.com/vi/WZ_91rDYG8s/maxresdefault.jpg
    :alt: ADMW Demo
    :width: 25%
    :target: https://youtu.be/WZ_91rDYG8s
